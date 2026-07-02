"""Tests for batch scheduling: Docket.call / add_many / replace_many.

These cover the pipelined batch path added for one-round-trip fan-out
scheduling: per-execution dispositions, key dedup, strike-list checks,
replace semantics, chunking, per-execution error capture, and the TaskCall
specs that feed the batch methods.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest

from docket import Disposition, Docket, ExecutionState, TaskCall, Worker
from docket.execution import schedule_many


@asynccontextmanager
async def counting_pipeline_executes(
    docket: Docket,
) -> AsyncGenerator[list[int], None]:
    """Count pipeline.execute() calls that carried commands -- each one is a
    Redis round-trip.

    Wraps (not replaces) the concrete pipeline class's ``execute`` for
    whichever backend the docket is connected to, so the commands still
    genuinely run.  Empty executes are ignored: burner's ``Pipeline.__aexit__``
    re-invokes ``execute()`` on clean exit, a command-less no-op that would
    otherwise double every count on the memory backend.  Yields a single-item
    list holding the running count.
    """
    async with docket.redis() as redis:
        pipeline_type = type(redis.pipeline())

    real_execute = pipeline_type.execute
    count = [0]

    async def spying_execute(self: Any, *args: Any, **kwargs: Any) -> Any:
        replies = await real_execute(self, *args, **kwargs)
        # A ternary rather than an `if`: the command-less execute only ever
        # happens on the memory backend, so an if-branch would be partially
        # covered on every real-Redis CI leg.
        count[0] += 1 if replies else 0
        return replies

    with patch.object(pipeline_type, "execute", spying_execute):
        yield count


async def test_add_many_schedules_all_tasks(docket: Docket, the_task: AsyncMock):
    """A batch of immediate adds lands every task on the stream."""
    executions = await docket.add_many(
        docket.call(the_task)(index) for index in range(5)
    )

    assert len(executions) == 5
    assert all(e.disposition is Disposition.SCHEDULED for e in executions)
    assert all(e.state is ExecutionState.QUEUED for e in executions)

    snapshot = await docket.snapshot()
    assert len(snapshot.running) + len(snapshot.future) == 5


async def test_add_many_executions_run(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Batch-added tasks actually execute, each with its own arguments."""
    await docket.add_many(docket.call(the_task)(index) for index in range(3))

    await worker.run_until_finished()

    assert the_task.await_count == 3
    called_with = {call.args[0] for call in the_task.await_args_list}
    assert called_with == {0, 1, 2}


async def test_add_many_schedules_future_tasks(docket: Docket, the_task: AsyncMock):
    """Future ``when``s park tasks on the queue rather than the stream."""
    when = datetime.now(timezone.utc) + timedelta(seconds=60)

    executions = await docket.add_many(
        docket.call(the_task, when=when, key=f"future-{index}")(index)
        for index in range(3)
    )

    assert all(e.disposition is Disposition.SCHEDULED for e in executions)
    assert all(e.state is ExecutionState.SCHEDULED for e in executions)

    snapshot = await docket.snapshot()
    assert {e.key for e in snapshot.future} == {"future-0", "future-1", "future-2"}


async def test_add_many_mixes_immediate_and_future(docket: Docket, the_task: AsyncMock):
    """One batch can carry both immediate and future schedules."""
    when = datetime.now(timezone.utc) + timedelta(seconds=60)

    immediate, future = await docket.add_many(
        [
            docket.call(the_task, key="immediate")("now"),
            docket.call(the_task, when=when, key="future")("later"),
        ]
    )

    assert immediate.state is ExecutionState.QUEUED
    assert future.state is ExecutionState.SCHEDULED


async def test_add_many_preserves_input_order(docket: Docket, the_task: AsyncMock):
    """Returned executions line up one-to-one with the input calls."""
    keys = [f"ordered-{index}" for index in range(10)]

    executions = await docket.add_many(
        docket.call(the_task, key=key)(key) for key in keys
    )

    assert [e.key for e in executions] == keys


async def test_add_many_deduplicates_by_key(docket: Docket, the_task: AsyncMock):
    """A key already known to the docket keeps its prior schedule."""
    original_when = datetime.now(timezone.utc) + timedelta(seconds=60)
    (first,) = await docket.add_many(
        [docket.call(the_task, when=original_when, key="dedup")("original")]
    )
    assert first.disposition is Disposition.SCHEDULED

    (second,) = await docket.add_many([docket.call(the_task, key="dedup")("usurper")])
    assert second.disposition is Disposition.ALREADY_SCHEDULED

    snapshot = await docket.snapshot()
    (scheduled,) = snapshot.future
    assert scheduled.when == original_when


async def test_add_many_deduplicates_within_a_single_batch(
    docket: Docket, the_task: AsyncMock
):
    """Two calls with the same key in one batch: first wins, second dedups."""
    first, second = await docket.add_many(
        [
            docket.call(the_task, key="dup")("winner"),
            docket.call(the_task, key="dup")("loser"),
        ]
    )

    assert first.disposition is Disposition.SCHEDULED
    assert second.disposition is Disposition.ALREADY_SCHEDULED


async def test_add_many_skips_stricken_tasks_individually(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    """A strike blocks only the matching executions; the rest schedule."""
    docket.register(the_task)
    docket.register(another_task)
    await docket.strike("the_task")

    stricken, allowed = await docket.add_many(
        [
            docket.call(the_task, key="stricken")(),
            docket.call(another_task, key="allowed")(),
        ]
    )

    assert stricken.disposition is Disposition.STRUCK
    assert allowed.disposition is Disposition.SCHEDULED

    snapshot = await docket.snapshot()
    assert {e.key for e in snapshot.running} | {e.key for e in snapshot.future} == {
        "allowed"
    }


async def test_replace_many_overwrites_prior_schedules(
    docket: Docket, the_task: AsyncMock
):
    """replace_many moves each key's schedule to the new time."""
    original_when = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add_many(
        docket.call(the_task, when=original_when, key=f"job-{index}")(index)
        for index in range(3)
    )

    new_when = datetime.now(timezone.utc) + timedelta(seconds=120)
    executions = await docket.replace_many(
        docket.call(the_task, when=new_when, key=f"job-{index}")(index)
        for index in range(3)
    )

    assert all(e.disposition is Disposition.SCHEDULED for e in executions)

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 3
    assert all(e.when == new_when for e in snapshot.future)


async def test_replace_many_last_duplicate_key_wins(
    docket: Docket, the_task: AsyncMock
):
    """Duplicate keys in one replace batch: each replaces the prior, last wins."""
    first_when = datetime.now(timezone.utc) + timedelta(seconds=60)
    last_when = datetime.now(timezone.utc) + timedelta(seconds=120)

    first, last = await docket.replace_many(
        [
            docket.call(the_task, when=first_when, key="dup")("first"),
            docket.call(the_task, when=last_when, key="dup")("last"),
        ]
    )

    assert first.disposition is Disposition.SCHEDULED
    assert last.disposition is Disposition.SCHEDULED

    snapshot = await docket.snapshot()
    (scheduled,) = snapshot.future
    assert scheduled.when == last_when


async def test_replace_many_schedules_when_no_prior_task_exists(
    docket: Docket, the_task: AsyncMock
):
    """Replacing a key with no prior schedule simply schedules it."""
    when = datetime.now(timezone.utc) + timedelta(seconds=60)

    (execution,) = await docket.replace_many(
        [docket.call(the_task, when=when, key="fresh")("value")]
    )

    assert execution.disposition is Disposition.SCHEDULED
    snapshot = await docket.snapshot()
    assert {e.key for e in snapshot.future} == {"fresh"}


async def test_replace_many_skips_stricken_tasks(docket: Docket, the_task: AsyncMock):
    """Strike rules apply per execution on the replace path too."""
    docket.register(the_task)
    await docket.strike("the_task")

    when = datetime.now(timezone.utc) + timedelta(seconds=60)
    (execution,) = await docket.replace_many(
        [docket.call(the_task, when=when, key="stricken")()]
    )

    assert execution.disposition is Disposition.STRUCK
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0


async def test_add_many_issues_one_pipeline_round_trip(
    docket: Docket, the_task: AsyncMock
):
    """All N schedules travel to Redis in a single pipeline execute()."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.add_many(
            docket.call(the_task, key=f"wire-{index}")() for index in range(50)
        )

    assert round_trips[0] == 1


async def test_add_many_chunks_into_bounded_round_trips(
    docket: Docket, the_task: AsyncMock
):
    """chunk_size caps how many schedules share one pipeline execute()."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.add_many(
            (docket.call(the_task, key=f"chunked-{index}")() for index in range(25)),
            chunk_size=10,
        )

    assert round_trips[0] == 3


async def test_chunk_size_none_sends_the_whole_batch_at_once(
    docket: Docket, the_task: AsyncMock
):
    """chunk_size=None disables chunking entirely."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.add_many(
            (docket.call(the_task, key=f"whole-{index}")() for index in range(25)),
            chunk_size=None,
        )

    assert round_trips[0] == 1


async def test_chunk_size_must_be_positive(docket: Docket, the_task: AsyncMock):
    """A non-positive chunk_size is rejected before anything schedules."""
    with pytest.raises(ValueError, match="chunk_size"):
        await docket.add_many([docket.call(the_task)()], chunk_size=0)

    snapshot = await docket.snapshot()
    assert len(snapshot.running) + len(snapshot.future) == 0


async def test_chunk_size_is_validated_even_for_empty_batches(docket: Docket):
    """An invalid chunk_size is rejected whether or not anything schedules."""
    with pytest.raises(ValueError, match="chunk_size"):
        await docket.add_many([], chunk_size=0)


async def test_schedule_many_validates_chunk_size(docket: Docket):
    """The pipelined scheduler itself rejects a non-positive chunk_size."""
    async with docket.redis() as redis:
        with pytest.raises(ValueError, match="chunk_size"):
            await schedule_many(redis, [], replace=False, chunk_size=-1)


async def test_batch_captures_per_execution_redis_errors(
    docket: Docket, the_task: AsyncMock
):
    """A Redis error for one task fails only that execution, not the batch."""
    async with docket.redis() as redis:
        # The _schedule script HSETs each task's runs hash; a string already
        # sitting at that key makes just that one command error (WRONGTYPE).
        await redis.set(docket.key("runs:poisoned"), "not-a-hash")

    try:
        poisoned, healthy = await docket.add_many(
            [
                docket.call(the_task, key="poisoned")(),
                docket.call(the_task, key="healthy")(),
            ]
        )

        assert poisoned.disposition is Disposition.FAILED
        assert isinstance(poisoned.schedule_exception, Exception)
        assert healthy.disposition is Disposition.SCHEDULED
        assert healthy.schedule_exception is None

        snapshot = await docket.snapshot()
        assert {e.key for e in snapshot.running} | {e.key for e in snapshot.future} == {
            "healthy"
        }
    finally:
        async with docket.redis() as redis:
            await redis.delete(docket.key("runs:poisoned"))


async def test_error_in_a_later_chunk_leaves_earlier_chunks_accounted_for(
    docket: Docket, the_task: AsyncMock
):
    """Per-execution error capture composes with chunking: a failure in a
    later chunk fails only its own execution, and every task in the chunks
    before it is fully scheduled and reported."""
    async with docket.redis() as redis:
        await redis.set(docket.key("runs:chunk-poisoned"), "not-a-hash")

    try:
        executions = await docket.add_many(
            [
                docket.call(the_task, key="chunk-ok-0")(),
                docket.call(the_task, key="chunk-ok-1")(),
                docket.call(the_task, key="chunk-ok-2")(),
                docket.call(the_task, key="chunk-ok-3")(),
                docket.call(the_task, key="chunk-poisoned")(),
                docket.call(the_task, key="chunk-ok-5")(),
            ],
            chunk_size=2,  # poisoned key lands in the third of three chunks
        )

        dispositions = {e.key: e.disposition for e in executions}
        assert dispositions.pop("chunk-poisoned") is Disposition.FAILED
        assert all(d is Disposition.SCHEDULED for d in dispositions.values())

        snapshot = await docket.snapshot()
        scheduled_keys = {e.key for e in snapshot.running} | {
            e.key for e in snapshot.future
        }
        assert scheduled_keys == set(dispositions)
    finally:
        async with docket.redis() as redis:
            await redis.delete(docket.key("runs:chunk-poisoned"))


async def test_add_many_with_empty_batch(docket: Docket):
    """An empty batch is a no-op that touches nothing."""
    assert await docket.add_many([]) == []
    assert await docket.replace_many([]) == []


async def test_schedule_many_with_no_executions(docket: Docket):
    """The pipelined scheduler returns immediately for an empty batch."""
    async with docket.redis() as redis:
        await schedule_many(redis, [], replace=False)


async def test_call_builds_a_complete_task_call(docket: Docket, the_task: AsyncMock):
    """call() resolves function, key, and when into a concrete TaskCall."""
    when = datetime.now(timezone.utc) + timedelta(seconds=60)

    task_call = docket.call(the_task, when=when, key="explicit")("arg", flag=True)

    assert isinstance(task_call, TaskCall)
    assert task_call.function is the_task
    assert task_call.args == ("arg",)
    assert task_call.kwargs == {"flag": True}
    assert task_call.key == "explicit"
    assert task_call.when == when
    assert task_call.function_name is None


async def test_call_defaults_key_and_when(docket: Docket, the_task: AsyncMock):
    """Omitted key and when default to a fresh uuid7 and now."""
    before = datetime.now(timezone.utc)
    task_call = docket.call(the_task)()
    after = datetime.now(timezone.utc)

    assert task_call.key
    assert before <= task_call.when <= after


async def test_call_registers_the_function(docket: Docket, the_task: AsyncMock):
    """Like add(), call() registers a not-yet-registered function."""
    assert "the_task" not in docket.tasks

    docket.call(the_task)

    assert docket.tasks["the_task"] is the_task


async def test_call_accepts_a_registered_task_name(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Tasks referenced by name schedule and run like direct references."""
    docket.register(the_task)

    (execution,) = await docket.add_many([docket.call("the_task")("by-name")])

    assert execution.disposition is Disposition.SCHEDULED
    assert execution.function_name == "the_task"

    await worker.run_until_finished()
    the_task.assert_awaited_once_with("by-name")


async def test_call_rejects_an_unregistered_task_name(docket: Docket):
    """An unknown task name fails at call() time, before anything schedules."""
    with pytest.raises(KeyError):
        docket.call("never-registered")
