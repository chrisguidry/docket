"""Tests for batch cancellation: Docket.cancel_many.

These cover the pipelined batch path for cancelling many keys at once:
removal from the queue and the stream, the cancelled state record, the
cancellation signal to running tasks, chunking, and the metrics and span.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from opentelemetry.metrics import Counter

from docket import Docket, ExecutionState, Worker, testing


@asynccontextmanager
async def counting_pipeline_executes(
    docket: Docket,
) -> AsyncGenerator[list[int], None]:
    """Count pipeline.execute() calls that carried commands, as in
    test_batch_scheduling, with the cancellation signals stubbed out so that
    only the cancel pipelines count, whichever backend runs the test."""
    async with docket.redis() as redis:
        pipeline_type = type(redis.pipeline())

    real_execute = pipeline_type.execute
    count = [0]

    async def spying_execute(self: Any, *args: Any, **kwargs: Any) -> Any:
        replies = await real_execute(self, *args, **kwargs)
        count[0] += 1 if replies else 0
        return replies

    with (
        patch.object(pipeline_type, "execute", spying_execute),
        patch.object(docket._redis, "publish_many", AsyncMock()),  # pyright: ignore[reportPrivateUsage]
    ):
        yield count


def later() -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=5)


async def test_cancel_many_removes_future_tasks(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Every key in the batch comes off the queue and never runs."""
    for index in range(5):
        await docket.add(the_task, when=later(), key=f"future-{index}")()

    await docket.cancel_many(f"future-{index}" for index in range(5))

    await testing.assert_task_not_scheduled(docket, the_task)
    await worker.run_until_finished()
    the_task.assert_not_called()


async def test_cancel_many_removes_immediate_tasks(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Tasks already on the stream come off it too."""
    for index in range(3):
        await docket.add(the_task, key=f"now-{index}")()

    await docket.cancel_many([f"now-{index}" for index in range(3)])

    await testing.assert_task_not_scheduled(docket, the_task)
    await worker.run_until_finished()
    the_task.assert_not_called()


async def test_cancel_many_leaves_other_keys_alone(docket: Docket, the_task: AsyncMock):
    """Only the keys in the batch are cancelled."""
    await docket.add(the_task, when=later(), key="cancel-me")()
    await docket.add(the_task, when=later(), key="keep-me")()

    await docket.cancel_many(["cancel-me"])

    await testing.assert_task_scheduled(docket, the_task, key="keep-me")
    await testing.assert_task_not_scheduled(docket, the_task, key="cancel-me")


async def test_cancel_many_records_the_cancelled_state(
    docket: Docket, the_task: AsyncMock
):
    """Each cancelled key keeps a CANCELLED record for execution_ttl, as
    cancel() leaves it."""
    execution = await docket.add(the_task, when=later(), key="recorded")()

    await docket.cancel_many(["recorded"])

    await execution.sync()
    assert execution.state is ExecutionState.CANCELLED
    async with docket.redis() as redis:
        assert await redis.ttl(docket.runs_key("recorded")) > 0


async def test_cancel_many_with_zero_execution_ttl_deletes_the_record(
    zero_ttl_docket: Docket, the_task: AsyncMock
):
    """With execution_ttl=0, the cancelled record is deleted at once, as
    cancel() deletes it."""
    await zero_ttl_docket.add(the_task, when=later(), key="unrecorded")()

    await zero_ttl_docket.cancel_many(["unrecorded"])

    async with zero_ttl_docket.redis() as redis:
        assert not await redis.exists(zero_ttl_docket.runs_key("unrecorded"))


async def test_cancel_many_ignores_unknown_keys(docket: Docket, the_task: AsyncMock):
    """Cancelling keys that were never scheduled is a no-op, as cancel() is."""
    await docket.cancel_many(["never-scheduled", "also-never-scheduled"])

    await testing.assert_task_not_scheduled(docket, the_task)


async def test_cancel_many_stops_running_tasks(docket: Docket, worker: Worker):
    """A running task in the batch receives its cancellation signal."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_task() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    docket.register(slow_task)
    execution = await docket.add(slow_task, key="running")()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel_many(["running"])

    await asyncio.wait_for(cancelled.wait(), timeout=5.0)
    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state is ExecutionState.CANCELLED


async def test_cancel_many_issues_one_pipeline_round_trip(
    docket: Docket, the_task: AsyncMock
):
    """All N cancels travel to Redis in a single pipeline execute()."""
    for index in range(50):
        await docket.add(the_task, when=later(), key=f"wire-{index}")()

    async with counting_pipeline_executes(docket) as round_trips:
        await docket.cancel_many(f"wire-{index}" for index in range(50))

    assert round_trips[0] == 1


async def test_cancel_many_chunks_into_bounded_round_trips(
    docket: Docket, the_task: AsyncMock
):
    """chunk_size caps how many cancels share one pipeline execute(), and
    each chunk publishes its own cancellation signals."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.cancel_many(
            (f"chunked-{index}" for index in range(25)), chunk_size=10
        )
        publish_many = docket._redis.publish_many  # pyright: ignore[reportPrivateUsage]

    assert round_trips[0] == 3
    assert isinstance(publish_many, AsyncMock)
    assert [len(call.args[0]) for call in publish_many.await_args_list] == [10, 10, 5]
    assert publish_many.await_args_list[0].args[0][0] == (
        docket.cancel_channel("chunked-0"),
        "chunked-0",
    )


async def test_cancel_many_chunk_size_none_sends_the_whole_batch_at_once(
    docket: Docket,
):
    """chunk_size=None disables chunking entirely."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.cancel_many(
            (f"whole-{index}" for index in range(25)), chunk_size=None
        )

    assert round_trips[0] == 1


async def test_cancel_many_chunk_size_must_be_positive(docket: Docket):
    """A non-positive chunk_size is rejected, even for an empty batch."""
    with pytest.raises(ValueError, match="chunk_size"):
        await docket.cancel_many(["a-key"], chunk_size=0)
    with pytest.raises(ValueError, match="chunk_size"):
        await docket.cancel_many([], chunk_size=-1)


async def test_cancel_many_with_empty_batch(docket: Docket):
    """An empty batch sends nothing to Redis."""
    async with counting_pipeline_executes(docket) as round_trips:
        await docket.cancel_many([])

    assert round_trips[0] == 0


async def test_cancel_many_counts_each_key_as_cancelled(
    docket: Docket, monkeypatch: pytest.MonkeyPatch
):
    """TASKS_CANCELLED counts every key, once per chunk."""
    tasks_cancelled = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_CANCELLED.add", tasks_cancelled)

    await docket.cancel_many((f"counted-{index}" for index in range(3)), chunk_size=2)

    labels = {"docket.name": docket.name}
    assert [call.args for call in tasks_cancelled.call_args_list] == [
        (2, labels),
        (1, labels),
    ]


async def test_cancel_many_emits_one_batch_span(docket: Docket):
    """A batch cancel opens one docket.cancel_many span with a count
    attribute, not one docket.cancel span per key."""
    with patch("docket.docket.tracer") as tracer:
        await docket.cancel_many(f"traced-{index}" for index in range(4))

    (call,) = tracer.start_as_current_span.call_args_list
    assert call.args == ("docket.cancel_many",)
    assert call.kwargs["attributes"]["docket.name"] == docket.name
    assert call.kwargs["attributes"]["docket.batch.count"] == 4


async def test_cancel_many_signals_the_rest_of_a_chunk_when_one_key_fails(
    docket: Docket, worker: Worker
):
    """When one key's cancel fails, the running tasks whose cancels went
    through in the same chunk still get their signals, as they would from
    cancel(), and the error is raised after."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_task() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    docket.register(slow_task)
    execution = await docket.add(slow_task, key="running")()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    # A string where the runs hash belongs makes the cancel script's HGET fail.
    async with docket.redis() as redis:
        await redis.set(docket.runs_key("wrong-type"), "not a hash")

    with pytest.raises(Exception, match="WRONGTYPE"):
        await docket.cancel_many(["wrong-type", "running"])

    await asyncio.wait_for(cancelled.wait(), timeout=5.0)
    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state is ExecutionState.CANCELLED


async def test_cancel_many_raises_when_every_key_in_a_chunk_fails(
    docket: Docket, monkeypatch: pytest.MonkeyPatch
):
    """A chunk whose every cancel fails publishes nothing and counts nothing."""
    tasks_cancelled = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_CANCELLED.add", tasks_cancelled)
    async with docket.redis() as redis:
        await redis.set(docket.runs_key("wrong-type"), "not a hash")

    with patch.object(docket._redis, "publish_many", AsyncMock()) as publish_many:  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(Exception, match="WRONGTYPE"):
            await docket.cancel_many(["wrong-type"])

    publish_many.assert_not_awaited()
    tasks_cancelled.assert_not_called()
