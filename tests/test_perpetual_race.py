"""Tests for the generation counter that prevents stale executions.

The generation counter closes two gaps:

1. **Perpetual rescheduling race**: When a running Perpetual task is externally
   replaced and both the old and new executions call on_complete(), the stale
   one's reschedule is suppressed (tail check).

2. **Replace-vs-XREADGROUP race**: When replace() runs XDEL on a stream message
   that a worker has already read via XREADGROUP, the stale message is skipped
   before claim() (head check).  This applies to ALL keyed tasks, not just
   Perpetual ones.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import cloudpickle  # type: ignore[import-untyped]
from docket import CurrentExecution, Docket, Perpetual, Worker
from docket.execution import Execution

TASK_KEY = "perpetual-race-test"

STALE_INTERVAL = timedelta(seconds=2)
CORRECT_INTERVAL = timedelta(milliseconds=500)


async def test_stale_perpetual_on_complete_overwrites_correct_successor(
    docket: Docket, worker: Worker
):
    """When a running Perpetual task is externally replaced and finishes after
    the replacement, its on_complete overwrites the correctly-timed successor."""

    config: dict[str, timedelta] = {"interval": STALE_INTERVAL}

    task_a_started = asyncio.Event()
    let_a_finish = asyncio.Event()
    task_b_started = asyncio.Event()
    let_b_finish = asyncio.Event()

    executions: list[Execution] = []

    async def racing_task(
        perpetual: Perpetual = Perpetual(),
        execution: Execution = CurrentExecution(),
    ):
        my_interval = config["interval"]
        call_number = len(executions) + 1
        executions.append(execution)

        if call_number == 1:
            # Task A: signal start, block until released
            task_a_started.set()
            await asyncio.wait_for(let_a_finish.wait(), timeout=10)
        elif call_number == 2:
            # Task B: signal start, block until released
            task_b_started.set()
            await asyncio.wait_for(let_b_finish.wait(), timeout=10)
        # Task C (call 3): the successor — just runs

        perpetual.after(my_interval)

    # Schedule the initial task (A)
    await docket.add(racing_task, key=TASK_KEY)()

    # Run the worker in the background (allow 3 executions of this key)
    worker_task = asyncio.create_task(worker.run_at_most({TASK_KEY: 3}))

    # Wait for task A to start executing
    await asyncio.wait_for(task_a_started.wait(), timeout=10)

    # Simulate user changing config to a shorter interval
    config["interval"] = CORRECT_INTERVAL

    # External replace: force immediate re-execution (creates task B)
    replace_time = datetime.now(timezone.utc)
    await docket.replace(racing_task, replace_time, TASK_KEY)()

    # Wait for task B to start
    await asyncio.wait_for(task_b_started.wait(), timeout=10)

    # Let B finish first — B's on_complete schedules successor at B_time + 500ms
    let_b_finish.set()
    # Give B's on_complete time to complete
    await asyncio.sleep(0.05)

    # Now let A finish — A's on_complete overwrites B's successor with
    # A_start + 2s (stale), pushing the successor much further out
    let_a_finish.set()

    # Wait for all 3 executions to complete
    await asyncio.wait_for(worker_task, timeout=15)

    assert len(executions) == 3

    # The third execution's `when` tells us which on_complete won the race.
    #
    # If B's on_complete won (correct): when ≈ B_completion + 500ms  (< 1s from replace)
    # If A's on_complete won (stale):   when ≈ A_completion + 2s     (> 1s from replace)
    successor = executions[2]
    gap = successor.when - replace_time

    assert gap < timedelta(seconds=1), (
        f"Stale execution won the race: successor scheduled "
        f"{gap.total_seconds():.2f}s after the correct replacement, "
        f"expected < 1s (correct interval is {CORRECT_INTERVAL})"
    )


async def test_is_superseded_after_replace(docket: Docket):
    """An execution becomes superseded when the same key is rescheduled."""

    async def noop():
        pass  # pragma: no cover

    await docket.add(noop, key="gen-test")()

    # Build an Execution from the stream message to capture its generation
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=1)
    _, message = messages[0]
    original = await Execution.from_message(docket, message)

    assert original.generation == 1
    assert not await original.is_superseded()

    # Replacing bumps the generation in the runs hash
    await docket.replace(noop, datetime.now(timezone.utc), "gen-test")()

    assert await original.is_superseded()


async def test_superseded_message_skipped_before_execution(
    docket: Docket, worker: Worker
):
    """A stale message in the stream is skipped without running the function.

    This covers the case where a message was already pending (e.g. after a
    worker crash and redelivery) when the task was replaced. The runs hash
    has a newer generation so the worker bails before claim().
    """
    calls: list[str] = []

    async def tracked_task():
        calls.append("ran")  # pragma: no cover

    await docket.add(tracked_task, key="head-check")()

    # Bump the generation in the runs hash without touching the stream message.
    # This simulates the state after a replace where the old message is still
    # pending in the consumer group (e.g. redelivery after crash).
    runs_key = docket.key("runs:head-check")
    async with docket.redis() as redis:
        await redis.hincrby(runs_key, "generation", 1)  # type: ignore[misc]

    await worker.run_until_finished()

    assert calls == [], "superseded task should not have executed"


async def test_old_message_without_generation_runs_normally(
    docket: Docket, worker: Worker
):
    """A message from pre-generation code (no generation field) still executes.

    Simulates the old→new upgrade: old code scheduled a task without the
    generation field in either the stream message or the runs hash. The new
    worker should treat generation=0 as "unknown" and run it normally.
    """
    calls: list[str] = []

    async def legacy_task():
        calls.append("ran")

    docket.register(legacy_task)

    # Manually construct a stream message the way old code would — no generation field
    message = {
        b"key": b"old-to-new",
        b"when": datetime.now(timezone.utc).isoformat().encode(),
        b"function": b"legacy_task",
        b"args": cloudpickle.dumps(()),  # type: ignore[no-untyped-call]
        b"kwargs": cloudpickle.dumps({}),  # type: ignore[no-untyped-call]
        b"attempt": b"1",
    }

    async with docket.redis() as redis:
        message_id = await redis.xadd(docket.stream_key, message)  # type: ignore[arg-type]

        # Set up runs hash without generation, as old code would
        await redis.hset(  # type: ignore[misc]
            docket.key("runs:old-to-new"),
            mapping={
                "state": "queued",
                "when": str(datetime.now(timezone.utc).timestamp()),
                "known": str(datetime.now(timezone.utc).timestamp()),
                "stream_id": message_id,
                "function": "legacy_task",
            },
        )

    await worker.run_until_finished()

    assert calls == ["ran"], "old message without generation should execute normally"


async def test_new_task_moved_by_old_scheduler_runs_normally(
    docket: Docket, worker: Worker
):
    """A task scheduled by new code but moved to stream by old scheduler runs.

    Simulates the new→old scheduler→new worker upgrade path: new code schedules
    a task (generation=1 in runs hash), but an older worker's scheduler Lua
    moves it from queue to stream WITHOUT the generation field. The new worker
    receives generation=0 from the message, sees generation=1 in the runs hash,
    but still runs the task because generation=0 is treated as "pre-tracking".
    """
    calls: list[str] = []

    async def upgraded_task():
        calls.append("ran")

    docket.register(upgraded_task)

    # Schedule with new code to get generation=1 in the runs hash
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await docket.add(upgraded_task, when=future, key="new-old-new")()

    async with docket.redis() as redis:
        # Verify new code set generation=1 in the runs hash
        gen = await redis.hget(docket.key("runs:new-old-new"), "generation")  # type: ignore[misc]
        assert gen == b"1"

        # Simulate old scheduler moving from queue to stream WITHOUT generation
        parked_data: dict[bytes, bytes] = await redis.hgetall(  # type: ignore[misc]
            docket.parked_task_key("new-old-new")
        )
        stream_message: dict[bytes, bytes] = {
            k: v
            for k, v in parked_data.items()  # type: ignore[misc]
            if k != b"generation"  # old scheduler doesn't know about this field
        }
        message_id = await redis.xadd(docket.stream_key, stream_message)  # type: ignore[arg-type]

        # Clean up queue/parked state as the old scheduler would
        await redis.zrem(docket.queue_key, "new-old-new")
        await redis.delete(docket.parked_task_key("new-old-new"))
        await redis.hset(  # type: ignore[misc]
            docket.key("runs:new-old-new"),
            mapping={"state": "queued", "stream_id": message_id},
        )

    await worker.run_until_finished()

    assert calls == ["ran"], (
        "task with generation=0 in message but generation=1 in runs hash "
        "should still execute (generation=0 means pre-tracking)"
    )


async def test_replace_skips_stale_stream_message(docket: Docket, worker: Worker):
    """When replace() can't XDEL a message the worker already read, the
    generation check prevents the stale message from executing.

    This simulates the tight race where XREADGROUP delivers a message to
    the worker before replace()'s XDEL can remove it, so the stream has
    both the old (gen=1) and new (gen=2) messages. Only gen=2 should run.
    """
    calls: list[int] = []

    async def tracked_task(
        execution: Execution = CurrentExecution(),
    ):
        calls.append(execution.generation)

    # Schedule the task (gen=1 in stream and runs hash)
    await docket.add(tracked_task, key="replace-race")()

    # Grab the gen=1 message before replace() deletes it
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=10)
    stale_message = None
    for _, msg in messages:
        if msg[b"key"] == b"replace-race":
            stale_message = msg
            break
    assert stale_message is not None

    # replace() deletes gen=1 from the stream and adds gen=2
    await docket.replace(tracked_task, datetime.now(timezone.utc), "replace-race")()

    # Re-inject the stale gen=1 message as if the worker had already read it
    # via XREADGROUP before the XDEL could remove it
    async with docket.redis() as redis:
        await redis.xadd(docket.stream_key, stale_message)  # type: ignore[arg-type]

    await worker.run_until_finished()

    # Only the gen=2 message should have executed
    assert calls == [2], (
        f"Expected only generation 2 to execute, got generations {calls}"
    )
