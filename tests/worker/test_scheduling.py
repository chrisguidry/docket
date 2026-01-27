"""Tests for worker task scheduling, perpetual tasks, and timeouts."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

import cloudpickle  # type: ignore[import]
import pytest

from docket import CurrentDocket, Docket, Perpetual, Worker
from docket.dependencies import Timeout, format_duration


async def test_perpetual_tasks_are_scheduled_close_to_target_time(
    docket: Docket, worker: Worker
):
    """A perpetual task is scheduled as close to the target period as possible"""
    timestamps: list[datetime] = []

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        timestamps.append(datetime.now(timezone.utc))

    await docket.add(perpetual_task, key="my-key")(a="a", b=2)

    await worker.run_at_most({"my-key": 8})

    assert len(timestamps) == 8

    intervals = [next - previous for previous, next in zip(timestamps, timestamps[1:])]

    # Skip the first interval as initial scheduling may differ from steady-state rescheduling
    steady_state_intervals = intervals[1:]
    average = sum(steady_state_intervals, timedelta(0)) / len(steady_state_intervals)

    debug = ", ".join([f"{i.total_seconds() * 1000:.2f}ms" for i in intervals])

    # It's not reliable to assert the maximum duration on different machine setups, but
    # we'll make sure that the minimum is observed (within 5ms), which is the guarantee
    assert average >= timedelta(milliseconds=50), debug


async def test_worker_can_exit_from_perpetual_tasks_that_queue_further_tasks(
    docket: Docket, worker: Worker
):
    """A worker can exit if it's processing a perpetual task that queues more tasks"""

    inner_calls = 0

    async def inner_task():
        nonlocal inner_calls
        inner_calls += 1

    async def perpetual_task(
        docket: Docket = CurrentDocket(),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        await docket.add(inner_task)()
        await docket.add(inner_task)()

    execution = await docket.add(perpetual_task)()

    await worker.run_at_most({execution.key: 3})

    assert inner_calls == 6


async def test_worker_can_exit_from_long_horizon_perpetual_tasks(
    docket: Docket, worker: Worker
):
    """A worker can exit in a timely manner from a perpetual task that has a long
    horizon because it is stricken on both execution and rescheduling"""
    calls: int = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(weeks=37)),
    ):
        nonlocal calls
        calls += 1

    await docket.add(perpetual_task, key="my-key")(a="a", b=2)

    await worker.run_at_most({"my-key": 1})

    assert calls == 1


def test_formatting_durations():
    assert format_duration(0.000001) == "     0ms"
    assert format_duration(0.000010) == "     0ms"
    assert format_duration(0.000100) == "     0ms"
    assert format_duration(0.001000) == "     1ms"
    assert format_duration(0.010000) == "    10ms"
    assert format_duration(0.100000) == "   100ms"
    assert format_duration(1.000000) == "  1000ms"
    assert format_duration(10.00000) == " 10000ms"
    assert format_duration(100.0000) == "   100s "
    assert format_duration(1000.000) == "  1000s "
    assert format_duration(10000.00) == " 10000s "
    assert format_duration(100000.0) == "100000s "


async def test_worker_timeout_exceeds_redelivery_timeout(docket: Docket):
    """Test worker handles user timeout longer than redelivery timeout."""

    task_executed = False

    async def test_task(
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ):
        nonlocal task_executed
        task_executed = True
        await asyncio.sleep(0.01)

    await docket.add(test_task)()

    # Use short redelivery timeout (100ms) to trigger the condition where user timeout > redelivery timeout
    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=100)) as worker:
        await worker.run_until_finished()

    assert task_executed


async def test_replacement_race_condition_stream_tasks(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Test that replace() properly cancels tasks already in the stream.

    This reproduces the race condition where:
    1. Task is scheduled for immediate execution
    2. Scheduler moves it to stream
    3. replace() tries to cancel but only checks queue/hash, not stream
    4. Both original and replacement tasks execute
    """
    key = f"my-cool-task:{uuid4()}"

    # Schedule a task immediately (will be moved to stream quickly)
    await docket.add(the_task, now(), key=key)("a", "b", c="c")

    # Let the scheduler move the task to the stream
    # The scheduler runs every 250ms by default
    await asyncio.sleep(0.3)

    # Now replace the task - this should cancel the one in the stream
    later = now() + timedelta(milliseconds=100)
    await docket.replace(the_task, later, key=key)("b", "c", c="d")

    # Run the worker to completion
    await worker.run_until_finished()

    # Should only execute the replacement task, not both
    the_task.assert_awaited_once_with("b", "c", c="d")
    assert the_task.await_count == 1, (
        f"Task was called {the_task.await_count} times, expected 1"
    )


async def test_replace_task_in_queue_before_stream(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Test that replace() works correctly when task is still in queue."""
    key = f"my-cool-task:{uuid4()}"

    # Schedule a task slightly in the future (stays in queue)
    soon = now() + timedelta(seconds=1)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    # Replace immediately (before scheduler can move it)
    later = now() + timedelta(milliseconds=100)
    await docket.replace(the_task, later, key=key)("b", "c", c="d")

    await worker.run_until_finished()

    # Should only execute the replacement
    the_task.assert_awaited_once_with("b", "c", c="d")
    assert the_task.await_count == 1


async def test_rapid_replace_operations(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Test multiple rapid replace operations."""
    key = f"my-cool-task:{uuid4()}"

    # Schedule initial task
    await docket.add(the_task, now(), key=key)("a", "b", c="c")

    # Rapid replacements
    for i in range(5):
        when = now() + timedelta(milliseconds=50 + i * 10)
        await docket.replace(the_task, when, key=key)(f"arg{i}", b=f"b{i}")

    await worker.run_until_finished()

    # Should only execute the last replacement
    the_task.assert_awaited_once_with("arg4", b="b4")
    assert the_task.await_count == 1


@pytest.mark.parametrize(
    "execution_ttl", [None, timedelta(0)], ids=["default_ttl", "zero_ttl"]
)
async def test_duplicate_execution_race_condition_non_perpetual_task(
    redis_url: str, execution_ttl: timedelta | None, make_docket_name: Callable[[], str]
):
    """Reproduce race condition where non-perpetual tasks execute multiple times.

    Bug: known_task_key is deleted BEFORE task function runs (worker.py:588),
    allowing duplicate docket.add() calls with the same key to succeed
    while the original task is still executing.

    Timeline:
    1. Task A scheduled with key="task:123" -> known_key set
    2. Worker picks up Task A, _perpetuate_if_requested() returns False
    3. Worker calls _delete_known_task() -> known_key DELETED
    4. Worker starts executing the actual task function (slow task)
    5. Meanwhile, docket.add(key="task:123") checks EXISTS known_key -> 0
    6. Duplicate task scheduled and picked up by concurrent worker
    7. Both tasks execute in parallel

    Tests both default TTL and execution_ttl=0 to ensure fix doesn't depend
    on volatile results keys.
    """
    execution_count = 0
    task_started = asyncio.Event()

    async def slow_task(task_id: str):
        nonlocal execution_count
        execution_count += 1
        task_started.set()
        await asyncio.sleep(0.3)

    docket_kwargs: dict[str, object] = {
        "name": make_docket_name(),
        "url": redis_url,
    }
    if execution_ttl is not None:
        docket_kwargs["execution_ttl"] = execution_ttl

    async with Docket(**docket_kwargs) as docket:  # type: ignore[arg-type]
        docket.register(slow_task)
        task_key = f"race-test:{uuid4()}"

        async with Worker(docket, concurrency=2) as worker:
            worker_task = asyncio.create_task(worker.run_until_finished())

            # Schedule first task
            await docket.add(slow_task, key=task_key)("first")

            # Wait for task to start (known_key already deleted at this point)
            await asyncio.wait_for(task_started.wait(), timeout=2.0)
            await asyncio.sleep(0.05)  # Small buffer to ensure deletion happened

            # Attempt duplicate - should be rejected but isn't due to bug
            await docket.add(slow_task, key=task_key)("second")

            await asyncio.wait_for(worker_task, timeout=5.0)

        # BUG: execution_count == 2 (both tasks ran)
        # EXPECTED: execution_count == 1 (duplicate rejected)
        assert execution_count == 1, (
            f"Task executed {execution_count} times, expected 1"
        )


async def test_wrongtype_error_with_legacy_known_task_key(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test graceful handling when known task keys exist as strings from legacy implementations.

    Regression test for issue where worker scheduler would get WRONGTYPE errors when trying to
    HSET on known task keys that existed as string values from older docket versions.

    The original error occurred when:
    1. A legacy docket created known task keys as simple string values (timestamps)
    2. The new scheduler tried to HSET stream_message_id on these keys
    3. Redis threw WRONGTYPE error because you can't HSET on a string key
    4. This caused scheduler loop failures in production

    This test reproduces that scenario by manually setting up the legacy state,
    then verifies the new code handles it gracefully without errors.
    """
    import logging

    key = f"legacy-task:{uuid4()}"

    # Simulate legacy behavior: create the known task key as a string
    # This is what older versions of docket would have done
    async with docket.redis() as redis:
        known_task_key = docket.known_task_key(key)
        when = now() + timedelta(seconds=1)

        # Set up legacy state: known key as string, task in queue with parked data
        await redis.set(known_task_key, str(when.timestamp()))
        await redis.zadd(docket.queue_key, {key: when.timestamp()})

        await redis.hset(  # type: ignore
            docket.parked_task_key(key),
            mapping={
                "key": key,
                "when": when.isoformat(),
                "function": "trace",
                "args": cloudpickle.dumps(["legacy task test"]),  # type: ignore[arg-type]
                "kwargs": cloudpickle.dumps({}),  # type: ignore[arg-type]
                "attempt": "1",
            },
        )

    # Capture logs to ensure no errors occur and see task execution
    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    # Should not have any ERROR logs now that the issue is fixed
    error_logs = [record for record in caplog.records if record.levelname == "ERROR"]
    assert len(error_logs) == 0, (
        f"Expected no error logs, but got: {[r.message for r in error_logs]}"
    )

    # The task should execute successfully
    # Since we used trace, we should see an INFO log with the message
    info_logs = [record for record in caplog.records if record.levelname == "INFO"]
    trace_logs = [
        record for record in info_logs if "legacy task test" in record.message
    ]
    assert len(trace_logs) > 0, (
        f"Expected to see trace log with 'legacy task test', got: {[r.message for r in info_logs]}"
    )


async def test_replace_task_with_legacy_known_key(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Test that replace() works with legacy string known_keys.

    This reproduces the exact production scenario where replace() would get
    WRONGTYPE errors when trying to HGET on legacy string known_keys.
    The main goal is to verify no WRONGTYPE error occurs.
    """
    key = f"legacy-replace-task:{uuid4()}"

    # Simulate legacy state: create known_key as string (old format)
    async with docket.redis() as redis:
        known_task_key = docket.known_task_key(key)
        when = now()

        # Create legacy known_key as STRING (what old code did)
        await redis.set(known_task_key, str(when.timestamp()))

    # Now try to replace - this should work without WRONGTYPE error
    # The key point is that this call succeeds without throwing WRONGTYPE
    replacement_time = now() + timedelta(seconds=1)
    await docket.replace("trace", replacement_time, key=key)("replacement message")


async def test_worker_run_classmethod_memory_backend() -> None:
    """Worker.run should complete immediately when there is no work queued."""

    await Worker.run(
        docket_name=f"test-run-{uuid4()}",
        url="memory://",
        tasks=[],
        schedule_automatic_tasks=False,
        until_finished=True,
    )
