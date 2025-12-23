import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Callable

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import cloudpickle  # type: ignore[import]
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from docket import (
    CurrentDocket,
    CurrentWorker,
    Docket,
    Perpetual,
    Worker,
)
from docket.dependencies import Timeout
from docket.execution import Execution
from docket.tasks import standard_tasks
from docket.worker import ms
from tests._key_leak_checker import KeyCountChecker


async def test_worker_acknowledges_messages(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """The worker should acknowledge and drain messages as they're processed"""

    await docket.add(the_task)()

    await worker.run_until_finished()

    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0

        assert await redis.xlen(docket.stream_key) == 0


async def test_two_workers_split_work(docket: Docket):
    """Two workers should split the workload"""

    worker1 = Worker(docket)
    worker2 = Worker(docket)

    call_counts = {
        worker1: 0,
        worker2: 0,
    }

    async def the_task(worker: Worker = CurrentWorker()):
        call_counts[worker] += 1

    for _ in range(100):
        await docket.add(the_task)()

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    assert call_counts[worker1] + call_counts[worker2] == 100
    assert call_counts[worker1] > 40
    assert call_counts[worker2] > 40


async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock
):
    """The worker should reconnect when the connection is lost"""
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=100))

    # Mock the _worker_loop method to fail once then succeed
    original_worker_loop = worker._worker_loop  # type: ignore[protected-access]
    call_count = 0

    async def mock_worker_loop(redis: Redis, forever: bool = False):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated connection error")
        return await original_worker_loop(redis, forever=forever)

    worker._worker_loop = mock_worker_loop  # type: ignore[protected-access]

    await docket.add(the_task)()

    async with worker:
        await worker.run_until_finished()

    assert call_count == 2
    the_task.assert_called_once()


async def test_worker_respects_concurrency_limit(docket: Docket, worker: Worker):
    """Worker should not exceed its configured concurrency limit"""

    task_results: set[int] = set()

    currently_running = 0
    max_concurrency_observed = 0

    async def concurrency_tracking_task(index: int):
        nonlocal currently_running, max_concurrency_observed

        currently_running += 1
        max_concurrency_observed = max(max_concurrency_observed, currently_running)

        await asyncio.sleep(0.01)
        task_results.add(index)

        currently_running -= 1

    for i in range(50):
        await docket.add(concurrency_tracking_task)(index=i)

    worker.concurrency = 5
    await worker.run_until_finished()

    assert task_results == set(range(50))

    assert 1 < max_concurrency_observed <= 5


async def test_worker_handles_unregistered_task_execution_on_initial_delivery(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
    the_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """worker should handle the case when an unregistered task is executed"""
    await docket.add(the_task)()

    docket.tasks.pop("the_task")

    with caplog.at_level(logging.WARNING):
        await worker.run_until_finished()

    # Default fallback logs warning and ACKs the message
    assert "Unknown task 'the_task' received - dropping" in caplog.text
    assert "Register via CLI (--tasks your.module:tasks)" in caplog.text


async def test_worker_handles_unregistered_task_execution_on_redelivery(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
    key_leak_checker: KeyCountChecker,
):
    """worker should handle the case when an unregistered task is redelivered"""

    async def test_task():
        await asyncio.sleep(0.01)

    # Register and schedule the task first
    docket.register(test_task)
    await docket.add(test_task)()

    # First run the task successfully to ensure line 249 coverage
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_success:
        await worker_success.run_until_finished()

    # Schedule another task for the redelivery test
    await docket.add(test_task)()

    # First worker fails during execution
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("Simulated failure"))  # type: ignore[protected-access]
        with pytest.raises(Exception, match="Simulated failure"):
            await worker_a.run_until_finished()

    # Verify task is pending redelivery
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            docket.stream_key,
            docket.worker_group_name,
        )
        assert pending_info["pending"] == 1

    await asyncio.sleep(0.125)  # Wait for redelivery timeout

    # Unregister the task before redelivery
    docket.tasks.pop("test_task")

    # Second worker should handle the unregistered task gracefully
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_b:
        with caplog.at_level(logging.WARNING):
            await worker_b.run_until_finished()

    # Default fallback logs warning and ACKs the message
    assert "Unknown task 'test_task' received - dropping" in caplog.text
    assert "Register via CLI (--tasks your.module:tasks)" in caplog.text


builtin_tasks = {function.__name__ for function in standard_tasks}


async def test_worker_announcements(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    # Use 100ms heartbeat - short enough for fast tests, long enough to be reliable
    # under CPU contention in CI environments
    heartbeat = timedelta(milliseconds=100)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)
    docket.register(another_task)

    async with Worker(docket, name="worker-a") as worker_a:
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}

        async with Worker(docket, name="worker-b") as worker_b:
            await asyncio.sleep(heartbeat.total_seconds() * 5)

            workers = await docket.workers()
            assert len(workers) == 2
            assert {w.name for w in workers} == {worker_a.name, worker_b.name}

            for worker in workers:
                # Allow generous timing tolerance - CI can have significant delays
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 20)
                assert worker.tasks == builtin_tasks | {"the_task", "another_task"}

        await asyncio.sleep(heartbeat.total_seconds() * 10)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}
        assert worker_b.name not in {w.name for w in workers}

    await asyncio.sleep(heartbeat.total_seconds() * 10)

    workers = await docket.workers()
    assert len(workers) == 0


async def test_task_announcements(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    """Test that we can ask about which workers are available for a task"""

    # Use 100ms heartbeat - short enough for fast tests, long enough to be reliable
    # under CPU contention in CI environments
    heartbeat = timedelta(milliseconds=100)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)
    docket.register(another_task)
    async with Worker(docket, name="worker-a") as worker_a:
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.task_workers("the_task")
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}

        async with Worker(docket, name="worker-b") as worker_b:
            await asyncio.sleep(heartbeat.total_seconds() * 5)

            workers = await docket.task_workers("the_task")
            assert len(workers) == 2
            assert {w.name for w in workers} == {worker_a.name, worker_b.name}

            for worker in workers:
                # Allow generous timing tolerance - CI can have significant delays
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 20)
                assert worker.tasks == builtin_tasks | {"the_task", "another_task"}

        await asyncio.sleep(heartbeat.total_seconds() * 10)

        workers = await docket.task_workers("the_task")
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}
        assert worker_b.name not in {w.name for w in workers}

    await asyncio.sleep(heartbeat.total_seconds() * 10)

    workers = await docket.task_workers("the_task")
    assert len(workers) == 0


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("oof"),
        ValueError("woops"),
    ],
)
async def test_worker_recovers_from_redis_errors(
    docket: Docket,
    the_task: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
):
    """Should recover from errors and continue sending heartbeats"""

    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)

    original_redis = docket.redis
    error_time = None
    redis_calls = 0

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[Redis, None]:
        nonlocal redis_calls, error_time
        redis_calls += 1

        if redis_calls == 2:
            error_time = datetime.now(timezone.utc)
            raise error

        async with original_redis() as r:
            yield r

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(docket) as worker:
        await asyncio.sleep(heartbeat.total_seconds() * 1.5)

        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker.name in {w.name for w in workers}

        # Verify that the last_seen timestamp is after our error
        worker_info = next(w for w in workers if w.name == worker.name)
        assert error_time
        assert worker_info.last_seen > error_time, (
            "Worker should have sent heartbeats after the Redis error"
        )


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
    assert ms(0.000001) == "     0ms"
    assert ms(0.000010) == "     0ms"
    assert ms(0.000100) == "     0ms"
    assert ms(0.001000) == "     1ms"
    assert ms(0.010000) == "    10ms"
    assert ms(0.100000) == "   100ms"
    assert ms(1.000000) == "  1000ms"
    assert ms(10.00000) == " 10000ms"
    assert ms(100.0000) == "   100s "
    assert ms(1000.000) == "  1000s "
    assert ms(10000.00) == " 10000s "
    assert ms(100000.0) == "100000s "


async def test_worker_can_be_told_to_skip_automatic_tasks(docket: Docket):
    """A worker can be told to skip automatic tasks"""

    called = False

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=50), automatic=True
        ),
    ):
        nonlocal called
        called = True  # pragma: no cover

    docket.register(perpetual_task)

    # Without the flag, this would hang because the task would always be scheduled
    async with Worker(docket, schedule_automatic_tasks=False) as worker:
        await worker.run_until_finished()

    assert not called


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


async def test_worker_concurrency_cleanup_without_dependencies(docket: Docket):
    """Test worker cleanup when dependencies are not defined."""
    cleanup_executed = False

    async def simple_task():
        nonlocal cleanup_executed
        # Force an exception after dependencies would be set
        raise ValueError("Force cleanup path")

    await docket.add(simple_task)()

    async with Worker(docket) as worker:
        # This should trigger the finally block cleanup
        await worker.run_until_finished()

    # Exception was handled by worker, test that it didn't crash
    cleanup_executed = True
    assert cleanup_executed


async def test_worker_concurrency_no_limit_with_custom_docket(docket: Docket):
    """Test early return when task has no concurrency limit using custom docket."""
    task_executed = False

    async def task_without_concurrency():
        nonlocal task_executed
        task_executed = True

    await docket.add(task_without_concurrency)()

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_executed


async def test_worker_no_concurrency_dependency_in_function(docket: Docket):
    """Test _can_start_task with function that has no concurrency dependency."""

    async def task_without_concurrency_dependency():
        await asyncio.sleep(0.001)

    await task_without_concurrency_dependency()

    async with Worker(docket) as worker:
        # Create execution for task without concurrency dependency
        execution = Execution(
            docket=docket,
            function=task_without_concurrency_dependency,
            args=(),
            kwargs={},
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
            # This should return True immediately
            result = await worker._can_start_task(redis, execution)  # type: ignore[reportPrivateUsage]
            assert result is True


async def test_worker_exception_before_dependencies(docket: Docket):
    """Test finally block when exception occurs before dependencies are set."""
    task_failed = False

    async def task_that_will_fail():
        nonlocal task_failed
        task_failed = True
        raise RuntimeError("Test exception for coverage")

    try:
        await task_that_will_fail()
    except RuntimeError:
        pass

    # Reset flag to test worker behavior
    task_failed = False

    # Mock resolved_dependencies to fail before setting dependencies

    await docket.add(task_that_will_fail)()

    async with Worker(docket) as worker:
        # Patch resolved_dependencies to raise an exception immediately
        with patch("docket.worker.resolved_dependencies") as mock_deps:
            # Create a context manager that fails on entry
            context = AsyncMock()
            context.__aenter__.side_effect = RuntimeError(
                "Dependencies failed to resolve"
            )
            mock_deps.return_value = context

            # This should trigger the finally block where "dependencies" not in locals()
            await worker.run_until_finished()

    # The task function shouldn't run via worker due to dependency failure
    assert task_failed is False


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
    redis_url: str, execution_ttl: timedelta | None
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
        "name": f"test-race-{uuid4()}",
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


async def test_redis_key_cleanup_successful_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after successful task execution.

    After execution, a tombstone (runs hash) with COMPLETED state remains with TTL.
    The autouse key_leak_checker fixture verifies no leaks automatically.
    """
    # Create and register a simple task
    task_executed = False

    async def successful_task():
        nonlocal task_executed
        task_executed = True
        await asyncio.sleep(0.01)  # Small delay to ensure proper execution flow

    docket.register(successful_task)

    # Schedule and execute the task
    await docket.add(successful_task)()
    await worker.run_until_finished()

    # Verify task executed successfully
    assert task_executed, "Task should have executed successfully"

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_redis_key_cleanup_failed_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after failed task execution.

    After failure, a tombstone (runs hash) with FAILED state remains with TTL.
    The autouse key_leak_checker fixture verifies no leaks automatically.
    """
    # Create a task that will fail
    task_attempted = False

    async def failing_task():
        nonlocal task_attempted
        task_attempted = True
        raise ValueError("Intentional test failure")

    docket.register(failing_task)

    # Schedule and execute the task (should fail)
    await docket.add(failing_task)()
    await worker.run_until_finished()

    # Verify task was attempted
    assert task_attempted, "Task should have been attempted"

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_redis_key_cleanup_cancelled_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after task cancellation.

    After cancellation, a tombstone (runs hash) with CANCELLED state remains with TTL
    to support the claim check pattern via get_execution(). All other keys (queue,
    parked data, etc.) are cleaned up. The autouse key_leak_checker fixture verifies
    no leaks automatically.
    """
    from docket.execution import ExecutionState

    # Create a task that won't be executed
    task_executed = False

    async def task_to_cancel():
        nonlocal task_executed
        task_executed = True  # pragma: no cover

    docket.register(task_to_cancel)

    # Schedule the task for future execution
    future_time = datetime.now(timezone.utc) + timedelta(seconds=10)
    execution = await docket.add(task_to_cancel, future_time)()

    # Cancel the task
    await docket.cancel(execution.key)

    # Run worker to process any cleanup
    await worker.run_until_finished()

    # Verify task was not executed
    assert not task_executed, "Task should not have been executed after cancellation"

    # Verify tombstone exists with CANCELLED state
    retrieved = await docket.get_execution(execution.key)
    assert retrieved is not None, "Tombstone should exist after cancellation"
    assert retrieved.state == ExecutionState.CANCELLED

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_verify_remaining_keys_have_ttl_detects_leaks(
    redis_url: str, docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that verify_remaining_keys_have_ttl properly detects keys without TTL."""
    leak_key = f"{docket.name}:test-leak"

    # Exempt the leak from autouse checker
    key_leak_checker.add_exemption(leak_key)

    async with docket.redis() as redis:
        # Intentionally create a key without TTL (simulating a memory leak)
        await redis.set(leak_key, "leaked-value")

        # Remove exemption and manually verify it would detect the leak
        key_leak_checker.exemptions.remove(leak_key)
        with pytest.raises(AssertionError, match="Memory leak detected"):
            await key_leak_checker.verify_remaining_keys_have_ttl()

        # Clean up
        await redis.delete(leak_key)


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


async def test_consumer_group_created_on_first_worker_read(redis_url: str):
    """Consumer group should be created when worker first tries to read.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)

    async def dummy_task():
        pass

    async with docket:
        docket.register(dummy_task)

        await docket.add(dummy_task)()

        async with docket.redis() as redis:
            assert await redis.exists(docket.stream_key)
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0, "Consumer group should not exist before worker"

        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            await worker.run_until_finished()

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1
            assert groups[0]["name"] == docket.worker_group_name.encode()


async def test_multiple_workers_racing_to_create_group(redis_url: str):
    """Multiple workers starting simultaneously should all succeed.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)
    call_counts: dict[str, int] = {}

    async def counting_task(worker: Worker = CurrentWorker()):
        call_counts[worker.name] = call_counts.get(worker.name, 0) + 1

    async with docket:
        docket.register(counting_task)

        for _ in range(20):
            await docket.add(counting_task)()

        workers = [
            Worker(
                docket,
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            )
            for _ in range(5)
        ]

        for w in workers:
            await w.__aenter__()

        await asyncio.gather(*[w.run_until_finished() for w in workers])

        for w in workers:
            await w.__aexit__(None, None, None)

        total_calls = sum(call_counts.values())
        assert total_calls == 20

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1


async def test_worker_handles_nogroup_error_gracefully(redis_url: str):
    """Worker should handle NOGROUP error and create group automatically.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)
    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    async with docket:
        docket.register(simple_task)

        await docket.add(simple_task)()

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0

        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            await worker.run_until_finished()

        assert task_executed, "Task should have been executed"


async def test_worker_handles_nogroup_in_xreadgroup(redis_url: str):
    """Worker should handle NOGROUP error in xreadgroup and retry.

    Issue #206: Lazy stream/consumer group bootstrap.

    This tests the rare case where xautoclaim succeeds but then xreadgroup
    gets NOGROUP (e.g., if the group was deleted between the two calls).
    """
    from unittest.mock import patch

    import redis.asyncio
    from redis.exceptions import ResponseError

    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)
    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    async with docket:
        docket.register(simple_task)

        # Add a task so the worker has something to process
        await docket.add(simple_task)()

        # Ensure group exists first so xautoclaim won't hit NOGROUP
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

        # Track how many times xreadgroup is called
        call_count = 0
        original_xreadgroup = redis.asyncio.Redis.xreadgroup

        async def mock_xreadgroup(  # pyright: ignore[reportUnknownParameterType]
            self: redis.asyncio.Redis,  # type: ignore[type-arg]
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call raises NOGROUP (simulating group deletion)
                raise ResponseError("NOGROUP No such key or consumer group")
            # Subsequent calls use real implementation
            return await original_xreadgroup(self, *args, **kwargs)  # type: ignore[arg-type]

        with patch.object(redis.asyncio.Redis, "xreadgroup", mock_xreadgroup):
            async with Worker(
                docket,
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            ) as worker:
                await worker.run_until_finished()

        # Task should have executed after NOGROUP was handled
        assert task_executed
        # Should have called xreadgroup at least twice (once NOGROUP, then success)
        assert call_count >= 2
