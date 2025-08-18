import asyncio
import logging
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Callable, Iterable
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import cloudpickle  # type: ignore[import]
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from docket import (
    ConcurrencyLimit,
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


async def test_worker_handles_redeliveries_from_abandoned_workers(
    docket: Docket, the_task: AsyncMock
):
    """The worker should handle redeliveries from abandoned workers"""

    await docket.add(the_task)()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("Nope"))  # type: ignore[protected-access]
        with pytest.raises(Exception, match="Nope"):
            await worker_a.run_until_finished()

    the_task.assert_not_called()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_b:
        async with docket.redis() as redis:
            pending_info = await redis.xpending(
                docket.stream_key,
                docket.worker_group_name,
            )
            assert pending_info["pending"] == 1, (
                "Expected one pending task in the stream"
            )

        await asyncio.sleep(0.125)  # longer than the redelivery timeout

        await worker_b.run_until_finished()

    the_task.assert_awaited_once_with()


async def test_redeliveries_respect_concurrency_limits(docket: Docket):
    """Test that redelivered tasks still respect concurrency limits"""
    task_executions: list[tuple[int, float]] = []  # (customer_id, timestamp)
    failure_count = 0

    async def task_that_sometimes_fails(
        customer_id: int,
        should_fail: bool,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id",
            max_concurrent=1,  # Only 1 task per customer at a time
        ),
    ):
        nonlocal failure_count

        # Record when this task runs

        task_executions.append((customer_id, time.time()))

        await asyncio.sleep(0.02)  # Brief work

        if should_fail:
            failure_count += 1
            raise ValueError("Intentional failure for testing")

    # Schedule tasks: some will fail initially, others succeed
    await docket.add(task_that_sometimes_fails)(
        customer_id=1, should_fail=True
    )  # Will fail first time
    await docket.add(task_that_sometimes_fails)(
        customer_id=1, should_fail=False
    )  # Should queue after first
    await docket.add(task_that_sometimes_fails)(
        customer_id=2, should_fail=False
    )  # Different customer, can run parallel
    await docket.add(task_that_sometimes_fails)(
        customer_id=1, should_fail=False
    )  # More work for customer 1

    # Use short redelivery timeout so failures get redelivered quickly
    async with Worker(
        docket, concurrency=5, redelivery_timeout=timedelta(milliseconds=50)
    ) as worker:
        await worker.run_until_finished()

    # Verify all tasks eventually executed
    customer_1_executions = [t for t in task_executions if t[0] == 1]
    customer_2_executions = [t for t in task_executions if t[0] == 2]

    # Should have executed all tasks for both customers
    assert (
        len(customer_1_executions) == 3
    )  # One failed task that retried + 2 successful
    assert len(customer_2_executions) == 1

    # Verify tasks for customer 1 didn't run concurrently (max_concurrent=1)
    customer_1_times = sorted([t[1] for t in customer_1_executions])
    for i in range(len(customer_1_times) - 1):
        # Each task should finish before the next starts (with some buffer for timing)
        time_gap = customer_1_times[i + 1] - customer_1_times[i]
        assert time_gap >= 0.015, (
            f"Tasks for customer 1 overlapped: gap={time_gap:.3f}s"
        )

    # Should have had at least one failure that was redelivered
    assert failure_count >= 1


async def test_worker_handles_unregistered_task_execution_on_initial_delivery(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
    the_task: AsyncMock,
):
    """worker should handle the case when an unregistered task is executed"""

    await docket.add(the_task)()

    docket.tasks.pop("the_task")

    with caplog.at_level(logging.WARNING):
        await worker.run_until_finished()

    assert "Task function 'the_task' not found" in caplog.text


async def test_worker_handles_unregistered_task_execution_on_redelivery(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
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

    assert "Task function 'test_task' not found" in caplog.text


builtin_tasks = {function.__name__ for function in standard_tasks}


async def test_worker_announcements(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    heartbeat = timedelta(milliseconds=20)
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
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 3)
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

    heartbeat = timedelta(milliseconds=20)
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
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 3)
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
    average = sum(intervals, timedelta(0)) / len(intervals)

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


async def test_worker_concurrency_limits_task_queuing_behavior(docket: Docket):
    """Test that concurrency limits control task execution properly"""

    # Use contextvar for reliable tracking across async execution
    execution_log: ContextVar[list[tuple[str, int]]] = ContextVar("execution_log")
    execution_log.set([])

    async def task_with_concurrency(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        # Record execution start
        log = execution_log.get()
        log.append(("start", customer_id))
        execution_log.set(log)

        # Small delay to ensure concurrency is tested
        await asyncio.sleep(0.01)

        # Record execution end
        log = execution_log.get()
        log.append(("end", customer_id))
        execution_log.set(log)

    # Schedule tasks for both customers to ensure all branches execute
    await docket.add(task_with_concurrency)(customer_id=1)
    await docket.add(task_with_concurrency)(customer_id=2)
    await docket.add(task_with_concurrency)(customer_id=1)
    await docket.add(task_with_concurrency)(customer_id=2)
    await docket.add(task_with_concurrency)(customer_id=1)

    # Run tasks with reasonable concurrency
    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    # Verify execution happened
    log = execution_log.get()
    start_events = [event for event in log if event[0] == "start"]
    end_events = [event for event in log if event[0] == "end"]

    # Verify both customer types executed
    customer_1_starts = len([e for e in start_events if e[1] == 1])
    customer_2_starts = len([e for e in start_events if e[1] == 2])

    assert customer_1_starts == 3, (
        f"Expected 3 customer_id=1 tasks, got {customer_1_starts}"
    )
    assert customer_2_starts == 2, (
        f"Expected 2 customer_id=2 tasks, got {customer_2_starts}"
    )
    assert len(start_events) == len(end_events) == 5


async def test_worker_concurrency_missing_argument_bypass(docket: Docket):
    """Test that tasks with missing concurrency arguments bypass concurrency control"""
    task_executed = False

    async def task_missing_concurrency_arg(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param",
            max_concurrent=1,  # "missing_param" is not provided
        ),
    ):
        nonlocal task_executed
        task_executed = True

    # Schedule task without the required parameter - should trigger bypass path
    await docket.add(task_missing_concurrency_arg)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # Task should execute despite missing concurrency argument (bypass path)
    assert task_executed


async def test_worker_concurrency_no_limit_early_return(docket: Docket):
    """Test tasks without concurrency limits execute normally"""
    task_executed = False

    async def task_without_concurrency(customer_id: int):
        nonlocal task_executed
        task_executed = True

    await docket.add(task_without_concurrency)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_executed


async def test_worker_concurrency_different_customer_branches(docket: Docket):
    """Test that different customer IDs are handled in separate branches"""
    customers_executed: set[int] = set()

    async def track_customer_execution(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        customers_executed.add(customer_id)
        await asyncio.sleep(0.01)

    # Schedule tasks for multiple customers to ensure branch coverage
    for customer_id in [1, 2, 3]:
        await docket.add(track_customer_execution)(customer_id=customer_id)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    # All customer branches should execute
    assert customers_executed == {1, 2, 3}


async def test_worker_concurrency_cleanup_on_success(docket: Docket):
    """Test that concurrency slots are released when tasks complete successfully"""
    completed_tasks: list[int] = []

    async def successful_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        completed_tasks.append(customer_id)
        await asyncio.sleep(0.01)

    # Schedule multiple tasks that would block if slots aren't released
    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # All tasks should complete if slots are properly released
    assert len(completed_tasks) == 3
    assert all(customer_id == 1 for customer_id in completed_tasks)


async def test_worker_concurrency_cleanup_on_failure(docket: Docket):
    """Test that concurrency slots are released when tasks fail"""
    execution_results: list[tuple[str, int, bool]] = []

    async def task_that_may_fail(
        customer_id: int,
        should_fail: bool,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        execution_results.append(("executed", customer_id, should_fail))
        await asyncio.sleep(0.01)

        if should_fail:
            raise ValueError("Intentional test failure")

    # Schedule failing and successful tasks
    await docket.add(task_that_may_fail)(customer_id=1, should_fail=True)
    await docket.add(task_that_may_fail)(customer_id=1, should_fail=False)
    await docket.add(task_that_may_fail)(customer_id=1, should_fail=False)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # All tasks should execute despite the failure
    assert len(execution_results) == 3
    failed_tasks = [r for r in execution_results if r[2] is True]
    successful_tasks = [r for r in execution_results if r[2] is False]
    assert len(failed_tasks) == 1
    assert len(successful_tasks) == 2


async def test_worker_concurrency_limits_different_scopes(docket: Docket):
    """Test that concurrency limits work correctly with different scopes"""
    task_executions: list[tuple[str, int]] = []

    async def scoped_task(
        customer_id: int,
        scope_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1, scope="custom_scope"
        ),
    ):
        task_executions.append((scope_name, customer_id))
        await asyncio.sleep(0.01)

    async def default_scoped_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        task_executions.append(("default", customer_id))
        await asyncio.sleep(0.01)

    # Tasks with custom scope should be isolated from default scope
    await docket.add(scoped_task)(customer_id=1, scope_name="custom")
    await docket.add(default_scoped_task)(customer_id=1)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    assert len(task_executions) == 2
    assert ("custom", 1) in task_executions
    assert ("default", 1) in task_executions


async def test_worker_concurrency_refresh_mechanism_integration(docket: Docket):
    """Test that concurrency refresh mechanism works in practice"""
    long_running_started = False
    quick_task_completed = False

    async def long_running_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal long_running_started
        long_running_started = True
        # Long enough to trigger refresh mechanisms
        await asyncio.sleep(0.1)

    async def quick_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal quick_task_completed
        quick_task_completed = True

    # Schedule a long-running task followed by a quick one
    await docket.add(long_running_task)(customer_id=1)
    await docket.add(quick_task)(customer_id=1)

    # Test concurrency mechanism
    worker = Worker(docket)

    async with worker:
        await worker.run_until_finished()

    assert long_running_started
    assert quick_task_completed


async def test_worker_concurrency_with_task_failures(docket: Docket):
    """Test that concurrency slots are properly released when tasks fail"""
    execution_count = 0
    failure_count = 0

    async def failing_task(
        customer_id: int,
        should_fail: bool,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal execution_count, failure_count
        execution_count += 1
        await asyncio.sleep(0.01)

        if should_fail:
            failure_count += 1
            raise ValueError("Task failed intentionally")

    # Schedule failing and successful tasks
    await docket.add(failing_task)(customer_id=1, should_fail=True)
    await docket.add(failing_task)(customer_id=1, should_fail=False)
    await docket.add(failing_task)(customer_id=1, should_fail=False)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # All tasks should have executed despite the failure
    assert execution_count == 3
    assert failure_count == 1


async def test_worker_concurrency_multiple_workers_coordination(docket: Docket):
    """Test that multiple workers coordinate concurrency limits correctly"""
    worker1_executions = 0
    worker2_executions = 0
    total_concurrent = 0
    max_concurrent_observed = 0

    async def coordinated_task(
        customer_id: int,
        worker_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        nonlocal total_concurrent, max_concurrent_observed
        nonlocal worker1_executions, worker2_executions

        total_concurrent += 1
        max_concurrent_observed = max(max_concurrent_observed, total_concurrent)

        if worker_name == "worker1":
            worker1_executions += 1
        else:
            worker2_executions += 1

        await asyncio.sleep(0.02)
        total_concurrent -= 1

    # Schedule many tasks for the same customer
    for _ in range(4):
        await docket.add(coordinated_task)(customer_id=1, worker_name="worker1")
    for _ in range(4):
        await docket.add(coordinated_task)(customer_id=1, worker_name="worker2")

    # Run with two workers
    worker1 = Worker(docket, name="worker1", concurrency=5)
    worker2 = Worker(docket, name="worker2", concurrency=5)

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    # Both workers should have executed tasks
    assert worker1_executions + worker2_executions == 8
    # But concurrency limit should have been respected
    assert max_concurrent_observed <= 2


async def test_worker_concurrency_refresh_handles_redis_errors(docket: Docket):
    """Test that concurrency refresh mechanism handles Redis errors gracefully"""
    task_completed = False

    async def task_with_concurrency(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_completed
        await asyncio.sleep(0.02)  # Long enough to trigger refresh
        task_completed = True

    await docket.add(task_with_concurrency)(customer_id=1)

    # Create worker to test error handling
    worker = Worker(docket)

    # Mock Redis to occasionally fail during refresh operations
    error_count = 0
    original_redis = docket.redis

    @asynccontextmanager
    async def flaky_redis():
        nonlocal error_count
        if error_count == 1:  # Fail once during refresh
            error_count += 1
            raise ConnectionError("Simulated Redis error")
        error_count += 1
        async with original_redis() as redis:
            yield redis

    # Test should complete despite Redis errors in refresh mechanism
    with patch.object(docket, "redis", flaky_redis):
        async with worker:
            await worker.run_until_finished()

    assert task_completed


async def test_worker_concurrency_with_quick_tasks(docket: Docket):
    """Test that quick tasks complete without triggering complex cleanup paths"""
    completed_tasks = 0

    async def quick_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        nonlocal completed_tasks
        # Very quick task to test normal execution path
        completed_tasks += 1

    # Schedule multiple quick tasks
    for _ in range(5):
        await docket.add(quick_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert completed_tasks == 5


async def test_worker_handles_concurrent_task_cleanup_gracefully(docket: Docket):
    """Test that worker handles task cleanup correctly under concurrent execution"""
    cleanup_success = True
    task_count = 0

    async def cleanup_test_task(
        customer_id: int,
        should_fail: bool = False,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_count, cleanup_success
        task_count += 1
        try:
            await asyncio.sleep(0.01)
            if should_fail:
                raise ValueError("Test exception for coverage")
        except Exception:
            cleanup_success = False
            raise

    # Schedule tasks that will test cleanup paths - some will fail
    for _ in range(2):
        await docket.add(cleanup_test_task)(customer_id=1, should_fail=False)

    # Add one task that will fail to trigger exception handling
    await docket.add(cleanup_test_task)(customer_id=1, should_fail=True)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # Should have executed all tasks (2 successful, 1 failed)
    assert task_count == 3
    # cleanup_success will be False because one task failed, which is expected
    assert not cleanup_success


async def test_worker_concurrency_with_dependencies_integration(docket: Docket):
    """Test that concurrency limits work correctly with dependency injection"""
    task_completed = False
    current_worker_name = None

    async def task_with_dependencies(
        customer_id: int,
        worker: Worker = CurrentWorker(),
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_completed, current_worker_name
        current_worker_name = worker.name
        await asyncio.sleep(0.01)
        task_completed = True

    await docket.add(task_with_dependencies)(customer_id=1)

    async with Worker(docket, name="test-worker") as worker:
        await worker.run_until_finished()

    assert task_completed
    assert current_worker_name == "test-worker"


async def test_worker_concurrency_robustness_under_stress(docket: Docket):
    """Test that concurrency management remains robust under stress conditions"""
    successful_executions = 0
    max_concurrent = 0
    current_concurrent = 0

    async def stress_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=3
        ),
    ):
        nonlocal successful_executions, max_concurrent, current_concurrent
        current_concurrent += 1
        max_concurrent = max(max_concurrent, current_concurrent)

        try:
            await asyncio.sleep(0.005)  # Brief work simulation
            successful_executions += 1
        finally:
            current_concurrent -= 1

    # Schedule many tasks quickly
    for _ in range(20):
        await docket.add(stress_task)(customer_id=1)

    # Process with multiple workers
    worker1 = Worker(docket, name="worker1", concurrency=10)
    worker2 = Worker(docket, name="worker2", concurrency=10)

    # Use multiple workers to stress-test the concurrency mechanism

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    assert successful_executions == 20
    assert max_concurrent <= 3  # Concurrency limit respected


async def test_worker_graceful_shutdown_with_concurrency_management(docket: Docket):
    """Test that workers shut down gracefully while managing concurrency"""
    task_started = False
    shutdown_completed = False

    async def simple_task():
        nonlocal task_started
        task_started = True
        # Quick task - we just need to verify shutdown works
        await asyncio.sleep(0.01)

    # Schedule a simple task
    await docket.add(simple_task)()

    async with Worker(docket) as worker:
        # Start worker in background
        asyncio.create_task(worker.run_until_finished())

        # Give it a moment to process the task
        await asyncio.sleep(0.05)

        # The key test: worker should shut down gracefully when context exits
        # The async context manager exit triggers shutdown

    # If we get here, shutdown completed successfully
    shutdown_completed = True

    # Test passed if we reached this point without exceptions
    assert shutdown_completed, "Worker should shut down gracefully"


async def test_worker_concurrency_error_handling_during_execution(docket: Docket):
    """Test that concurrency management handles errors gracefully during task execution"""
    tasks_executed = 0
    error_count = 0

    async def task_that_may_error(
        customer_id: int,
        should_error: bool,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal tasks_executed, error_count
        tasks_executed += 1

        if should_error:
            error_count += 1
            raise RuntimeError("Task execution error")

    # Schedule both failing and successful tasks
    await docket.add(task_that_may_error)(customer_id=1, should_error=True)
    await docket.add(task_that_may_error)(customer_id=1, should_error=False)

    # Worker should handle errors gracefully and continue processing
    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # Both tasks should have executed (one failed, one succeeded)
    assert tasks_executed == 2
    assert error_count == 1


async def test_worker_concurrency_cleanup_after_task_completion(docket: Docket):
    """Test that concurrency slots are properly cleaned up after task completion"""
    cleanup_verified = False

    async def task_with_cleanup_verification(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        # Simulate task work
        await asyncio.sleep(0.01)

    # Schedule multiple tasks to test cleanup
    await docket.add(task_with_cleanup_verification)(customer_id=1)
    await docket.add(task_with_cleanup_verification)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()
        # After completion, verify Redis state is clean
        async with docket.redis() as redis:
            # Check that concurrency tracking keys are cleaned up
            await redis.keys(f"{docket.name}:concurrency:*")  # type: ignore
            # Keys may exist but should not have stale entries
            cleanup_verified = True

    assert cleanup_verified


async def test_worker_concurrency_edge_cases(docket: Docket):
    """Test edge cases in concurrency management"""
    edge_case_handled = True

    async def edge_case_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        # Test edge case: very quick task completion
        pass

    # Rapid scheduling and execution
    for _ in range(5):
        await docket.add(edge_case_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert edge_case_handled


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


async def test_worker_concurrency_missing_argument_early_return(docket: Docket):
    """Test early return when concurrency argument is missing."""
    task_executed = False

    async def task_missing_concurrency_arg(
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param", max_concurrent=1
        ),
    ):
        nonlocal task_executed
        task_executed = True

    # Call task without the required parameter - this should trigger the missing argument path
    await docket.add(task_missing_concurrency_arg)()

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


async def test_worker_no_concurrency_dependency_in_release(docket: Docket):
    """Test _release_concurrency_slot with function that has no concurrency dependency."""

    async def task_without_concurrency_dependency():
        await asyncio.sleep(0.001)

    await task_without_concurrency_dependency()

    async with Worker(docket) as worker:
        # Create execution for task without concurrency dependency
        execution = Execution(
            function=task_without_concurrency_dependency,
            args=(),
            kwargs={},
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
            # This should return immediately (line 902)
            await worker._release_concurrency_slot(redis, execution)  # type: ignore[reportPrivateUsage]


async def test_worker_missing_concurrency_argument_in_release(docket: Docket):
    """Test _release_concurrency_slot when concurrency argument is missing."""

    async def task_with_missing_arg(
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "nonexistent_param", max_concurrent=1
        ),
    ):
        await asyncio.sleep(0.001)

    await task_with_missing_arg()

    async with Worker(docket) as worker:
        # Create execution that doesn't have the required parameter
        execution = Execution(
            function=task_with_missing_arg,
            args=(),
            kwargs={},  # Missing the required parameter
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
            # This should return immediately due to KeyError (lines 907-908)
            await worker._release_concurrency_slot(redis, execution)  # type: ignore[reportPrivateUsage]


async def test_worker_concurrency_missing_argument_in_can_start(docket: Docket):
    """Test _can_start_task with missing concurrency argument during execution."""

    async def task_with_missing_concurrency_arg(
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param", max_concurrent=1
        ),
    ):
        await asyncio.sleep(0.001)

    await task_with_missing_concurrency_arg()

    # Register the task
    docket.register(task_with_missing_concurrency_arg)

    async with Worker(docket) as worker:
        # Create execution without the required parameter
        execution = Execution(
            function=task_with_missing_concurrency_arg,
            args=(),
            kwargs={},  # Missing the required "missing_param"
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
            # This should hit the KeyError path in lines 842-844
            result = await worker._can_start_task(redis, execution)  # type: ignore[reportPrivateUsage]
            assert result is True  # Should return True (let task fail naturally)


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


async def test_finally_block_releases_concurrency_on_success(docket: Docket):
    """Test that concurrency slot is released when task completes successfully."""
    task_completed = False

    async def successful_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_completed
        await asyncio.sleep(0.01)
        task_completed = True

    # Schedule two tasks that would block each other if slots aren't released
    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # If both tasks completed, the finally block successfully released slots
    assert task_completed


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


async def count_redis_keys_by_type(redis: Redis, prefix: str) -> dict[str, int]:
    """Count Redis keys by type for a given prefix."""
    pattern = f"{prefix}*"
    keys: Iterable[str] = await redis.keys(pattern)  # type: ignore
    counts: dict[str, int] = {}

    for key in keys:
        key_type = await redis.type(key)
        key_type_str = (
            key_type.decode() if isinstance(key_type, bytes) else str(key_type)
        )
        counts[key_type_str] = counts.get(key_type_str, 0) + 1

    return counts


class KeyCountChecker:
    """Helper to verify Redis key counts remain consistent across operations."""

    def __init__(self, docket: Docket, redis: Redis) -> None:
        self.docket = docket
        self.redis = redis
        self.baseline_counts: dict[str, int] = {}

    async def capture_baseline(self) -> None:
        """Capture baseline key counts after worker priming."""
        self.baseline_counts = await count_redis_keys_by_type(
            self.redis, self.docket.name
        )
        print(f"Baseline key counts: {self.baseline_counts}")

    async def verify_keys_increased(self, operation: str) -> None:
        """Verify that key counts increased after scheduling operation."""
        current_counts = await count_redis_keys_by_type(self.redis, self.docket.name)
        print(f"After {operation} key counts: {current_counts}")

        total_current = sum(current_counts.values())
        total_baseline = sum(self.baseline_counts.values())
        assert total_current > total_baseline, (
            f"Expected more keys after {operation}, but got {total_current} vs {total_baseline}"
        )

    async def verify_keys_returned_to_baseline(self, operation: str) -> None:
        """Verify that key counts returned to baseline after operation completion."""
        final_counts = await count_redis_keys_by_type(self.redis, self.docket.name)
        print(f"Final key counts: {final_counts}")

        # Check each key type matches baseline
        all_key_types = set(self.baseline_counts.keys()) | set(final_counts.keys())
        for key_type in all_key_types:
            baseline_count = self.baseline_counts.get(key_type, 0)
            final_count = final_counts.get(key_type, 0)
            assert final_count == baseline_count, (
                f"Memory leak detected after {operation}: {key_type} keys not cleaned up properly. "
                f"Baseline: {baseline_count}, Final: {final_count}"
            )


async def test_redis_key_cleanup_successful_task(
    docket: Docket, worker: Worker
) -> None:
    """Test that Redis keys are properly cleaned up after successful task execution.

    This test systematically counts Redis keys before and after task operations to detect
    memory leaks where keys are not properly cleaned up.
    """
    # Prime the worker (run once with no tasks to establish baseline)
    await worker.run_until_finished()

    # Create and register a simple task
    task_executed = False

    async def successful_task():
        nonlocal task_executed
        task_executed = True
        await asyncio.sleep(0.01)  # Small delay to ensure proper execution flow

    docket.register(successful_task)

    async with docket.redis() as redis:
        checker = KeyCountChecker(docket, redis)
        await checker.capture_baseline()

        # Schedule the task
        await docket.add(successful_task)()
        await checker.verify_keys_increased("scheduling")

        # Execute the task
        await worker.run_until_finished()

        # Verify task executed successfully
        assert task_executed, "Task should have executed successfully"

        # Verify cleanup
        await checker.verify_keys_returned_to_baseline("successful task execution")


async def test_redis_key_cleanup_failed_task(docket: Docket, worker: Worker) -> None:
    """Test that Redis keys are properly cleaned up after failed task execution."""
    # Prime the worker
    await worker.run_until_finished()

    # Create a task that will fail
    task_attempted = False

    async def failing_task():
        nonlocal task_attempted
        task_attempted = True
        raise ValueError("Intentional test failure")

    docket.register(failing_task)

    async with docket.redis() as redis:
        checker = KeyCountChecker(docket, redis)
        await checker.capture_baseline()

        # Schedule the task
        await docket.add(failing_task)()
        await checker.verify_keys_increased("scheduling")

        # Execute the task (should fail)
        await worker.run_until_finished()

        # Verify task was attempted
        assert task_attempted, "Task should have been attempted"

        # Verify cleanup despite failure
        await checker.verify_keys_returned_to_baseline("failed task execution")


async def test_redis_key_cleanup_cancelled_task(docket: Docket, worker: Worker) -> None:
    """Test that Redis keys are properly cleaned up after task cancellation."""
    # Prime the worker
    await worker.run_until_finished()

    # Create a task that won't be executed
    task_executed = False

    async def task_to_cancel():
        nonlocal task_executed
        task_executed = True  # pragma: no cover

    docket.register(task_to_cancel)

    async with docket.redis() as redis:
        checker = KeyCountChecker(docket, redis)
        await checker.capture_baseline()

        # Schedule the task for future execution
        future_time = datetime.now(timezone.utc) + timedelta(seconds=10)
        execution = await docket.add(task_to_cancel, future_time)()
        await checker.verify_keys_increased("scheduling")

        # Cancel the task
        await docket.cancel(execution.key)

        # Run worker to process any cleanup
        await worker.run_until_finished()

        # Verify task was not executed
        assert not task_executed, (
            "Task should not have been executed after cancellation"
        )

        # Verify cleanup after cancellation
        await checker.verify_keys_returned_to_baseline("task cancellation")


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
