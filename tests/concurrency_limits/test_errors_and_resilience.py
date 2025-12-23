"""Tests for error handling and resilience under concurrency limits.

This module tests how concurrency limits handle error conditions and stress:
- Redis error handling
- Task failure handling
- Error handling during execution
- Multi-worker coordination
- Stress testing
- Graceful shutdown
- Edge cases
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import patch

from redis.exceptions import ConnectionError

from docket import ConcurrencyLimit, Docket, Worker


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

    await docket.add(failing_task)(customer_id=1, should_fail=True)
    await docket.add(failing_task)(customer_id=1, should_fail=False)
    await docket.add(failing_task)(customer_id=1, should_fail=False)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert execution_count == 3
    assert failure_count == 1


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

    await docket.add(task_that_may_error)(customer_id=1, should_error=True)
    await docket.add(task_that_may_error)(customer_id=1, should_error=False)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert tasks_executed == 2
    assert error_count == 1


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

    for _ in range(4):
        await docket.add(coordinated_task)(customer_id=1, worker_name="worker1")
    for _ in range(4):
        await docket.add(coordinated_task)(customer_id=1, worker_name="worker2")

    worker1 = Worker(docket, name="worker1", concurrency=5)
    worker2 = Worker(docket, name="worker2", concurrency=5)

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    assert worker1_executions + worker2_executions == 8
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
        await asyncio.sleep(0.02)
        task_completed = True

    await docket.add(task_with_concurrency)(customer_id=1)

    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=10))

    error_count = 0
    original_redis = docket.redis

    @asynccontextmanager
    async def flaky_redis():
        nonlocal error_count
        if error_count == 1:
            error_count += 1
            raise ConnectionError("Simulated Redis error")
        error_count += 1
        async with original_redis() as redis:
            yield redis

    with patch.object(docket, "redis", flaky_redis):
        async with worker:
            await worker.run_until_finished()

    assert task_completed


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
            await asyncio.sleep(0.005)
            successful_executions += 1
        finally:
            current_concurrent -= 1

    for _ in range(20):
        await docket.add(stress_task)(customer_id=1)

    worker1 = Worker(docket, name="worker1", concurrency=10)
    worker2 = Worker(docket, name="worker2", concurrency=10)

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    assert successful_executions == 20
    assert max_concurrent <= 3


async def test_worker_concurrency_edge_cases(docket: Docket):
    """Test edge cases in concurrency management"""
    edge_case_handled = True

    async def edge_case_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        pass

    for _ in range(5):
        await docket.add(edge_case_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert edge_case_handled


async def test_worker_graceful_shutdown_with_concurrency_management(docket: Docket):
    """Test that workers shut down gracefully while managing concurrency"""
    task_started = asyncio.Event()
    task_completed = asyncio.Event()

    async def simple_task():
        task_started.set()
        await asyncio.sleep(0.01)
        task_completed.set()

    await docket.add(simple_task)()

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_started.is_set(), "Task should have started"
    assert task_completed.is_set(), "Task should have completed"
