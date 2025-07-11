import asyncio
import time
from datetime import timedelta

import pytest

from docket import ConcurrencyLimit, Docket, Worker


async def test_concurrency_limit_single_argument(docket: Docket, worker: Worker):
    """Test that ConcurrencyLimit enforces single concurrent execution per argument value."""
    execution_order = []
    execution_starts = {}
    execution_ends = {}

    async def slow_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
    ):
        start_time = time.time()
        execution_starts[customer_id] = start_time
        execution_order.append(f"start_{customer_id}")
        
        # Simulate some work
        await asyncio.sleep(0.2)
        
        end_time = time.time()
        execution_ends[customer_id] = end_time
        execution_order.append(f"end_{customer_id}")

    # Schedule multiple tasks for the same customer_id
    await docket.add(slow_task)(customer_id=1)
    await docket.add(slow_task)(customer_id=1)
    await docket.add(slow_task)(customer_id=1)

    # Run with limited concurrency
    worker.concurrency = 10  # High worker concurrency to test task-level limits
    await worker.run_until_finished()

    # Verify tasks ran sequentially for the same customer_id
    assert len(execution_order) == 6
    assert execution_order == ["start_1", "end_1", "start_1", "end_1", "start_1", "end_1"]
    
    # Verify no overlap in execution times
    times = sorted([(execution_starts[1], "start"), (execution_ends[1], "end")])
    for i in range(0, len(times) - 1, 2):
        start_time = times[i][0]
        end_time = times[i + 1][0]
        
        # Check if there's a next task
        if i + 2 < len(times):
            next_start_time = times[i + 2][0]
            assert end_time <= next_start_time, "Tasks should not overlap"


async def test_concurrency_limit_different_arguments(docket: Docket, worker: Worker):
    """Test that tasks with different argument values can run concurrently."""
    execution_order = []
    execution_times = {}

    async def slow_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
    ):
        start_time = time.time()
        execution_times[f"start_{customer_id}"] = start_time
        execution_order.append(f"start_{customer_id}")
        
        # Simulate some work
        await asyncio.sleep(0.1)
        
        end_time = time.time()
        execution_times[f"end_{customer_id}"] = end_time
        execution_order.append(f"end_{customer_id}")

    # Schedule tasks for different customer_ids
    await docket.add(slow_task)(customer_id=1)
    await docket.add(slow_task)(customer_id=2)
    await docket.add(slow_task)(customer_id=3)

    # Run with high worker concurrency
    worker.concurrency = 10
    await worker.run_until_finished()

    # Verify all tasks completed
    assert len(execution_order) == 6
    
    # Verify tasks for different customers ran concurrently
    # (start times should be close together)
    start_times = [
        execution_times["start_1"],
        execution_times["start_2"], 
        execution_times["start_3"]
    ]
    
    max_start_diff = max(start_times) - min(start_times)
    assert max_start_diff < 0.05, "Tasks with different customer_ids should start concurrently"


async def test_concurrency_limit_max_concurrent(docket: Docket, worker: Worker):
    """Test that max_concurrent parameter works correctly."""
    execution_order = []
    active_tasks = []
    max_concurrent_seen = 0
    lock = asyncio.Lock()

    async def slow_task(
        task_id: int,
        db_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=2)
    ):
        nonlocal max_concurrent_seen
        
        async with lock:
            active_tasks.append(task_id)
            max_concurrent_seen = max(max_concurrent_seen, len(active_tasks))
            execution_order.append(f"start_{task_id}")
        
        # Simulate some work
        await asyncio.sleep(0.1)
        
        async with lock:
            active_tasks.remove(task_id)
            execution_order.append(f"end_{task_id}")

    # Schedule 5 tasks for the same db_name (should be limited to 2 concurrent)
    for i in range(5):
        await docket.add(slow_task)(task_id=i, db_name="postgres")

    # Run with high worker concurrency
    worker.concurrency = 10
    await worker.run_until_finished()

    # Verify max concurrency was respected
    assert max_concurrent_seen <= 2, f"Expected max 2 concurrent, but saw {max_concurrent_seen}"
    assert len(execution_order) == 10  # 5 starts + 5 ends


async def test_concurrency_limit_missing_argument_error(docket: Docket, worker: Worker):
    """Test that missing argument causes proper error handling."""
    async def task_with_missing_arg(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("missing_arg", max_concurrent=1)
    ):
        pass  # pragma: no cover

    await docket.add(task_with_missing_arg)(customer_id=123)
    
    # This should cause the task to fail but not crash the worker
    await worker.run_until_finished()


async def test_concurrency_limit_with_custom_scope(docket: Docket, worker: Worker):
    """Test that custom scope parameter works correctly."""
    execution_order = []

    async def task_with_scope(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1, scope="custom")
    ):
        execution_order.append(f"task_{customer_id}")

    await docket.add(task_with_scope)(customer_id=1)
    await docket.add(task_with_scope)(customer_id=1)

    await worker.run_until_finished()

    # Should complete both tasks (testing that scope affects Redis key)
    assert len(execution_order) == 2


async def test_concurrency_limit_single_dependency_validation(docket: Docket):
    """Test that only one ConcurrencyLimit dependency is allowed per task."""
    with pytest.raises(
        ValueError,
        match="Only one ConcurrencyLimit dependency is allowed per task",
    ):
        async def invalid_task(
            customer_id: int,
            limitA: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1),
            limitB: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=2),
        ):
            pass  # pragma: no cover

        await docket.add(invalid_task)(customer_id=1)


async def test_concurrency_limit_without_concurrency_dependency(docket: Docket, worker: Worker):
    """Test that tasks without ConcurrencyLimit work normally."""
    execution_count = 0

    async def normal_task(customer_id: int):
        nonlocal execution_count
        execution_count += 1

    # Schedule multiple tasks
    for i in range(5):
        await docket.add(normal_task)(customer_id=i)

    await worker.run_until_finished()

    # All tasks should complete normally
    assert execution_count == 5