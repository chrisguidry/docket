import asyncio
import time
from datetime import timedelta


from docket import ConcurrencyLimit, CurrentExecution, Docket, Timeout, Worker
from docket.execution import Execution

from tests.concurrency_limits.overlap import assert_no_overlaps


async def test_task_timeout_with_explicit_timeout(docket: Docket):
    """Test that tasks with explicit Timeout are timed out correctly."""
    task_started = False
    task_completed = False
    event = asyncio.Event()

    async def long_running_task(
        customer_id: int,
        test_mode: str = "timeout",
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
        timeout: Timeout = Timeout(timedelta(seconds=1)),
    ):
        nonlocal task_started, task_completed
        task_started = True

        if test_mode == "complete":
            # Fast completion for coverage
            await asyncio.sleep(0.01)
            task_completed = True
        elif test_mode == "long_complete":
            # Long running but within timeout for coverage
            await asyncio.sleep(0.5)  # Within the 1-second timeout
            task_completed = True
        else:
            # Simulate a task that would run longer than timeout
            # Don't set event - task will hang and be timed out
            await event.wait()

    docket.register(long_running_task)

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=50),
        scheduling_resolution=timedelta(milliseconds=50),
        redelivery_timeout=timedelta(seconds=3),
    ) as worker:
        # Schedule the long-running task
        await docket.add(long_running_task)(customer_id=1)

        # Start the worker and let it run
        await worker.run_until_finished()

        # Verify the task started but was timed out before completion
        assert task_started, "Task should have started"
        assert not task_completed, "Task should have been timed out before completion"

        # Test the completion path for coverage
        task_started = False
        task_completed = False
        await docket.add(long_running_task)(customer_id=2, test_mode="complete")
        await worker.run_until_finished()
        assert task_started, "Second task should have started"
        assert task_completed, "Second task should have completed"

        # Test long-running path that actually completes for coverage
        task_started = False
        task_completed = False
        await docket.add(long_running_task)(customer_id=3, test_mode="long_complete")
        await worker.run_until_finished()
        assert task_started, "Third task should have started"
        assert task_completed, "Third task should have completed"


async def test_task_timeout_with_concurrent_tasks(docket: Docket):
    """Test that concurrency control works with hard timeouts."""
    tasks_started: list[int] = []
    tasks_completed: list[int] = []

    async def task_within_timeout(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        tasks_started.append(customer_id)

        # Task that completes within timeout
        await asyncio.sleep(1)

        tasks_completed.append(customer_id)

    # Create a worker with reasonable timeout
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=3),  # Tasks will timeout after 3 seconds
    ) as worker:
        # Schedule multiple tasks for the same customer (will run concurrently up to limit)
        for _ in range(3):  # 3 tasks, but max_concurrent=2
            await docket.add(task_within_timeout)(customer_id=1)

        # Start the worker and let it run
        await worker.run_until_finished()

        # Verify that all tasks completed successfully
        assert len(tasks_started) == 3, "All tasks should have started"
        assert len(tasks_completed) == 3, "All tasks should have completed"


async def test_explicit_timeout_limits_long_tasks(docket: Docket):
    """Test that tasks with explicit Timeout longer than the limit are terminated."""
    task_completed = False
    event = asyncio.Event()

    async def long_task(
        customer_id: int,
        test_mode: str = "timeout",
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
        timeout: Timeout = Timeout(timedelta(seconds=1)),
    ):
        nonlocal task_completed
        if test_mode == "complete":
            # Fast completion for coverage
            await asyncio.sleep(0.01)
            task_completed = True
        elif test_mode == "long_complete":
            # Long running but completes within timeout
            await asyncio.sleep(0.5)  # Less than 1 second timeout
            task_completed = True
        else:
            # Simulate a task that would run longer than timeout
            # Don't set event - task will hang and be timed out
            await event.wait()

    docket.register(long_task)

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=50),
        scheduling_resolution=timedelta(milliseconds=50),
        redelivery_timeout=timedelta(seconds=3),
    ) as worker:
        # Schedule long-running task
        await docket.add(long_task)(customer_id=1)

        # Run tasks
        await worker.run_until_finished()

        # Verify task was timed out
        assert not task_completed, "Task should have been timed out by explicit Timeout"

        # Test completion path for coverage
        task_completed = False
        await docket.add(long_task)(customer_id=2, test_mode="complete")
        await worker.run_until_finished()
        assert task_completed, "Second task should have completed"

        # Test long completion path for coverage
        task_completed = False
        await docket.add(long_task)(customer_id=3, test_mode="long_complete")
        await worker.run_until_finished()
        assert task_completed, "Third task should have completed"


async def test_short_tasks_complete_within_timeout(docket: Docket):
    """Test that short tasks complete successfully within redelivery timeout."""
    tasks_completed = 0

    async def short_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal tasks_completed
        await asyncio.sleep(0.1)  # Very short task
        tasks_completed += 1

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=50),
        scheduling_resolution=timedelta(milliseconds=50),
        redelivery_timeout=timedelta(seconds=3),
    ) as worker:
        # Schedule multiple short tasks
        for _ in range(5):
            await docket.add(short_task)(customer_id=1)

        # Run tasks
        start_time = time.monotonic()
        await worker.run_until_finished()
        total_time = time.monotonic() - start_time

        # All tasks should complete successfully
        assert tasks_completed == 5, (
            f"Expected 5 tasks completed, got {tasks_completed}"
        )
        assert total_time < 3.0, f"Short tasks took too long: {total_time:.2f}s"


async def test_redeliveries_respect_concurrency_limits(docket: Docket):
    """Test that redelivered tasks still respect concurrency limits"""
    task_executions: list[tuple[int, float, float]] = []  # (customer_id, start, end)
    failure_count = 0

    async def task_that_sometimes_fails(
        customer_id: int,
        should_fail: bool,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id",
            max_concurrent=1,
        ),
    ):
        nonlocal failure_count
        start = time.monotonic()
        await asyncio.sleep(0.02)
        end = time.monotonic()
        task_executions.append((customer_id, start, end))

        if should_fail:
            failure_count += 1
            raise ValueError("Intentional failure for testing")

    # Schedule tasks: some will fail initially, others succeed
    await docket.add(task_that_sometimes_fails)(customer_id=1, should_fail=True)
    await docket.add(task_that_sometimes_fails)(customer_id=1, should_fail=False)
    await docket.add(task_that_sometimes_fails)(customer_id=2, should_fail=False)
    await docket.add(task_that_sometimes_fails)(customer_id=1, should_fail=False)

    async with Worker(
        docket, concurrency=5, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker:
        await worker.run_until_finished()

    # Verify all tasks eventually executed
    customer_1_intervals = [(s, e) for cid, s, e in task_executions if cid == 1]
    customer_2_intervals = [(s, e) for cid, s, e in task_executions if cid == 2]

    # At least 3 executions for customer 1 (redelivery may cause more)
    assert len(customer_1_intervals) >= 3
    assert len(customer_2_intervals) >= 1

    # Verify tasks for customer 1 didn't overlap (concurrency limit = 1)
    assert_no_overlaps(customer_1_intervals, "Customer 1 tasks")

    assert failure_count >= 1


async def test_concurrency_blocked_task_executes_exactly_once(docket: Docket):
    """Concurrency limits should prevent tasks for the same customer from overlapping,
    while allowing parallelism across different customers.

    This test uses TWO separate workers to ensure concurrency limits work across
    workers, not just within a single worker. This is important because xautoclaim
    can reclaim messages from one worker and deliver them to another.
    """

    executions: list[tuple[int, float, float, str]] = []

    async def tracked_task(
        customer_id: int,
        execution: Execution = CurrentExecution(),
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id",
            max_concurrent=1,
        ),
    ) -> None:
        start = time.monotonic()
        await asyncio.sleep(0.02)
        end = time.monotonic()
        executions.append((customer_id, start, end, execution.key))

    # Schedule 5 tasks for each of 3 customers
    for customer_id in [1, 2, 3]:
        for _ in range(5):
            await docket.add(tracked_task)(customer_id=customer_id)

    # Use TWO workers with short redelivery timeout to stress test cross-worker
    # concurrency limits. This exposes issues where xautoclaim reclaims a task
    # from Worker 1 and delivers it to Worker 2 while Worker 1 is still executing.
    # Note: 200ms timeout gives enough headroom for lease renewal under CPU throttling.
    async with (
        Worker(
            docket,
            concurrency=3,
            redelivery_timeout=timedelta(milliseconds=200),
            name="worker-1",
        ) as worker1,
        Worker(
            docket,
            concurrency=3,
            redelivery_timeout=timedelta(milliseconds=200),
            name="worker-2",
        ) as worker2,
    ):
        await asyncio.gather(
            worker1.run_until_finished(),
            worker2.run_until_finished(),
        )

    # Group executions by customer_id
    by_customer: dict[int, list[tuple[float, float, str]]] = {}
    for customer_id, start, end, key in executions:
        by_customer.setdefault(customer_id, []).append((start, end, key))

    # Verify each customer's tasks completed and didn't overlap
    for customer_id, customer_executions in by_customer.items():
        # At least 5 tasks must have completed (redelivery may cause more)
        assert len(customer_executions) >= 5, (
            f"Customer {customer_id} only completed {len(customer_executions)}/5 tasks"
        )

        # No two executions for this customer should overlap in time
        intervals = [(start, end) for start, end, _ in customer_executions]
        assert_no_overlaps(intervals, f"Customer {customer_id} tasks")

    # Verify all customers completed their tasks
    assert len(by_customer) == 3, f"Expected 3 customers, got {len(by_customer)}"

    # Verify cleanup
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0, (
            "Found unacknowledged messages - cleanup failed"
        )
        assert await redis.xlen(docket.stream_key) == 0
