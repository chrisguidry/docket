import asyncio
import time
from datetime import timedelta


from docket import ConcurrencyLimit, Docket, Worker


async def test_task_timeout_respects_redelivery_timeout(docket: Docket):
    """Test that tasks are automatically timed out at the redelivery timeout."""
    task_started = False
    task_completed = False

    async def long_running_task(
        customer_id: int,
        test_mode: str = "timeout",  # Add parameter to control behavior
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_started, task_completed
        task_started = True

        if test_mode == "complete":
            # Fast completion for coverage
            await asyncio.sleep(0.01)
            task_completed = True
        elif test_mode == "long_complete":
            # Long running but within timeout for coverage
            await asyncio.sleep(1.5)  # Within the 2-second timeout
            task_completed = True
        else:
            # Simulate a task that would run longer than redelivery timeout
            await asyncio.sleep(4)  # This will be cancelled by timeout

    # Create a worker with short redelivery timeout
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(
            seconds=2
        ),  # Tasks will be timed out after 2 seconds
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


async def test_redelivery_timeout_limits_long_tasks(docket: Docket):
    """Test that tasks longer than redelivery timeout are terminated."""
    task_completed = False

    async def long_task(
        customer_id: int,
        test_mode: str = "timeout",  # Add parameter to control behavior
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_completed
        if test_mode == "complete":
            # Fast completion for coverage
            await asyncio.sleep(0.01)
            task_completed = True
        elif test_mode == "long_complete":
            # Long running but completes within timeout
            await asyncio.sleep(0.8)  # Less than 1 second timeout
            task_completed = True
        else:
            # This will be cancelled by timeout
            await asyncio.sleep(3)  # Try to run for 3 seconds

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=1),  # Short timeout
    ) as worker:
        # Schedule long-running task
        await docket.add(long_task)(customer_id=1)

        # Run tasks
        await worker.run_until_finished()

        # Verify task was timed out
        assert not task_completed, (
            "Task should have been timed out by redelivery timeout"
        )

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
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=2),  # Reasonable timeout
    ) as worker:
        # Schedule multiple short tasks
        for _ in range(5):
            await docket.add(short_task)(customer_id=1)

        # Run tasks
        start_time = time.time()
        await worker.run_until_finished()
        total_time = time.time() - start_time

        # All tasks should complete successfully
        assert tasks_completed == 5, (
            f"Expected 5 tasks completed, got {tasks_completed}"
        )
        assert total_time < 2.0, f"Short tasks took too long: {total_time:.2f}s"
