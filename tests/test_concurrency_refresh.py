import asyncio
import time
from datetime import timedelta


from docket import ConcurrencyLimit, Docket, Worker


async def test_concurrency_refresh_manager_for_long_running_tasks(docket: Docket):
    """Test that the refresh manager keeps long-running tasks' concurrency slots alive."""
    task_started = False
    task_completed = False

    async def long_running_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_started, task_completed
        task_started = True

        # Simulate a long-running task (longer than refresh threshold)
        await asyncio.sleep(4)  # Run for 4 seconds

        task_completed = True

    # Create a worker with short refresh settings for testing
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(
            seconds=5
        ),  # This sets refresh threshold to 2.5 seconds
    ) as worker:
        # Override refresh interval for faster testing
        worker.concurrency_refresh_interval = timedelta(seconds=1)

        # Schedule the long-running task
        await docket.add(long_running_task)(customer_id=1)

        # Start the worker and let it run
        await worker.run_until_finished()

        # Verify the task started and completed successfully
        assert task_started, "Task should have started"
        assert task_completed, (
            "Task should have completed despite running longer than refresh threshold"
        )


async def test_concurrency_refresh_manager_batch_operations(docket: Docket):
    """Test that the refresh manager batches multiple slot refreshes efficiently."""
    tasks_started: list[int] = []
    tasks_completed: list[int] = []

    async def long_running_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        tasks_started.append(customer_id)

        # Simulate a long-running task
        await asyncio.sleep(3)

        tasks_completed.append(customer_id)

    # Create a worker with settings that will trigger refresh
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=4),  # Refresh threshold = 2 seconds
    ) as worker:
        # Override refresh interval for faster testing
        worker.concurrency_refresh_interval = timedelta(seconds=1)

        # Schedule multiple tasks for the same customer (will run concurrently up to limit)
        for _ in range(3):  # 3 tasks, but max_concurrent=2
            await docket.add(long_running_task)(customer_id=1)

        # Start the worker and let it run
        await worker.run_until_finished()

        # Verify that 2 tasks ran concurrently, and the third ran after
        assert len(tasks_started) == 3, "All tasks should have started"
        assert len(tasks_completed) == 3, "All tasks should have completed"


async def test_concurrency_refresh_manager_cleanup_on_completion(docket: Docket):
    """Test that completed tasks are removed from the refresh manager."""
    task_execution_times: list[tuple[float, float]] = []

    async def tracked_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        start_time = time.time()
        await asyncio.sleep(0.5)  # Short task
        end_time = time.time()
        task_execution_times.append((start_time, end_time))

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=10),
    ) as worker:
        # Schedule several tasks
        for _ in range(3):
            await docket.add(tracked_task)(customer_id=1)

        # Run tasks
        await worker.run_until_finished()

        # Verify all tasks completed
        assert len(task_execution_times) == 3

        # Verify that the refresh manager's tracking dict is empty after completion
        assert len(worker.concurrency_slots_needing_refresh) == 0, (
            "Refresh manager should have cleaned up all completed tasks"
        )


async def test_short_tasks_dont_trigger_refresh(docket: Docket):
    """Test that short tasks don't unnecessarily trigger refresh operations."""

    async def short_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        await asyncio.sleep(0.1)  # Very short task

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
        redelivery_timeout=timedelta(seconds=10),  # Refresh threshold = 5 seconds
    ) as worker:
        # Override refresh interval for testing
        worker.concurrency_refresh_interval = timedelta(seconds=0.5)

        # Schedule multiple short tasks
        for _ in range(5):
            await docket.add(short_task)(customer_id=1)

        # Give time for potential refresh operations
        start_time = time.time()
        await worker.run_until_finished()
        total_time = time.time() - start_time

        # Tasks should complete quickly (well under refresh threshold)
        assert total_time < 2.0, f"Short tasks took too long: {total_time:.2f}s"

        # Verify no slots are left in the refresh manager
        assert len(worker.concurrency_slots_needing_refresh) == 0
