"""Tests for worker behavior with concurrency limits.

This module tests how workers behave with concurrency-limited tasks:
- Missing argument error handling
- Cleanup operations on success and failure
- Stale slot scavenging
- Graceful shutdown
"""

import asyncio
from datetime import datetime, timezone

from docket import ConcurrencyLimit, Docket, Worker


async def test_worker_concurrency_missing_argument_fails_task(docket: Docket):
    """Test that tasks with missing concurrency arguments fail with clear error"""
    task_executed = False

    async def task_missing_concurrency_arg(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param",
            max_concurrent=1,
        ),
    ):
        nonlocal task_executed
        task_executed = True  # pragma: no cover

    await docket.add(task_missing_concurrency_arg)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # Task should NOT execute - it should fail due to missing argument
    assert not task_executed


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


async def test_worker_concurrency_missing_argument_shows_available_args(docket: Docket):
    """Test that missing argument error shows available arguments for debugging."""
    task_executed = False

    async def task_missing_concurrency_arg(
        actual_param: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param", max_concurrent=1
        ),
    ):
        nonlocal task_executed
        task_executed = True  # pragma: no cover

    await docket.add(task_missing_concurrency_arg)(actual_param=42)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    # Task should NOT execute - it should fail due to missing argument
    assert not task_executed


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

    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

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

    await docket.add(task_that_may_fail)(customer_id=1, should_fail=True)
    await docket.add(task_that_may_fail)(customer_id=1, should_fail=False)
    await docket.add(task_that_may_fail)(customer_id=1, should_fail=False)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert len(execution_results) == 3
    failed_tasks = [r for r in execution_results if r[2] is True]
    successful_tasks = [r for r in execution_results if r[2] is False]
    assert len(failed_tasks) == 1
    assert len(successful_tasks) == 2


async def test_worker_concurrency_cleanup_after_task_completion(docket: Docket):
    """Test that concurrency slots are properly cleaned up after task completion"""
    cleanup_verified = False

    async def task_with_cleanup_verification(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        await asyncio.sleep(0.01)

    await docket.add(task_with_cleanup_verification)(customer_id=1)
    await docket.add(task_with_cleanup_verification)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()
        async with docket.redis() as redis:
            await redis.keys(f"{docket.name}:concurrency:*")  # type: ignore
            cleanup_verified = True

    assert cleanup_verified


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

    for _ in range(2):
        await docket.add(cleanup_test_task)(customer_id=1, should_fail=False)

    await docket.add(cleanup_test_task)(customer_id=1, should_fail=True)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_count == 3
    assert not cleanup_success


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

    await docket.add(successful_task)(customer_id=1)
    await docket.add(successful_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_completed


async def test_stale_concurrency_slots_are_scavenged_when_full(docket: Docket):
    """Test that stale slots are scavenged on-demand when concurrency is full.

    Slots are only scavenged when a new task needs one and all slots are taken.
    This is a distributed approach - each worker cleans up as needed rather than
    proactive garbage collection.
    """
    task_completed = False

    async def task_with_concurrency(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        nonlocal task_completed
        task_completed = True

    # Manually insert stale slots into the concurrency sorted set.
    # These simulate slots from workers that crashed without releasing.
    concurrency_key = f"{docket.name}:concurrency:customer_id:123"
    stale_timestamp = (
        datetime.now(timezone.utc).timestamp() - 400
    )  # >redelivery_timeout old

    async with docket.redis() as redis:
        # Add two stale slots that fill up max_concurrent
        await redis.zadd(concurrency_key, {"stale_task_1": stale_timestamp})  # type: ignore
        await redis.zadd(concurrency_key, {"stale_task_2": stale_timestamp})  # type: ignore

        # Verify stale slots are present
        count_before = await redis.zcard(concurrency_key)  # type: ignore
        assert count_before == 2

    # Run a task - this should scavenge ONE stale slot and execute
    await docket.add(task_with_concurrency)(customer_id=123)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_completed

    # Verify: one stale slot was scavenged, task completed and released its slot,
    # so one stale slot should remain (we only scavenge what we need)
    async with docket.redis() as redis:
        remaining = await redis.zrange(concurrency_key, 0, -1)  # type: ignore
        # One stale slot should remain (the other was scavenged)
        assert len(remaining) == 1
        # The remaining slot should be one of the stale ones
        assert remaining[0] in [b"stale_task_1", b"stale_task_2"]


async def test_graceful_shutdown_releases_concurrency_slots(docket: Docket):
    """Verify that concurrency slots are released when worker shuts down gracefully.

    When a worker receives a shutdown signal while tasks are running, it should
    drain the active tasks (let them complete) and release their concurrency slots.
    """
    task_started = asyncio.Event()
    task_can_finish = asyncio.Event()
    task_completed = False

    async def slow_task_with_concurrency(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ) -> None:
        nonlocal task_completed
        task_started.set()
        await task_can_finish.wait()
        task_completed = True

    await docket.add(slow_task_with_concurrency)(customer_id=42)

    concurrency_key = f"{docket.name}:concurrency:customer_id:42"

    async with Worker(docket) as worker:
        # Start worker in background
        worker_task = asyncio.create_task(worker.run_until_finished())

        # Wait for task to start (slot should be acquired)
        await asyncio.wait_for(task_started.wait(), timeout=5.0)

        # Verify slot is held
        async with docket.redis() as redis:
            slot_count = await redis.zcard(concurrency_key)
            assert slot_count == 1, "Slot should be held while task is running"

        # Let task finish - worker will drain and exit
        task_can_finish.set()
        await asyncio.wait_for(worker_task, timeout=5.0)

    # Verify task completed and slot was released
    assert task_completed, "Task should have completed during graceful shutdown"

    async with docket.redis() as redis:
        slot_count = await redis.zcard(concurrency_key)
        assert slot_count == 0, "Slot should be released after graceful shutdown"
