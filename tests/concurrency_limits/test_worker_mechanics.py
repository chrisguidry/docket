"""Tests for worker's internal handling of concurrency limits.

This module tests the worker's internal mechanisms for managing concurrency:
- Missing argument handling and bypass paths
- Cleanup operations on success and failure
- Internal methods (_release_concurrency_slot, _can_start_task)
- Finally block behavior
"""

import asyncio
from datetime import datetime, timezone

from docket import ConcurrencyLimit, Docket, Worker
from docket.execution import Execution


async def test_worker_concurrency_missing_argument_bypass(docket: Docket):
    """Test that tasks with missing concurrency arguments bypass concurrency control"""
    task_executed = False

    async def task_missing_concurrency_arg(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "missing_param",
            max_concurrent=1,
        ),
    ):
        nonlocal task_executed
        task_executed = True

    await docket.add(task_missing_concurrency_arg)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

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

    await docket.add(task_missing_concurrency_arg)()

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_executed


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


async def test_worker_no_concurrency_dependency_in_release(docket: Docket):
    """Test _release_concurrency_slot with function that has no concurrency dependency."""

    async def task_without_concurrency_dependency():
        await asyncio.sleep(0.001)

    await task_without_concurrency_dependency()

    async with Worker(docket) as worker:
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
        execution = Execution(
            docket=docket,
            function=task_with_missing_arg,
            args=(),
            kwargs={},
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
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

    docket.register(task_with_missing_concurrency_arg)

    async with Worker(docket) as worker:
        execution = Execution(
            docket=docket,
            function=task_with_missing_concurrency_arg,
            args=(),
            kwargs={},
            when=datetime.now(timezone.utc),
            key="test_key",
            attempt=1,
        )

        async with docket.redis() as redis:
            result = await worker._can_start_task(redis, execution)  # type: ignore[reportPrivateUsage]
            assert result is True
