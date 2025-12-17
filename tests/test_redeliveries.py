"""Tests for retry behavior and lease renewal.

These tests verify that the lease renewal mechanism prevents duplicate task
execution when tasks run longer than the redelivery_timeout.
"""

import asyncio
from datetime import timedelta

from docket import Docket, Retry, Worker


async def test_long_running_task_not_duplicated(docket: Docket):
    """Test that lease renewal prevents duplicate execution when task exceeds redelivery_timeout.

    This test runs a task that takes 500ms with a 200ms redelivery_timeout.
    Without lease renewal, XAUTOCLAIM would reclaim the message after 200ms,
    causing duplicate execution. With lease renewal (every 50ms), the message
    stays claimed and no duplicates occur.
    """
    executions: list[int] = []

    async def slow_task(task_id: int):
        executions.append(task_id)
        await asyncio.sleep(0.5)

    await docket.add(slow_task, key="slow-1")(task_id=1)
    await docket.add(slow_task, key="slow-2")(task_id=2)

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert sorted(executions) == [1, 2], f"Expected [1, 2], got {executions}"


async def test_retry_with_long_running_task(docket: Docket):
    """Test that retries work correctly with lease renewal.

    A task that fails and retries should still benefit from lease renewal.
    Each attempt should be a distinct execution without duplicates.
    """
    attempts: list[tuple[str, int]] = []

    async def flaky_task(
        task_id: str,
        retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=50)),
    ):
        attempts.append((task_id, retry.attempt))
        await asyncio.sleep(0.3)

        if retry.attempt < 3:
            raise ValueError("Temporary failure")

    await docket.add(flaky_task, key="flaky")(task_id="test")

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert attempts == [
        ("test", 1),
        ("test", 2),
        ("test", 3),
    ], f"Expected 3 distinct attempts, got {attempts}"
