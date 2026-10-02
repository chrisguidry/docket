"""Tests for task cancellation."""

from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

from docket import Docket, Worker, testing


async def test_cancelling_future_task(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for cancelling a task"""

    soon = now() + timedelta(milliseconds=100)
    execution = await docket.add(the_task, soon)("a", "b", c="c")

    await docket.cancel(execution.key)

    await testing.assert_task_not_scheduled(docket, the_task)

    await worker.run_until_finished()

    the_task.assert_not_called()


async def test_cancelling_immediate_task(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket can cancel a task that is scheduled immediately"""

    execution = await docket.add(the_task, now())("a", "b", c="c")

    await docket.cancel(execution.key)

    await testing.assert_task_not_scheduled(docket, the_task)

    await worker.run_until_finished()

    the_task.assert_not_called()


async def test_cancellation_is_idempotent(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Test that canceling the same task twice doesn't error."""
    key = f"test-task:{uuid4()}"

    # Schedule a task
    later = now() + timedelta(seconds=1)
    await docket.add(the_task, later, key=key)("test")

    # Cancel it twice - both should succeed without error
    await docket.cancel(key)
    await docket.cancel(key)  # Should be idempotent

    await testing.assert_task_not_scheduled(docket, the_task)

    # Run worker to ensure the task was actually cancelled
    await worker.run_until_finished()

    # Task should not have been executed since it was cancelled
    the_task.assert_not_called()
