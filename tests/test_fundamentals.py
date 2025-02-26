"""Tests that illustrate the core behavior of docket.

These tests should serve as documentation highlighting the core behavior of docket and
don't need to cover detailed edge cases.  Keep these tests as straightforward and clean
as possible to aid with understanding docket.
"""

from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from docket import Docket, Worker


@pytest.fixture
async def docket() -> AsyncGenerator[Docket, None]:
    async with Docket() as docket:
        yield docket


@pytest.fixture
async def worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    async with Worker(docket) as worker:
        yield worker


@pytest.fixture
def the_task() -> AsyncMock:
    return AsyncMock()


async def test_immediate_task_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately."""

    await docket.add(the_task)("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_called_once_with("a", "b", c="c")


async def test_immedate_task_execution_by_name(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately by name."""

    the_task.__name__ = "the_task"

    docket.register(the_task)

    await docket.add("the_task")("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_called_once_with("a", "b", c="c")


async def test_scheduled_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should execute a task at a specific time."""

    when = now() + timedelta(milliseconds=100)
    await docket.add(the_task, when)("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_called_once_with("a", "b", c="c")

    assert when <= now()


async def test_rescheduling_later(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task-{uuid4()}"

    when = now() + timedelta(milliseconds=10)
    await docket.add(the_task, when, key=key)("a", "b", c="c")

    when = now() + timedelta(milliseconds=100)
    await docket.add(the_task, when, key=key)("b", "c", c="d")

    await worker.run_until_current()

    the_task.assert_called_once_with("b", "c", c="d")

    assert when <= now()


async def test_rescheduling_earlier(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for earlier"""

    key = f"my-cool-task-{uuid4()}"

    when = now() + timedelta(milliseconds=100)
    await docket.add(the_task, when, key=key)("a", "b", c="c")

    when = now() + timedelta(milliseconds=10)
    await docket.add(the_task, when, key=key)("b", "c", c="d")

    await worker.run_until_current()

    the_task.assert_called_once_with("b", "c", c="d")

    assert when <= now()


async def test_cancelling_task(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for cancelling a task"""

    execution = await docket.add(the_task)("a", "b", c="c")

    await docket.cancel(execution.key)

    await worker.run_until_current()

    the_task.assert_not_called()
