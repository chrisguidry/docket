"""Tests that illustrate the core behavior of docket.

These tests should serve as documentation highlighting the core behavior of docket and
don't need to cover detailed edge cases.  Keep these tests as straightforward and clean
as possible to aid with understanding docket.
"""

from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable
from unittest.mock import AsyncMock

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


async def test_immediate_task_execution(docket: Docket, worker: Worker):
    """docket should execute a task immediately."""

    the_task = AsyncMock()

    await docket.run(the_task)("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_called_once_with("a", "b", c="c")


async def test_immedate_task_execution_by_name(docket: Docket, worker: Worker):
    """docket should execute a task immediately by name."""

    the_task = AsyncMock()
    the_task.__name__ = "the_task"

    docket.add(the_task)

    await docket.run("the_task")("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_called_once_with("a", "b", c="c")


async def test_scheduled_execution(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should execute a task at a specific time."""

    the_task = AsyncMock()

    when = now() + timedelta(milliseconds=100)
    await docket.schedule(the_task, when)("a", "b", c="c")

    await worker.run_until_current()

    assert now() >= when

    the_task.assert_called_once_with("a", "b", c="c")
