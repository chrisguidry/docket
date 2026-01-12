"""Tests for basic task scheduling and execution."""

from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

from docket import Docket, Worker, testing


async def test_immediate_task_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately."""

    await docket.add(the_task)("a", "b", c="c")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_immediate_task_execution_by_name(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately by name."""

    docket.register(the_task)

    await docket.add("the_task")("a", "b", c="c")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_scheduled_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should execute a task at a specific time."""

    when = now() + timedelta(milliseconds=100)
    await docket.add(the_task, when)("a", "b", c="c")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")

    assert when <= now()


async def test_rescheduling_later(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=100)
    await docket.replace(the_task, later, key=key)("b", "c", c="d")

    await testing.assert_task_scheduled(docket, the_task, key=key)

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert later <= now()


async def test_rescheduling_earlier(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for earlier"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=100)
    await docket.add(the_task, soon, key)("a", "b", c="c")

    earlier = now() + timedelta(milliseconds=10)
    await docket.replace(the_task, earlier, key)("b", "c", c="d")

    await testing.assert_task_scheduled(docket, the_task, key=key)

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert earlier <= now()


async def test_rescheduling_by_name(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=100)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=200)
    await docket.replace("the_task", later, key=key)("b", "c", c="d")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert later <= now()


async def test_replace_without_existing_task_acts_like_add(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket.replace() on a non-existent key should schedule the task like add()"""

    key = f"my-cool-task:{uuid4()}"

    # Replace without prior add - should just schedule the task
    later = now() + timedelta(milliseconds=100)
    await docket.replace(the_task, later, key=key)("b", "c", c="d")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert later <= now()
