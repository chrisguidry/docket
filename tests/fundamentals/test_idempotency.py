"""Tests for task key idempotency behavior."""

from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

from docket import Docket, Worker, testing


async def test_adding_is_idempotent(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Adding a task with the same key twice should only run the first one."""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=500)
    await docket.add(the_task, later, key=key)("b", "c", c="d")

    await testing.assert_task_scheduled(docket, the_task, key=key)

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")

    assert soon <= now() < later


async def test_task_keys_are_idempotent_in_the_future(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """A future task blocks an immediate task with the same key."""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, when=soon, key=key)("a", "b", c="c")
    await docket.add(the_task, when=now(), key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")
    the_task.reset_mock()

    # It should be fine to run it afterward
    await docket.add(the_task, key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("d", "e", c="f")


async def test_task_keys_are_idempotent_between_the_future_and_present(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """An immediate task blocks a future task with the same key."""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, when=now(), key=key)("a", "b", c="c")
    await docket.add(the_task, when=soon, key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")
    the_task.reset_mock()

    # It should be fine to run it afterward
    await docket.add(the_task, key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("d", "e", c="f")


async def test_task_keys_are_idempotent_in_the_present(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """Two immediate tasks with the same key only runs the first one."""

    key = f"my-cool-task:{uuid4()}"

    await docket.add(the_task, when=now(), key=key)("a", "b", c="c")
    await docket.add(the_task, when=now(), key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")
    the_task.reset_mock()

    # It should be fine to run it afterward
    await docket.add(the_task, key=key)("d", "e", c="f")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("d", "e", c="f")
