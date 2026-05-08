"""Tests for task key idempotency behavior."""

from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

from docket import Disposition, Docket, Worker, testing


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


async def test_disposition_reports_whether_a_handoff_was_accepted(
    docket: Docket, the_task: AsyncMock
):
    """The Execution returned by docket.add reports whether the call actually
    placed a task on the queue or was a no-op because a task with the same
    key was already known."""

    key = f"my-cool-task:{uuid4()}"

    first = await docket.add(the_task, key=key)("first")
    assert first.disposition is Disposition.SCHEDULED

    second = await docket.add(the_task, key=key)("second")
    assert second.disposition is Disposition.ALREADY_SCHEDULED


async def test_replace_disposition_is_always_scheduled(
    docket: Docket, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket.replace overwrites any prior schedule for the key, so its
    disposition is always SCHEDULED (the no-op path doesn't apply)."""

    key = f"my-cool-task:{uuid4()}"
    soon = now() + timedelta(seconds=60)

    first = await docket.add(the_task, when=soon, key=key)("first")
    assert first.disposition is Disposition.SCHEDULED

    second = await docket.replace(the_task, when=soon, key=key)("second")
    assert second.disposition is Disposition.SCHEDULED
