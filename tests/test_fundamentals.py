"""Tests that illustrate the core behavior of docket.

These tests should serve as documentation highlighting the core behavior of docket and
don't need to cover detailed edge cases.  Keep these tests as straightforward and clean
as possible to aid with understanding docket.
"""

import logging
from datetime import datetime, timedelta
from logging import Logger
from typing import Callable
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    Execution,
    Retry,
    TaskKey,
    TaskLogger,
    Worker,
)


@pytest.fixture
def the_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "the_task"
    return task


async def test_immediate_task_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately."""

    await docket.add(the_task)("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_immedate_task_execution_by_name(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """docket should execute a task immediately by name."""

    docket.register(the_task)

    await docket.add("the_task")("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_scheduled_execution(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should execute a task at a specific time."""

    when = now() + timedelta(milliseconds=100)
    await docket.add(the_task, when)("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")

    assert when <= now()


async def test_adding_is_itempotent(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=500)
    await docket.add(the_task, later, key=key)("b", "c", c="d")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")

    assert soon <= now() < later


async def test_rescheduling_later(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=100)
    await docket.replace(the_task, later, key=key)("b", "c", c="d")

    await worker.run_until_current()

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

    await worker.run_until_current()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert earlier <= now()


async def test_rescheduling_by_name(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=100)
    await docket.replace("the_task", later, key=key)("b", "c", c="d")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert later <= now()


async def test_cancelling_future_task(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for cancelling a task"""

    soon = now() + timedelta(milliseconds=100)
    execution = await docket.add(the_task, soon)("a", "b", c="c")

    await docket.cancel(execution.key)

    await worker.run_until_current()

    the_task.assert_not_called()


async def test_cancelling_current_task_not_supported(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket does not allow cancelling a task that is schedule now"""

    execution = await docket.add(the_task, now())("a", "b", c="c")

    await docket.cancel(execution.key)

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_errors_are_logged(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
):
    """docket should log errors when a task fails"""

    the_task.side_effect = Exception("Faily McFailerson")
    await docket.add(the_task, now())("a", "b", c="c")

    await worker.run_until_current()

    the_task.assert_awaited_once_with("a", "b", c="c")

    assert "Faily McFailerson" in caplog.text


async def test_supports_simple_linear_retries(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support simple linear retries"""

    calls = 0

    async def the_task(
        a: str,
        b: str = "b",
        retry: Retry = Retry(attempts=3),
    ) -> None:
        assert a == "a"
        assert b == "c"

        assert retry is not None

        nonlocal calls
        calls += 1

        assert retry.attempts == 3
        assert retry.attempt == calls

        raise Exception("Failed")

    await docket.add(the_task)("a", b="c")

    await worker.run_until_current()

    assert calls == 3


async def test_supports_simple_linear_retries_with_delay(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support simple linear retries with a delay"""

    calls = 0

    async def the_task(
        a: str,
        b: str = "b",
        retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=100)),
    ) -> None:
        assert a == "a"
        assert b == "c"

        assert retry is not None

        nonlocal calls
        calls += 1

        assert retry.attempts == 3
        assert retry.attempt == calls

        raise Exception("Failed")

    await docket.add(the_task)("a", b="c")

    start = now()

    await worker.run_until_current()

    total_delay = now() - start
    assert total_delay >= timedelta(milliseconds=300)

    assert calls == 3


async def test_supports_infinite_retries(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support infinite retries (None for attempts)"""

    calls = 0

    async def the_task(
        a: str,
        b: str = "b",
        retry: Retry = Retry(attempts=None),
    ) -> None:
        assert a == "a"
        assert b == "c"

        assert retry is not None
        assert retry.attempts is None

        nonlocal calls
        calls += 1

        assert retry.attempt == calls

        if calls < 3:
            raise Exception("Failed")

    await docket.add(the_task)("a", b="c")

    await worker.run_until_current()

    assert calls == 3


async def test_supports_requesting_current_docket(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current docket to a task"""

    called = False

    async def the_task(a: str, b: str, this_docket: Docket = CurrentDocket()):
        assert a == "a"
        assert b == "c"
        assert this_docket is docket

        nonlocal called
        called = True

    await docket.add(the_task)("a", b="c")

    await worker.run_until_current()

    assert called


async def test_supports_requesting_current_worker(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current worker to a task"""

    called = False

    async def the_task(a: str, b: str, this_worker: Worker = CurrentWorker()):
        assert a == "a"
        assert b == "c"
        assert this_worker is worker

        nonlocal called
        called = True

    await docket.add(the_task)("a", b="c")

    await worker.run_until_current()

    assert called


async def test_supports_requesting_current_execution(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current execution to a task"""

    called = False

    async def the_task(a: str, b: str, this_execution: Execution = CurrentExecution()):
        assert a == "a"
        assert b == "c"

        assert isinstance(this_execution, Execution)
        assert this_execution.key == "my-cool-task:123"

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    await worker.run_until_current()

    assert called


async def test_supports_requesting_current_task_key(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current task key to a task"""

    called = False

    async def the_task(a: str, b: str, this_key: str = TaskKey()):
        assert a == "a"
        assert b == "c"
        assert this_key == "my-cool-task:123"

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    await worker.run_until_current()

    assert called


async def test_logging_inside_of_task(
    docket: Docket,
    worker: Worker,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
):
    """docket should support providing a logger with task context to a task"""
    called = False

    async def the_task(a: str, b: str, logger: Logger = TaskLogger()):
        assert a == "a"
        assert b == "c"

        # Use the logger and verify message is captured
        logger.info("Task is running")

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    with caplog.at_level(logging.INFO):
        await worker.run_until_current()

    assert called
    assert "Task is running" in caplog.text

    assert "docket.task.my-cool-task:123" in caplog.text
