"""Tests that illustrate the core behavior of docket.

These tests should serve as documentation highlighting the core behavior of docket and
don't need to cover detailed edge cases.  Keep these tests as straightforward and clean
as possible to aid with understanding docket.
"""

import logging
from datetime import datetime, timedelta
from logging import LoggerAdapter
from typing import Annotated, Callable
from unittest.mock import AsyncMock, call
from uuid import uuid4

import pytest

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    Execution,
    ExponentialRetry,
    Logged,
    Retry,
    TaskKey,
    TaskLogger,
    Worker,
    tasks,
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

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", "b", c="c")


async def test_immedate_task_execution_by_name(
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


async def test_adding_is_itempotent(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=500)
    await docket.add(the_task, later, key=key)("b", "c", c="d")

    await worker.run_until_finished()

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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("b", "c", c="d")

    assert later <= now()


async def test_cancelling_future_task(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for cancelling a task"""

    soon = now() + timedelta(milliseconds=100)
    execution = await docket.add(the_task, soon)("a", "b", c="c")

    await docket.cancel(execution.key)

    await worker.run_until_finished()

    the_task.assert_not_called()


async def test_cancelling_current_task_not_supported(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket does not allow cancelling a task that is schedule now"""

    execution = await docket.add(the_task, now())("a", "b", c="c")

    await docket.cancel(execution.key)

    await worker.run_until_finished()

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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

    total_delay = now() - start
    assert total_delay >= timedelta(milliseconds=200)

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

    await worker.run_until_finished()

    assert calls == 3


async def test_supports_exponential_backoff_retries(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support exponential backoff retries"""

    calls = 0

    async def the_task(
        a: str,
        b: str = "b",
        retry: Retry = ExponentialRetry(
            attempts=5,
            minimum_delay=timedelta(milliseconds=25),
            maximum_delay=timedelta(milliseconds=1000),
        ),
    ) -> None:
        assert a == "a"
        assert b == "c"

        assert isinstance(retry, ExponentialRetry)

        nonlocal calls
        calls += 1

        assert retry.attempts == 5
        assert retry.attempt == calls

        raise Exception("Failed")

    await docket.add(the_task)("a", b="c")

    start = now()

    await worker.run_until_finished()

    total_delay = now() - start
    assert total_delay >= timedelta(milliseconds=25 + 50 + 100 + 200)

    assert calls == 5


async def test_supports_exponential_backoff_retries_under_maximum_delay(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support exponential backoff retries"""

    calls = 0

    async def the_task(
        a: str,
        b: str = "b",
        retry: Retry = ExponentialRetry(
            attempts=5,
            minimum_delay=timedelta(milliseconds=25),
            maximum_delay=timedelta(milliseconds=100),
        ),
    ) -> None:
        assert a == "a"
        assert b == "c"

        assert isinstance(retry, ExponentialRetry)

        nonlocal calls
        calls += 1

        assert retry.attempts == 5
        assert retry.attempt == calls

        raise Exception("Failed")

    await docket.add(the_task)("a", b="c")

    start = now()

    await worker.run_until_finished()

    total_delay = now() - start
    assert total_delay >= timedelta(milliseconds=25 + 50 + 100 + 100)

    assert calls == 5


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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

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

    await worker.run_until_finished()

    assert called


async def test_all_dockets_have_a_trace_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """All dockets should have a trace task"""

    await docket.add(tasks.trace)("Hello, world!")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        assert "Hello, world!" in caplog.text


async def test_all_dockets_have_a_fail_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """All dockets should have a fail task"""

    await docket.add(tasks.fail)("Hello, world!")

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

        assert "Hello, world!" in caplog.text


async def test_tasks_can_opt_into_argument_logging(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """Tasks can opt into argument logging for specific arguments"""

    async def the_task(
        a: Annotated[str, Logged],
        b: str,
        c: Annotated[str, Logged()] = "c",
        d: Annotated[str, "nah chief"] = "d",
        docket: Docket = CurrentDocket(),
    ):
        pass

    await docket.add(the_task)("value-a", b="value-b", c="value-c", d="value-d")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        assert "the_task('value-a', b=..., c='value-c', d=...)" in caplog.text
        assert "value-b" not in caplog.text
        assert "value-d" not in caplog.text


async def test_logging_inside_of_task(
    docket: Docket,
    worker: Worker,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
):
    """docket should support providing a logger with task context"""
    called = False

    async def the_task(
        a: str, b: str, logger: LoggerAdapter[logging.Logger] = TaskLogger()
    ):
        assert a == "a"
        assert b == "c"

        logger.info("Task is running")

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    assert called
    assert "Task is running" in caplog.text
    assert "docket.task.the_task" in caplog.text


async def test_self_perpetuating_immediate_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            await docket.add(the_task, key=key)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)

    await worker.run_until_finished()

    assert calls["first"] == [11, 12, 13]
    assert calls["second"] == [21, 22, 23]


async def test_self_perpetuating_scheduled_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            soon = now() + timedelta(milliseconds=100)
            await docket.add(the_task, key=key, when=soon)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)

    await worker.run_until_finished()

    assert calls["first"] == [11, 12, 13]
    assert calls["second"] == [21, 22, 23]


async def test_striking_entire_tasks(
    docket: Docket, worker: Worker, the_task: AsyncMock, another_task: AsyncMock
):
    """docket should support striking and restoring entire tasks"""

    await docket.add(the_task)("a", b="c")
    await docket.add(another_task)("d", e="f")

    await docket.strike(the_task)

    await worker.run_until_finished()

    the_task.assert_not_called()
    the_task.reset_mock()

    another_task.assert_awaited_once_with("d", e="f")
    another_task.reset_mock()

    await docket.restore(the_task)

    await docket.add(the_task)("g", h="i")
    await docket.add(another_task)("j", k="l")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("g", h="i")
    another_task.assert_awaited_once_with("j", k="l")


async def test_striking_entire_parameters(
    docket: Docket, worker: Worker, the_task: AsyncMock, another_task: AsyncMock
):
    """docket should support striking and restoring entire parameters"""

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await docket.strike(None, "customer_id", "==", "789")

    await worker.run_until_finished()

    assert the_task.call_count == 2
    the_task.assert_has_awaits(
        [
            call(customer_id="123", order_id="456"),
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is stricken
        ]
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ]
    )
    another_task.reset_mock()

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await docket.strike(None, "customer_id", "==", "123")

    await worker.run_until_finished()

    assert the_task.call_count == 1
    the_task.assert_has_awaits(
        [
            # customer_id == 123 is stricken
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is stricken
        ]
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ]
    )
    another_task.reset_mock()

    await docket.restore(None, "customer_id", "==", "123")

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await worker.run_until_finished()

    assert the_task.call_count == 2
    the_task.assert_has_awaits(
        [
            call(customer_id="123", order_id="456"),
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is still stricken
        ]
    )

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is still stricken
        ]
    )


async def test_striking_tasks_for_specific_parameters(
    docket: Docket, worker: Worker, the_task: AsyncMock, another_task: AsyncMock
):
    """docket should support striking and restoring tasks for specific parameters"""
    await docket.add(the_task)("a", b=1)
    await docket.add(the_task)("a", b=2)
    await docket.add(the_task)("a", b=3)
    await docket.add(another_task)("d", b=1)
    await docket.add(another_task)("d", b=2)
    await docket.add(another_task)("d", b=3)

    await docket.strike(the_task, "b", "<=", 2)

    await worker.run_until_finished()

    assert the_task.call_count == 1
    the_task.assert_has_awaits(
        [
            # b <= 2 is stricken, so b=1 is out
            # b <= 2 is stricken, so b=2 is out
            call("a", b=3),
        ]
    )
    the_task.reset_mock()

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ]
    )
    another_task.reset_mock()

    await docket.restore(the_task, "b", "<=", 2)

    await docket.add(the_task)("a", b=1)
    await docket.add(the_task)("a", b=2)
    await docket.add(the_task)("a", b=3)
    await docket.add(another_task)("d", b=1)
    await docket.add(another_task)("d", b=2)
    await docket.add(another_task)("d", b=3)

    await worker.run_until_finished()

    assert the_task.call_count == 3
    the_task.assert_has_awaits(
        [
            call("a", b=1),
            call("a", b=2),
            call("a", b=3),
        ]
    )

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ]
    )


async def test_adding_task_by_name_when_not_registered(docket: Docket):
    """docket should raise an error when attempting to add a task by name that isn't registered"""

    with pytest.raises(KeyError, match="unregistered_task"):
        await docket.add("unregistered_task")()


async def test_adding_task_with_unbindable_arguments(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should not raise an error when a task is scheduled or executed with
    incorrect arguments."""

    async def task_with_specific_args(a: str, b: int, c: bool = False) -> None:
        pass  # pragma: no cover

    await docket.add(task_with_specific_args)("a", 2, d="unexpected")  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "got an unexpected keyword argument 'd'" in caplog.text
