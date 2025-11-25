"""Tests that illustrate the core behavior of docket.

These tests should serve as documentation highlighting the core behavior of docket and
don't need to cover detailed edge cases.  Keep these tests as straightforward and clean
as possible to aid with understanding docket.
"""

import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from logging import LoggerAdapter
from typing import Annotated, AsyncGenerator, Callable, Generator
from unittest.mock import AsyncMock, call
from uuid import uuid4

import pytest

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Depends,
    Docket,
    Execution,
    ExecutionState,
    ExponentialRetry,
    Logged,
    Perpetual,
    Progress,
    Retry,
    TaskArgument,
    TaskKey,
    TaskLogger,
    Timeout,
    Worker,
    tasks,
    testing,
)
from docket.execution import StateEvent
from tests._key_leak_checker import KeyCountChecker


@pytest.fixture
def the_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
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


async def test_adding_is_idempotent(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should allow for rescheduling a task for later"""

    key = f"my-cool-task:{uuid4()}"

    soon = now() + timedelta(milliseconds=10)
    await docket.add(the_task, soon, key=key)("a", "b", c="c")

    later = now() + timedelta(milliseconds=500)
    await docket.add(the_task, later, key=key)("b", "c", c="d")

    await testing.assert_task_scheduled(docket, the_task, key=key)

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


async def test_task_keys_are_idempotent_in_the_future(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """docket should only allow one task with the same key to be scheduled or due"""

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
    """docket should only allow one task with the same key to be scheduled or due"""

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
    """docket should only allow one task with the same key to be scheduled or due"""

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


async def test_tasks_can_report_progress(docket: Docket, worker: Worker):
    """docket should support tasks reporting their progress"""

    called = False

    async def the_task(
        a: str,
        b: str,
        progress: Progress = Progress(),
    ):
        assert a == "a"
        assert b == "c"

        # Set the total expected work
        await progress.set_total(100)

        # Increment progress
        await progress.increment(10)
        await progress.increment(20)

        # Set a status message
        await progress.set_message("Processing items...")

        # Read back current progress
        assert progress.current == 30
        assert progress.total == 100
        assert progress.message == "Processing items..."

        nonlocal called
        called = True

    await docket.add(the_task, key="progress-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_tasks_can_access_execution_state(docket: Docket, worker: Worker):
    """docket should support providing execution state and metadata to a task"""

    called = False

    async def the_task(
        a: str,
        b: str,
        this_execution: Execution = CurrentExecution(),
    ):
        assert a == "a"
        assert b == "c"

        assert isinstance(this_execution, Execution)
        assert this_execution.key == "stateful-task:123"
        assert this_execution.state == ExecutionState.RUNNING
        assert this_execution.worker is not None
        assert this_execution.started_at is not None

        nonlocal called
        called = True

    await docket.add(the_task, key="stateful-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_execution_state_lifecycle(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket executions transition through states: QUEUED → RUNNING → COMPLETED"""

    async def successful_task():
        await asyncio.sleep(0.01)

    async def failing_task():
        await asyncio.sleep(0.01)
        raise ValueError("Task failed")

    # Test successful execution lifecycle
    execution = await docket.add(
        successful_task, key="success:123", when=now() + timedelta(seconds=1)
    )()

    # Collect state events
    state_events: list[StateEvent] = []

    async def collect_states() -> None:
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                state_events.append(event)
                if event["state"] == ExecutionState.COMPLETED:
                    break

    subscriber_task = asyncio.create_task(collect_states())

    await worker.run_until_finished()
    await asyncio.wait_for(subscriber_task, timeout=7.0)

    # Verify we saw the state transitions
    # Note: subscribe() emits the initial state first, then real-time updates
    states = [e["state"] for e in state_events]
    assert states == [
        ExecutionState.SCHEDULED,
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.COMPLETED,
    ]

    # Verify final state has completion metadata
    final_state = state_events[-1]
    assert final_state["state"] == ExecutionState.COMPLETED
    assert final_state["completed_at"] is not None
    assert "error" not in final_state  # No error for successful completion

    # Test failed execution lifecycle
    execution = await docket.add(
        failing_task, key="failure:456", when=now() + timedelta(seconds=1)
    )()

    failed_state_events: list[StateEvent] = []

    async def collect_failed_states() -> None:
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                failed_state_events.append(event)
                if event["state"] == ExecutionState.FAILED:
                    break

    subscriber_task = asyncio.create_task(collect_failed_states())

    await worker.run_until_finished()
    await asyncio.wait_for(subscriber_task, timeout=7.0)

    # Verify we saw the state transitions
    # Note: subscribe() emits the initial state first, then real-time updates
    states = [e["state"] for e in failed_state_events]
    assert states == [
        ExecutionState.SCHEDULED,
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.FAILED,
    ]

    # Verify final state has error information
    final_state = failed_state_events[-1]
    assert final_state["state"] == ExecutionState.FAILED
    assert final_state["completed_at"] is not None
    assert final_state["error"] is not None
    assert final_state["error"] == "ValueError: Task failed"


async def test_task_results_can_be_stored_and_retrieved(docket: Docket, worker: Worker):
    """Test that string results are stored and retrievable."""
    result_value = "hello world"

    async def returns_str() -> str:
        return result_value

    docket.register(returns_str)
    execution = await docket.add(returns_str)()
    await worker.run_until_finished()

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


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


async def test_tasks_can_opt_into_logging_collection_lengths(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """Tasks can opt into logging the length of collections"""

    async def the_task(
        a: Annotated[list[str], Logged(length_only=True)],
        b: Annotated[dict[str, str], Logged(length_only=True)],
        c: Annotated[tuple[str, ...], Logged(length_only=True)],
        d: Annotated[set[str], Logged(length_only=True)],
        e: Annotated[int, Logged(length_only=True)],
        docket: Docket = CurrentDocket(),
    ):
        pass

    await docket.add(the_task)(
        ["a", "b"], b={"d": "e", "f": "g"}, c=("h", "i"), d={"a", "b", "c"}, e=123
    )

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        assert (
            "the_task([len 2], b={len 2}, c=(len 2), d={len 3}, e=123)" in caplog.text
        )


async def test_logging_inside_of_task(
    docket: Docket,
    worker: Worker,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
):
    """docket should support providing a logger with task context"""
    called = False

    async def the_task(
        a: str, b: str, logger: "LoggerAdapter[logging.Logger]" = TaskLogger()
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
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(the_task, now(), key=key)(start, iteration + 1)

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
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(the_task, key=key, when=soon)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)

    await worker.run_until_finished()

    assert calls["first"] == [11, 12, 13]
    assert calls["second"] == [21, 22, 23]


async def test_infinitely_self_perpetuating_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support testing use cases for infinitely self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
        "unaffected": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        soon = now() + timedelta(milliseconds=100)
        # Use replace() for self-perpetuating to allow rescheduling while running
        await docket.replace(the_task, key=key, when=soon)(start, iteration + 1)

    async def unaffected_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(unaffected_task, now(), key=key)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)
    await docket.add(unaffected_task, key="unaffected")(30, 1)

    # Using worker.run_until_finished() would hang here because the task is always
    # queueing up a future run of itself.  With worker.run_at_most(),
    # we can specify tasks keys that will only be allowed to run a limited number of
    # times, thus allowing the worker to exist cleanly.
    await worker.run_at_most({"first": 4, "second": 2})

    assert calls["first"] == [11, 12, 13, 14]
    assert calls["second"] == [21, 22]
    assert calls["unaffected"] == [31, 32, 33]


async def test_striking_entire_tasks(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring entire tasks"""

    execution1 = await docket.add(the_task)("a", b="c")
    await docket.add(another_task)("d", e="f")

    # Struck tasks leave runs hash behind (intentional - task might be restored)
    key_leak_checker.add_exemption(f"{docket.name}:runs:{execution1.key}")

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
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring entire parameters"""

    # Struck tasks remain without TTL so they can be restored
    key_leak_checker.add_pattern_exemption(f"{docket.name}:runs:*")

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
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
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
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
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
        ],
        any_order=True,
    )

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is still stricken
        ],
        any_order=True,
    )


async def test_striking_tasks_for_specific_parameters(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring tasks for specific parameters"""

    # Struck tasks remain without TTL so they can be restored
    key_leak_checker.add_pattern_exemption(f"{docket.name}:runs:*")

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
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ],
        any_order=True,
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
        ],
        any_order=True,
    )

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ],
        any_order=True,
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


async def test_perpetual_tasks(docket: Docket, worker: Worker):
    """Perpetual tasks should reschedule themselves forever"""

    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        assert a == "a"
        assert b == 2

        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

    execution = await docket.add(perpetual_task)(a="a", b=2)

    await worker.run_at_most({execution.key: 3})

    assert calls == 3


async def test_perpetual_tasks_can_cancel_themselves(docket: Docket, worker: Worker):
    """A perpetual task can request its own cancellation"""
    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        assert a == "a"
        assert b == 2

        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

        if calls == 3:
            perpetual.cancel()

    await docket.add(perpetual_task)(a="a", b=2)

    await worker.run_until_finished()

    assert calls == 3


async def test_perpetual_tasks_can_change_their_parameters(
    docket: Docket, worker: Worker
):
    """Perpetual tasks may change their parameters each time"""
    arguments: list[tuple[str, int]] = []

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        arguments.append((a, b))
        perpetual.perpetuate(a + "a", b=b + 1)

    execution = await docket.add(perpetual_task)(a="a", b=1)

    await worker.run_at_most({execution.key: 3})

    assert len(arguments) == 3
    assert arguments == [("a", 1), ("aa", 2), ("aaa", 3)]


async def test_perpetual_tasks_perpetuate_even_after_errors(
    docket: Docket, worker: Worker
):
    """Perpetual tasks may change their parameters each time"""
    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        nonlocal calls
        calls += 1

        raise ValueError("woops!")

    execution = await docket.add(perpetual_task)(a="a", b=1)

    await worker.run_at_most({execution.key: 3})

    assert calls == 3


async def test_perpetual_tasks_can_be_automatically_scheduled(
    docket: Docket, worker: Worker
):
    """Perpetual tasks can be automatically scheduled"""

    calls = 0

    async def my_automatic_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=50), automatic=True
        ),
    ):
        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

    # Note we never add this task to the docket, we just register it.
    docket.register(my_automatic_task)

    # The automatic key will be the task function's name
    await worker.run_at_most({"my_automatic_task": 3})

    assert calls == 3


async def test_simple_timeout(docket: Docket, worker: Worker):
    """A task can be scheduled with a timeout"""

    called = False

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        await asyncio.sleep(0.01)

        nonlocal called
        called = True

    await docket.add(task_with_timeout)()

    start = datetime.now(timezone.utc)

    await worker.run_until_finished()

    elapsed = datetime.now(timezone.utc) - start

    assert called
    assert elapsed <= timedelta(milliseconds=150)


async def test_simple_timeout_cancels_tasks(docket: Docket, worker: Worker):
    """A task can be scheduled with a timeout and are cancelled"""

    called = False

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            nonlocal called
            called = True

    await docket.add(task_with_timeout)()

    start = datetime.now(timezone.utc)

    await worker.run_until_finished()

    elapsed = datetime.now(timezone.utc) - start

    assert called
    assert timedelta(milliseconds=100) <= elapsed <= timedelta(milliseconds=200)


async def test_timeout_can_be_extended(docket: Docket, worker: Worker):
    """A task can be scheduled with a timeout and extend themselves"""

    called = False

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        await asyncio.sleep(0.05)

        timeout.extend(timedelta(milliseconds=200))

        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            nonlocal called
            called = True

    await docket.add(task_with_timeout)()

    start = datetime.now(timezone.utc)

    await worker.run_until_finished()

    elapsed = datetime.now(timezone.utc) - start

    assert called
    assert timedelta(milliseconds=250) <= elapsed <= timedelta(milliseconds=400)


async def test_timeout_extends_by_base_by_default(docket: Docket, worker: Worker):
    """A task can be scheduled with a timeout and extend itself by the base timeout"""

    called = False

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        await asyncio.sleep(0.05)

        timeout.extend()  # defaults to the base timeout

        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            nonlocal called
            called = True

    await docket.add(task_with_timeout)()

    start = datetime.now(timezone.utc)

    await worker.run_until_finished()

    elapsed = datetime.now(timezone.utc) - start

    assert called
    assert timedelta(milliseconds=150) <= elapsed <= timedelta(milliseconds=400)


async def test_timeout_is_compatible_with_retry(docket: Docket, worker: Worker):
    """A task that times out can be retried"""

    successes: list[int] = []

    async def task_with_timeout(
        retry: Retry = Retry(attempts=3),
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        if retry.attempt == 1:
            await asyncio.sleep(1)

        successes.append(retry.attempt)

    await docket.add(task_with_timeout)()

    await worker.run_until_finished()

    assert successes == [2]


async def test_simple_function_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of simple functions"""

    async def dependency_one() -> str:
        return f"one-{uuid4()}"

    async def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_contextual_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of async context managers"""

    stages: list[str] = []

    @asynccontextmanager
    async def dependency_one() -> AsyncGenerator[str, None]:
        stages.append("one-before")
        yield f"one-{uuid4()}"
        stages.append("one-after")

    async def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1
    assert stages == ["one-before", "one-after"]


async def test_dependencies_of_dependencies(docket: Docket, worker: Worker):
    """A task dependency can depend on other dependencies"""
    counter = 0

    async def dependency_one() -> list[str]:
        nonlocal counter
        counter += 1
        return [f"one-{counter}"]

    async def dependency_two(my_one: list[str] = Depends(dependency_one)) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + [f"two-{counter}"]

    async def dependency_three(
        my_one: list[str] = Depends(dependency_one),
        my_two: list[str] = Depends(dependency_two),
    ) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + my_two + [f"three-{counter}"]

    async def dependent_task(
        one_a: list[str] = Depends(dependency_one),
        one_b: list[str] = Depends(dependency_one),
        two: list[str] = Depends(dependency_two),
        three: list[str] = Depends(dependency_three),
    ):
        assert one_a is one_b

        assert one_a == ["one-1"]
        assert two == ["one-1", "two-2"]
        assert three == ["one-1", "two-2", "three-3"]

    await docket.add(dependent_task)()

    await worker.run_until_finished()


async def test_dependencies_can_ask_for_docket_dependencies(
    docket: Docket, worker: Worker
):
    """A task dependency can ask for a docket dependency"""

    called = 0

    async def dependency_one(this_docket: Docket = CurrentDocket()) -> str:
        assert this_docket is docket

        nonlocal called
        called += 1

        return f"one-{called}"

    async def dependency_two(
        this_worker: Worker = CurrentWorker(),
        one: str = Depends(dependency_one),
    ) -> str:
        assert this_worker is worker

        assert one == "one-1"

        nonlocal called
        called += 1

        return f"two-{called}"

    async def dependent_task(
        one: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
        this_docket: Docket = CurrentDocket(),
        this_worker: Worker = CurrentWorker(),
    ):
        assert one == "one-1"
        assert two == "two-2"

        assert this_docket is docket
        assert this_worker is worker

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()


async def test_dependency_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task dependency failure will cause the task to fail"""

    called: bool = False

    async def dependency_one() -> str:
        raise ValueError("this one is bad")

    async def dependency_two() -> str:
        raise ValueError("and so is this one")

    async def dependent_task(
        a: str = Depends(dependency_one),
        b: str = Depends(dependency_two),
    ) -> None:
        nonlocal called
        called = True  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert not called

    assert "Failed to resolve dependencies for parameter(s): a, b" in caplog.text
    assert "ValueError: this one is bad" in caplog.text
    assert "ValueError: and so is this one" in caplog.text


async def test_contextual_dependency_before_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A contextual task dependency failure will cause the task to fail"""

    called: int = 0

    @asynccontextmanager
    async def dependency_before() -> AsyncGenerator[str, None]:
        raise ValueError("this one is bad")
        yield "this won't be used"  # pragma: no cover

    async def dependent_task(
        a: str = Depends(dependency_before),
    ) -> None:
        nonlocal called
        called += 1  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert not called

    assert "Failed to resolve dependencies for parameter(s): a" in caplog.text
    assert "ValueError: this one is bad" in caplog.text


async def test_contextual_dependency_after_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A contextual task dependency failure will cause the task to fail"""

    called: int = 0

    @asynccontextmanager
    async def dependency_after() -> AsyncGenerator[str, None]:
        yield "this will be used"
        raise ValueError("this one is bad")

    async def dependent_task(
        a: str = Depends(dependency_after),
    ) -> None:
        assert a == "this will be used"

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert called == 1

    assert "ValueError: this one is bad" in caplog.text


async def test_dependencies_can_ask_for_task_arguments(docket: Docket, worker: Worker):
    """A task dependency can ask for a task argument"""

    called = 0

    async def dependency_one(a: list[str] = TaskArgument()) -> list[str]:
        return a

    async def dependency_two(another_name: list[str] = TaskArgument("a")) -> list[str]:
        return another_name

    async def dependent_task(
        a: list[str],
        b: list[str] = TaskArgument("a"),
        c: list[str] = Depends(dependency_one),
        d: list[str] = Depends(dependency_two),
    ) -> None:
        assert a is b
        assert a is c
        assert a is d

        nonlocal called
        called += 1

    await docket.add(dependent_task)(a=["hello", "world"])

    await worker.run_until_finished()

    assert called == 1


async def test_task_arguments_may_be_optional(docket: Docket, worker: Worker):
    """A task dependency can ask for a task argument optionally"""

    called = 0

    async def dependency_one(
        a: list[str] | None = TaskArgument(optional=True),
    ) -> list[str] | None:
        return a

    async def dependent_task(
        not_a: list[str],
        b: list[str] | None = Depends(dependency_one),
    ) -> None:
        assert not_a == ["hello", "world"]
        assert b is None

        nonlocal called
        called += 1

    await docket.add(dependent_task)(not_a=["hello", "world"])

    await worker.run_until_finished()

    assert called == 1


async def test_sync_function_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of sync functions"""

    def dependency_one() -> str:
        return f"one-{uuid4()}"

    def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_sync_contextual_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of sync context managers"""

    stages: list[str] = []

    @contextmanager
    def dependency_one() -> Generator[str, None, None]:
        stages.append("one-before")
        yield f"one-{uuid4()}"
        stages.append("one-after")

    def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1
    assert stages == ["one-before", "one-after"]


async def test_mixed_sync_and_async_dependencies(docket: Docket, worker: Worker):
    """A task can depend on both sync and async dependencies"""

    def sync_dependency() -> str:
        return f"sync-{uuid4()}"

    async def async_dependency() -> str:
        return f"async-{uuid4()}"

    called = 0

    async def dependent_task(
        sync_val: str = Depends(sync_dependency),
        async_val: str = Depends(async_dependency),
    ):
        assert sync_val.startswith("sync-")
        assert async_val.startswith("async-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_sync_dependencies_of_dependencies(docket: Docket, worker: Worker):
    """A sync task dependency can depend on other sync dependencies"""
    counter = 0

    def dependency_one() -> list[str]:
        nonlocal counter
        counter += 1
        return [f"one-{counter}"]

    def dependency_two(my_one: list[str] = Depends(dependency_one)) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + [f"two-{counter}"]

    def dependency_three(
        my_one: list[str] = Depends(dependency_one),
        my_two: list[str] = Depends(dependency_two),
    ) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + my_two + [f"three-{counter}"]

    async def dependent_task(
        one_a: list[str] = Depends(dependency_one),
        one_b: list[str] = Depends(dependency_one),
        two: list[str] = Depends(dependency_two),
        three: list[str] = Depends(dependency_three),
    ):
        assert one_a is one_b

        assert one_a == ["one-1"]
        assert two == ["one-1", "two-2"]
        assert three == ["one-1", "two-2", "three-3"]

    await docket.add(dependent_task)()

    await worker.run_until_finished()


async def test_sync_dependencies_can_ask_for_docket_dependencies(
    docket: Docket, worker: Worker
):
    """A sync task dependency can ask for a docket dependency"""

    called = 0

    def dependency_one(this_docket: Docket = CurrentDocket()) -> str:
        assert this_docket is docket

        nonlocal called
        called += 1

        return f"one-{called}"

    def dependency_two(
        this_worker: Worker = CurrentWorker(),
        one: str = Depends(dependency_one),
    ) -> str:
        assert this_worker is worker

        assert one == "one-1"

        nonlocal called
        called += 1

        return f"two-{called}"

    async def dependent_task(
        one: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
        this_docket: Docket = CurrentDocket(),
        this_worker: Worker = CurrentWorker(),
    ):
        assert one == "one-1"
        assert two == "two-2"

        assert this_docket is docket
        assert this_worker is worker

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 3


async def test_mixed_sync_async_nested_dependencies(docket: Docket, worker: Worker):
    """Dependencies can mix sync and async at different nesting levels"""
    counter = 0

    def sync_base() -> int:
        nonlocal counter
        counter += 1
        return counter

    async def async_multiplier(base: int = Depends(sync_base)) -> int:
        nonlocal counter
        counter += 1
        return base * 10

    def sync_adder(multiplied: int = Depends(async_multiplier)) -> int:
        nonlocal counter
        counter += 1
        return multiplied + 5

    async def dependent_task(result: int = Depends(sync_adder)):
        # 1 * 10 + 5 = 15
        assert result == 15

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert counter == 3
