"""Tests for core dependency injection and retry strategies."""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from docket import CurrentDocket, CurrentWorker, Docket, Worker
from typing import Any, Awaitable, Callable

from docket.dependencies import (
    CompletionHandler,
    Depends,
    ExponentialRetry,
    FailureHandler,
    Perpetual,
    Retry,
    Runtime,
    TaskArgument,
    TaskOutcome,
    Timeout,
)
from docket.execution import Execution


async def test_dependencies_may_be_duplicated(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        docketA: Docket = CurrentDocket(),
        docketB: Docket = CurrentDocket(),
        workerA: Worker = CurrentWorker(),
        workerB: Worker = CurrentWorker(),
    ):
        assert a == "a"
        assert b == "b"
        assert docketA is docket
        assert docketB is docket
        assert workerA is worker
        assert workerB is worker

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert called


async def test_retries_must_be_unique(docket: Docket, worker: Worker):
    async def the_task(
        a: str,
        retryA: Retry = Retry(attempts=3),
        retryB: Retry = Retry(attempts=5),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match="Only one Retry dependency is allowed per task",
    ):
        await docket.add(the_task)("a")


async def test_runtime_subclasses_must_be_unique(docket: Docket, worker: Worker):
    """Two different Runtime subclasses should conflict since Runtime.single=True."""

    class CustomRuntime(Runtime):
        async def __aenter__(self) -> "CustomRuntime":
            return self  # pragma: no cover

        async def run(
            self,
            execution: Execution,
            function: Callable[..., Awaitable[Any]],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> Any:
            return await function(*args, **kwargs)  # pragma: no cover

    async def the_task(
        a: str,
        timeout: Timeout = Timeout(timedelta(seconds=10)),
        custom: CustomRuntime = CustomRuntime(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one Runtime dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")


async def test_users_can_provide_dependencies_directly(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=3),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 42

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b", retry=Retry(attempts=42))

    await worker.run_until_finished()

    assert called


async def test_user_provide_retries_are_used(docket: Docket, worker: Worker):
    calls = 0

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=42),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 2

        nonlocal calls
        calls += 1

        raise Exception("womp womp")

    await docket.add(the_task)("a", "b", retry=Retry(attempts=2))

    await worker.run_until_finished()

    assert calls == 2


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_in_timedelta_time(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            first_call_time = datetime.now(timezone.utc)
            retry.in_(timedelta(seconds=0.5))
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_at_a_specific_time(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) + timedelta(seconds=0.5)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_user_can_request_a_retry_at_a_specific_time_in_the_past(
    docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=2),
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_dependencies_error_for_missing_task_argument(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task will fail when asking for a missing task argument"""

    async def dependency_one(nope: list[str] = TaskArgument()) -> list[str]:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def dependent_task(
        a: list[str],
        b: list[str] = TaskArgument("a"),
        c: list[str] = Depends(dependency_one),
    ) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)(a=["hello", "world"])

    await worker.run_until_finished()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): c" in caplog.text
    assert "ExceptionGroup" in caplog.text
    assert "KeyError: 'nope'" in caplog.text


async def test_a_task_argument_cannot_ask_for_itself(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task argument cannot ask for itself"""

    # This task would be nonsense, because it's asking for itself.
    async def dependent_task(a: list[str] = TaskArgument()) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): a" in caplog.text
    assert "ValueError: No parameter name specified" in caplog.text


async def test_failure_handler_subclasses_must_be_unique(
    docket: Docket, worker: Worker
):
    """Two different FailureHandler subclasses should conflict since FailureHandler.single=True."""

    class CustomFailureHandler(FailureHandler):
        async def __aenter__(self) -> "CustomFailureHandler":
            return self  # pragma: no cover

        async def handle_failure(
            self, execution: Execution, outcome: TaskOutcome
        ) -> bool:
            return False  # pragma: no cover

    async def the_task(
        a: str,
        retry: Retry = Retry(attempts=3),
        custom: CustomFailureHandler = CustomFailureHandler(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one FailureHandler dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")


async def test_completion_handler_subclasses_must_be_unique(
    docket: Docket, worker: Worker
):
    """Two different CompletionHandler subclasses should conflict since CompletionHandler.single=True."""

    class CustomCompletionHandler(CompletionHandler):
        async def __aenter__(self) -> "CustomCompletionHandler":
            return self  # pragma: no cover

        async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
            return False  # pragma: no cover

    async def the_task(
        a: str,
        perpetual: Perpetual = Perpetual(),
        custom: CustomCompletionHandler = CustomCompletionHandler(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one CompletionHandler dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")
