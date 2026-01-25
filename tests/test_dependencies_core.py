"""Tests for core dependency injection and retry strategies."""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from docket import CurrentDocket, CurrentWorker, Docket, Worker
from docket.dependencies import (
    Depends,
    ExponentialRetry,
    Retry,
    TaskArgument,
)


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
async def test_user_can_request_a_retry_after_a_delay(
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
            retry.after(timedelta(seconds=0.5))
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


async def test_retry_in_is_backwards_compatible_alias_for_after(
    docket: Docket, worker: Worker
):
    """retry.in_() still works as an alias for retry.after()"""
    calls = 0

    async def the_task(retry: Retry = Retry(attempts=2)):
        nonlocal calls
        calls += 1
        if calls == 1:
            retry.in_(timedelta(seconds=0.1))

    await docket.add(the_task)()
    await worker.run_until_finished()

    assert calls == 2


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
