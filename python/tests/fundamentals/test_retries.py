"""Tests for error handling and retry strategies."""

import logging
from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock

import pytest

from docket import Docket, ExponentialRetry, Retry, Worker
from docket.dependencies import TaskOutcome
from docket.execution import Execution


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


async def test_retry_errors_are_logged(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """Each retry attempt logs the exception at ERROR level."""
    calls = 0

    async def the_task(retry: Retry = Retry(attempts=3)):
        nonlocal calls
        calls += 1
        raise ValueError("attempt failed")

    await docket.add(the_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert calls == 3
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "attempt failed" in r.getMessage()
        or (r.exc_info and "attempt failed" in str(r.exc_info[1]))
        for r in error_records
    )


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


async def test_supports_retry_forever(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """Retry.forever() creates a retry that keeps trying until the task succeeds."""

    calls = 0

    async def the_task(
        retry: Retry = Retry.forever(),
    ) -> None:
        nonlocal calls
        calls += 1

        assert retry.attempts is None
        assert retry.attempt == calls

        if calls < 3:
            raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert calls == 3


async def test_supports_retry_forever_with_delay(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """Retry.forever() accepts a delay between attempts."""

    calls = 0

    async def the_task(
        retry: Retry = Retry.forever(delay=timedelta(milliseconds=50)),
    ) -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise Exception("Failed")

    await docket.add(the_task)()

    start = now()

    await worker.run_until_finished()

    total_delay = now() - start
    assert total_delay >= timedelta(milliseconds=100)

    assert calls == 3


async def test_supports_exponential_retry_forever(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """ExponentialRetry.forever() retries indefinitely with exponential backoff."""

    calls = 0

    async def the_task(
        retry: Retry = ExponentialRetry.forever(
            delay=timedelta(milliseconds=10),
            maximum_delay=timedelta(milliseconds=100),
        ),
    ) -> None:
        nonlocal calls
        calls += 1

        assert isinstance(retry, ExponentialRetry)
        assert retry.attempts is None

        if calls < 3:
            raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert calls == 3


def test_retry_forever_sets_attempts_to_none():
    """Retry.forever() is equivalent to Retry(attempts=None)."""
    retry = Retry.forever(delay=timedelta(seconds=5))
    assert retry.attempts is None
    assert retry.delay == timedelta(seconds=5)


def test_exponential_retry_forever_sets_attempts_to_none():
    """ExponentialRetry.forever() is equivalent to ExponentialRetry(attempts=None)."""
    retry = ExponentialRetry.forever(
        delay=timedelta(seconds=2),
        maximum_delay=timedelta(minutes=10),
    )
    assert retry.attempts is None
    assert retry.delay == timedelta(seconds=2)
    assert retry.maximum_delay == timedelta(minutes=10)


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
    """Exponential backoff should cap delays at the configured maximum."""

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


class NeverRetry(Retry):
    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


class NeverExponentialRetry(ExponentialRetry):
    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


@pytest.mark.parametrize(
    "retry",
    [NeverRetry(attempts=3), NeverExponentialRetry(attempts=3)],
)
async def test_subclass_decides_whether_to_retry(
    docket: Docket, worker: Worker, retry: Retry
):
    """A subclass's handle_failure decides whether a failed task is retried."""

    calls = 0

    async def the_task(retry: Retry = retry) -> None:
        nonlocal calls
        calls += 1
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert calls == 1


class LabeledRetry(Retry):
    def __init__(self, label: str, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts)
        self.label = label


class LabeledExponentialRetry(ExponentialRetry):
    def __init__(self, label: str, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts, minimum_delay=timedelta(0))
        self.label = label


@pytest.mark.parametrize(
    "retry",
    [
        LabeledRetry("billing", attempts=2),
        LabeledExponentialRetry("billing", attempts=2),
    ],
)
async def test_task_receives_subclass_with_its_own_arguments(
    docket: Docket, worker: Worker, retry: LabeledRetry | LabeledExponentialRetry
):
    """A task receives its retry subclass, including state from a constructor
    that takes different arguments than the base class."""

    received: list[tuple[type[Retry], str, int]] = []

    async def the_task(
        retry: LabeledRetry | LabeledExponentialRetry = retry,
    ) -> None:
        received.append((type(retry), retry.label, retry.attempt))
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert received == [(type(retry), "billing", 1), (type(retry), "billing", 2)]


async def test_forced_retry_delay_applies_to_one_attempt_only(
    docket: Docket, worker: Worker
):
    """The delay a task passes to after() does not change the delay the task
    declared for its later attempts."""

    declared = Retry(attempts=3, delay=timedelta(milliseconds=5))
    delays: list[timedelta] = []

    async def the_task(retry: Retry = declared) -> None:
        delays.append(retry.delay)
        if retry.attempt == 1:
            retry.after(timedelta(milliseconds=10))
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert delays == [timedelta(milliseconds=5)] * 3
    assert declared.delay == timedelta(milliseconds=5)


class CountingRetry(Retry):
    def __init__(self, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts)
        self.failures = 0

    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        self.failures += 1
        return await super().handle_failure(execution, outcome)


async def test_each_attempt_starts_from_the_declared_state(
    docket: Docket, worker: Worker
):
    """What a subclass records on one attempt's copy is not carried to the next
    attempt, which may run on another worker."""

    failures: list[int] = []

    async def the_task(retry: CountingRetry = CountingRetry(attempts=3)) -> None:
        failures.append(retry.failures)
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert failures == [0, 0, 0]


def test_forever_returns_the_subclass():
    """forever() on a subclass is typed as, and returns, that subclass."""
    retry: NeverRetry = NeverRetry.forever()
    exponential: NeverExponentialRetry = NeverExponentialRetry.forever()

    assert (type(retry), type(exponential)) == (NeverRetry, NeverExponentialRetry)
