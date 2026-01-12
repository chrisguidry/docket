"""Tests for error handling and retry strategies."""

from datetime import datetime, timedelta
from typing import Callable
from unittest.mock import AsyncMock

import pytest

from docket import Docket, ExponentialRetry, Retry, Worker


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
