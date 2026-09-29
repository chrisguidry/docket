from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from docket import Docket, ExponentialRetry, Retry, Worker
from docket.dependencies._base import current_execution


@pytest.mark.parametrize(
    "attempt, minimum, maximum, expected",
    [
        (1, 1, 10, 1),
        (2, 1, 10, 2),
        (4, 1, 10, 8),
        (4, 1, 8, 8),
        (5, 1, 10, 10),
        (47, 1, 10, 10),
        (48, 1, 10, 10),
        (1000, 1, 10, 10),
        (1_000_000_000, 1, 10, 10),
        (1, 10, 1, 10),
        (2, 10, 1, 1),
        (1_000_000_000, 0, 10, 0),
        (2, 1, 0, 0),
        (3, -1, 10, -4),
    ],
)
async def test_exponential_retry_delay_is_bounded(
    attempt: int, minimum: int, maximum: int, expected: int
) -> None:
    retry = ExponentialRetry.forever(
        delay=timedelta(seconds=minimum),
        maximum_delay=timedelta(seconds=maximum),
    )
    execution = MagicMock(attempt=attempt)
    token = current_execution.set(execution)
    try:
        async with retry as resolved:
            assert resolved.delay == timedelta(seconds=expected)
            assert resolved.attempt == attempt
            assert resolved.attempts is None
    finally:
        current_execution.reset(token)


@pytest.mark.parametrize(
    "attempt, expected",
    [(67, timedelta(microseconds=2**66)), (68, timedelta.max)],
)
async def test_exponential_retry_caps_at_timedelta_max(
    attempt: int, expected: timedelta
) -> None:
    retry = ExponentialRetry.forever(
        delay=timedelta.resolution, maximum_delay=timedelta.max
    )
    execution = MagicMock(attempt=attempt)
    token = current_execution.set(execution)
    try:
        async with retry as resolved:
            assert resolved.delay == expected
    finally:
        current_execution.reset(token)


async def test_worker_retries_past_exponential_overflow(
    docket: Docket, worker: Worker
) -> None:
    attempts: list[int] = []

    async def task(
        retry: Retry = ExponentialRetry.forever(
            delay=timedelta(milliseconds=1), maximum_delay=timedelta(milliseconds=10)
        ),
    ) -> None:
        attempts.append(retry.attempt)
        if len(attempts) == 1:
            raise ConnectionError("temporary outage")

    execution = await docket.add(task)()
    execution.attempt = 58
    await execution.schedule(replace=True)
    await worker.run_until_finished()

    assert attempts == [58, 59]
