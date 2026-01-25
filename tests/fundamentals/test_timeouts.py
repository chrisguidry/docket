"""Tests for Timeout dependency."""

import asyncio
from datetime import datetime, timedelta, timezone

from docket import Docket, Retry, Timeout, Worker


async def test_simple_timeout(docket: Docket, worker: Worker):
    """A task with a timeout completes normally when it finishes before the limit."""

    remaining_at_end: timedelta | None = None

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(milliseconds=500)),
    ):
        await asyncio.sleep(0.01)

        nonlocal remaining_at_end
        remaining_at_end = timeout.remaining()

    await docket.add(task_with_timeout)()

    await worker.run_until_finished()

    # Task completed with time to spare
    assert remaining_at_end is not None
    assert remaining_at_end > timedelta(0)


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
    # Task should complete well before the 5s sleep - timeout cancelled it
    assert elapsed < timedelta(seconds=1)


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
    # Task should complete well before the 5s sleep - timeout cancelled it
    assert elapsed < timedelta(seconds=1)


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
    # Task should complete well before the 5s sleep - timeout cancelled it
    assert elapsed < timedelta(seconds=1)


async def test_timeout_is_compatible_with_retry(docket: Docket, worker: Worker):
    """A task that times out can be retried"""

    successes: list[int] = []

    async def task_with_timeout(
        retry: Retry = Retry(attempts=3),
        _timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        if retry.attempt == 1:
            await asyncio.sleep(1)

        successes.append(retry.attempt)

    await docket.add(task_with_timeout)()

    await worker.run_until_finished()

    assert successes == [2]
