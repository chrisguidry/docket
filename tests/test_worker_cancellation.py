"""Regression tests: cancelling a worker externally must always shut it down.

asyncio.wait_for on Python 3.10 and 3.11 swallows a cancellation that is
delivered to the caller in the same event-loop tick that the inner future
completes (python/cpython#86296; the 3.12 rewrite fixed it).  The worker
waits on events between polls, so a worker cancelled at exactly that moment
lost the cancellation and ran forever.  These tests cancel at a range of
event-loop tick offsets so at least one case lands inside that window.
"""

import asyncio

import pytest

from docket import Docket, Worker
from docket._cancellation import (
    _wait_for_event,  # pyright: ignore[reportPrivateUsage]
    cancel_task,
)

OFFSETS = [0, 1, 2, 3]


async def test_wait_for_event_returns_true_for_a_set_event():
    """An already-set event returns True without waiting"""
    event = asyncio.Event()
    event.set()

    assert await _wait_for_event(event, 10.0) is True


async def test_wait_for_event_returns_false_on_timeout():
    """An event that never sets returns False after the timeout"""
    event = asyncio.Event()

    assert await _wait_for_event(event, 0.01) is False


async def test_cancel_task_reraises_a_real_exception():
    """A subtask that fails instead of cancelling re-raises its exception"""

    async def defiant() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("cleanup went sideways") from None

    task = asyncio.create_task(defiant())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="cleanup went sideways"):
        await cancel_task(task, "test cleanup")


async def test_cancel_task_tolerates_a_task_that_finishes_anyway():
    """A subtask that absorbs the cancellation and returns is left alone"""

    async def stubborn() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(stubborn())
    await asyncio.sleep(0)

    await cancel_task(task, "test cleanup")

    assert task.done()
    assert not task.cancelled()


async def _wait_for_worker_readiness(worker: Worker) -> None:
    """Yield until the worker's cancellation listener reports readiness."""
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        session = worker._processing_session  # pyright: ignore[reportPrivateUsage]
        if session is not None and session.cancellation_ready.is_set():
            return
        await asyncio.sleep(0)
    raise TimeoutError("worker never became ready")


@pytest.mark.parametrize("offset", OFFSETS)
async def test_wait_for_event_honors_caller_cancellation(offset: int):
    """A cancellation that races the event being set still cancels the caller"""
    event = asyncio.Event()
    task = asyncio.create_task(_wait_for_event(event, 10.0))
    await asyncio.sleep(0)

    event.set()
    for _ in range(offset):
        await asyncio.sleep(0)
    cancel_requested = task.cancel()

    await asyncio.wait([task], timeout=5)

    assert task.done(), "the wait outlived its cancellation"
    assert not cancel_requested or task.cancelled(), (
        "the caller's cancellation was swallowed"
    )


@pytest.mark.parametrize("offset", OFFSETS)
async def test_cancel_task_propagates_caller_cancellation(offset: int):
    """A cancellation that races the subtask's own cancellation still propagates"""
    forever = asyncio.Event()
    inner = asyncio.create_task(forever.wait())
    await asyncio.sleep(0)

    outer = asyncio.create_task(cancel_task(inner, "test cleanup"))
    for _ in range(offset):
        await asyncio.sleep(0)
    cancel_requested = outer.cancel()

    await asyncio.wait([outer], timeout=5)

    assert outer.done(), "cancel_task outlived its cancellation"
    assert not cancel_requested or outer.cancelled(), (
        "the caller's cancellation was swallowed"
    )


@pytest.mark.parametrize("offset", OFFSETS)
async def test_cancelling_run_forever_at_readiness_shuts_down(
    docket: Docket, offset: int
):
    """run_forever cancelled the moment the worker becomes ready still exits"""
    async with Worker(docket, name=f"cancel-me-{offset}") as worker:
        run_task = asyncio.create_task(worker.run_forever())
        await _wait_for_worker_readiness(worker)

        for _ in range(offset):
            await asyncio.sleep(0)
        run_task.cancel()

        _, pending = await asyncio.wait([run_task], timeout=5)

        assert not pending, "the worker did not shut down after cancellation"
