"""What the worker does with a task that ends in CancelledError without docket.cancel().

While the worker shuts down, it leaves the task's message pending, and another
worker runs the task again after redelivery_timeout.  At any other time, the
task ends cancelled, and the worker acknowledges its message.
"""

import asyncio
import sys
from datetime import timedelta
from typing import Any, Awaitable, Callable, cast

import pytest

from docket import Docket, ExecutionState, Worker
from tests.conftest import wait_until


async def raise_cancelled_error() -> None:
    raise asyncio.CancelledError()


async def cancel_own_task() -> None:
    cast(asyncio.Task[Any], asyncio.current_task()).cancel()
    await asyncio.sleep(1)


@pytest.mark.parametrize(
    "end_in_cancelled_error", [raise_cancelled_error, cancel_own_task]
)
async def test_task_that_cancels_itself_ends_cancelled(
    docket: Docket, end_in_cancelled_error: Callable[[], Awaitable[None]]
):
    """A task whose body ends in CancelledError on its own runs once and ends
    cancelled.

    Nobody called docket.cancel(), and the worker is not shutting down, so the
    worker acknowledges the message.  If the worker left it pending, a task
    that cancels itself would run again after every redelivery_timeout.
    """
    runs = 0

    async def the_task() -> None:
        nonlocal runs
        runs += 1
        if runs == 1:  # pragma: no branch - only a redelivery runs it again
            await end_in_cancelled_error()

    execution = await docket.add(the_task)()

    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        await worker.run_until_finished()

    await execution.sync()
    async with docket.redis() as redis:
        pending = await redis.xpending(docket.stream_key, docket.worker_group_name)

    assert runs == 1
    assert execution.state == ExecutionState.CANCELLED
    assert pending["pending"] == 0


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="asyncio.Task has no cancelling() before Python 3.11",
)
async def test_task_interrupted_by_loop_shutdown_is_redelivered(  # pragma: no cover
    docket: Docket,
):
    """A task interrupted by event loop shutdown runs again on another worker.

    Nobody called docket.cancel(), so the worker should leave the message
    pending for redelivery instead of marking the task cancelled.
    """
    started: list[asyncio.Task[Any]] = []
    release = asyncio.Event()

    async def long_task() -> None:
        started.append(cast(asyncio.Task[Any], asyncio.current_task()))
        await release.wait()

    await docket.add(long_task)()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        run = asyncio.create_task(worker_a.run_until_finished())
        await wait_until(lambda: len(started) == 1)

        # Before asyncio.run() returns or raises, for example after a task
        # calls sys.exit(), it cancels every task still running and waits for
        # all of them.  Here those are the task that runs the worker and the
        # task that runs long_task.
        run.cancel()
        started[0].cancel()
        await asyncio.gather(run, started[0], return_exceptions=True)

    release.set()
    await asyncio.sleep(0.25)  # longer than the redelivery timeout

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_until_finished()

    assert len(started) == 2


async def test_task_cancelled_while_the_worker_drains_is_redelivered(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    """A task cancelled while the worker drains runs again on another worker.

    A second SIGTERM or SIGINT cancels the tasks that the worker is draining.
    Nobody called docket.cancel(), so the worker leaves their messages pending
    for redelivery.
    """
    started: list[asyncio.Task[Any]] = []
    release = asyncio.Event()

    async def long_task() -> None:
        started.append(cast(asyncio.Task[Any], asyncio.current_task()))
        await release.wait()

    await docket.add(long_task)()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        run = asyncio.create_task(worker_a.run_until_finished())
        await wait_until(lambda: len(started) == 1)

        run.cancel()
        # The worker logs this in the same step that starts its drain.
        await wait_until(lambda: "Shutdown requested, finishing" in caplog.text)
        started[0].cancel()
        await asyncio.gather(run, return_exceptions=True)

    release.set()
    await asyncio.sleep(0.25)  # longer than the redelivery timeout

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_until_finished()

    assert len(started) == 2
