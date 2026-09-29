"""What a waiting ``get_result()`` sees when its task is cancelled."""

import asyncio
from datetime import datetime, timedelta
from typing import Callable

import pytest

from docket import Docket, ExecutionCancelled, Perpetual, Worker
from tests.conftest import wait_until


async def wait_for_subscriber(docket: Docket, key: str) -> None:
    """Wait until a client has subscribed to the task's state channel.

    ``get_result()`` takes no ``ready`` event, so this polls the server's list
    of channels that have subscribers.  A waiter that reads the runs hash after
    the cancel raises without any event.  A waiter that subscribes after the
    cancel misses the event.
    """
    channel = docket.key(f"state:{key}")

    async def subscribed() -> bool:
        async with docket.redis() as redis:
            return bool(await redis.pubsub_channels(channel.encode()))  # type: ignore[attr-defined]

    await wait_until(subscribed, description=f"a subscriber on {channel}")


@pytest.mark.parametrize(
    "delay",
    [
        pytest.param(timedelta(hours=1), id="scheduled"),
        pytest.param(timedelta(0), id="queued"),
    ],
)
async def test_cancel_wakes_get_result_before_the_task_starts(
    docket: Docket, now: Callable[[], datetime], delay: timedelta
):
    """No worker holds a task that has not started, so the cancel itself has to
    publish the cancelled state."""

    async def never_runs() -> None: ...

    execution = await docket.add(never_runs, when=now() + delay)()
    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))
    await wait_for_subscriber(docket, execution.key)

    await docket.cancel(execution.key)

    with pytest.raises(ExecutionCancelled):
        await waiter


async def test_cancel_wakes_get_result_on_a_running_task(
    docket: Docket, worker: Worker
):
    """The worker publishes the cancelled state when it stops the task."""
    started = asyncio.Event()

    async def runs_until_cancelled() -> None:
        started.set()
        await asyncio.sleep(60)

    execution = await docket.add(runs_until_cancelled)()
    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))
    await wait_for_subscriber(docket, execution.key)

    await docket.cancel(execution.key)

    with pytest.raises(ExecutionCancelled):
        await waiter
    await asyncio.wait_for(worker_task, timeout=5.0)


async def test_a_perpetual_that_cancels_itself_still_returns_its_result(
    docket: Docket, worker: Worker
):
    """``Perpetual.on_complete`` cancels the key while the run is still running,
    and the worker marks the run completed after that.  If that cancel
    published the cancelled state, the waiter would raise ``ExecutionCancelled``."""

    async def last_run(perpetual: Perpetual = Perpetual()) -> str:
        perpetual.cancel()
        return "the last result"

    execution = await docket.add(last_run)()
    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))
    await wait_for_subscriber(docket, execution.key)

    await worker.run_until_finished()

    assert await waiter == "the last result"
