"""What a caller waiting on ``get_result()`` or ``subscribe()`` sees when its
task is cancelled."""

import asyncio
from contextlib import aclosing
from datetime import datetime, timedelta
from typing import AsyncIterator, Callable

import pytest

from docket import (
    Docket,
    Execution,
    ExecutionCancelled,
    ExecutionState,
    Perpetual,
    Worker,
)
from docket.execution import ProgressEvent, StateEvent
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


@pytest.mark.parametrize(
    "delay",
    [
        pytest.param(timedelta(hours=1), id="scheduled"),
        pytest.param(timedelta(0), id="queued"),
    ],
)
async def test_clear_wakes_get_result_before_the_task_starts(
    docket: Docket, now: Callable[[], datetime], delay: timedelta
):
    """clear() cancels each task that has not started, so it publishes the
    cancelled state the way cancel() does."""

    async def never_runs() -> None: ...

    execution = await docket.add(never_runs, when=now() + delay)()
    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))
    await wait_for_subscriber(docket, execution.key)

    await docket.clear()

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


async def claim_as_a_worker_that_dies(docket: Docket) -> Execution:
    """Read and claim the next task as a worker that then dies.

    A worker killed mid-run writes nothing more to Redis.  Its message stays
    pending, and the runs hash says the task is running.  Returns the execution
    it claimed, whose ``message_id`` is that message.
    """
    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
    async with docket.redis() as redis:
        reply = await redis.xreadgroup(
            groupname=docket.worker_group_name,
            consumername="dead-worker",
            streams={docket.stream_key: ">"},
            count=1,
        )
    assert reply
    [(_, [(message_id, message)])] = reply
    execution = await Execution.from_message(
        docket, message, message_id=message_id, sync=False
    )
    assert await execution.claim("dead-worker")
    return execution


async def test_cancel_wakes_get_result_on_a_task_whose_worker_died(docket: Docket):
    """The cancel leaves a running task's cancelled state to its worker.  When
    that worker is dead, the redelivery sweep reclaims its message for another
    worker, and that worker's claim refuses the cancelled key.  So the claim
    has to publish the cancelled state."""

    async def never_runs() -> None: ...

    execution = await docket.add(never_runs)()
    await claim_as_a_worker_that_dies(docket)

    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=3)))
    await wait_for_subscriber(docket, execution.key)

    await docket.cancel(execution.key)

    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        await worker.run_until_finished()

    with pytest.raises(ExecutionCancelled):
        await waiter


async def next_cancelled_event(
    events: AsyncIterator[StateEvent | ProgressEvent],
) -> StateEvent:
    """Return the next cancelled state event, and skip the events before it.

    Those are the current state and progress, and on Redis Cluster also an
    event published just before the subscription.  A PUBLISH reaches the
    subscriber's node over the cluster bus, so it can arrive after the
    subscription.
    """
    while True:
        event = await anext(events)
        if event["type"] == "state" and event["state"] == ExecutionState.CANCELLED:
            return event


async def test_a_refused_claim_publishes_the_stored_completed_at(docket: Docket):
    """The claim's cancelled event has the completed_at that the cancel stored,
    so a subscriber sees the same time that ``sync()`` reads.  The claim itself
    can run minutes later, after the redelivery_timeout."""

    async def never_runs() -> None: ...

    execution = await docket.add(never_runs)()
    redelivered = await claim_as_a_worker_that_dies(docket)

    subscribed = asyncio.Event()
    async with aclosing(execution.subscribe(ready=subscribed)) as events:
        waiter = asyncio.create_task(next_cancelled_event(events))
        await asyncio.wait_for(subscribed.wait(), timeout=5)
        await docket.cancel(execution.key)
        await redelivered.claim("next-worker")
        cancelled = await asyncio.wait_for(waiter, timeout=5)

    await execution.sync()
    assert execution.completed_at is not None
    assert cancelled == {
        "type": "state",
        "key": execution.key,
        "state": ExecutionState.CANCELLED,
        "completed_at": execution.completed_at.isoformat(),
    }


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
