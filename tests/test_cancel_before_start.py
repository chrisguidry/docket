"""Tests for cancelling a task before a worker starts it.

A cancel can land while the task waits in the queue, after the scheduler moves
it to the stream, or after a worker reads it but before the worker claims it.
In every case the task must never run, and anyone waiting on it must hear
that it was cancelled.  Cancelling a running task is covered in
tests/test_cancellation.py.
"""

import asyncio
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable

import pytest

from docket import Docket, Execution, ExecutionCancelled, ExecutionState, Worker
from docket.execution import ProgressEvent, StateEvent
from tests.conftest import wait_until


async def test_cancelling_a_due_task_after_the_scheduler_queues_it(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """A future task still stops when the cancel lands after the scheduler has
    moved it to the stream."""
    release = asyncio.Event()
    calls: list[str] = []

    async def blocker() -> None:
        await release.wait()

    async def reminder() -> None:
        calls.append("reminder")  # pragma: no cover

    # With its one slot taken by the blocker, the worker keeps moving due tasks
    # to the stream but does not read them.
    worker.concurrency = 1
    await docket.add(blocker)()
    execution = await docket.add(reminder, when=now() + timedelta(milliseconds=50))()
    run = asyncio.create_task(worker.run_until_finished())

    async def is_queued() -> bool:
        await execution.sync()
        return execution.state == ExecutionState.QUEUED

    await wait_until(is_queued, description="the scheduler to queue the task")
    await docket.cancel(execution.key)
    release.set()
    await asyncio.wait_for(run, timeout=5)

    assert calls == []
    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED


async def test_a_worker_never_claims_a_cancelled_task(docket: Docket, worker: Worker):
    """A worker refuses to claim a cancelled task, even when the cancel could not
    find the task's stream entry to remove it."""
    calls: list[str] = []

    async def reminder() -> None:
        calls.append("reminder")  # pragma: no cover

    execution = await docket.add(reminder)()
    # A worker that read the entry before the cancel still holds it, and an
    # older scheduler moved due tasks without recording where the entry went.
    # Both leave an entry that the cancel cannot delete.
    async with docket.redis() as redis:
        await redis.hdel(docket.runs_key(execution.key), "stream_id")
    await docket.cancel(execution.key)

    await worker.run_until_finished()

    assert calls == []
    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED
    async with docket.redis() as redis:
        pending = await redis.xpending(docket.stream_key, docket.worker_group_name)
    assert pending["pending"] == 0


async def test_a_cancel_that_interrupts_a_claim_leaves_the_worker_running(
    docket: Docket, worker: Worker, monkeypatch: pytest.MonkeyPatch
):
    """When the cancel signal reaches a task while its worker is still claiming
    it, only that task stops; the worker carries on with the rest of its work."""
    claiming = asyncio.Event()
    calls: list[str] = []
    original_claim = Execution.claim

    async def claim_until_cancelled(execution: Execution, worker_name: str) -> bool:
        if execution.key == "cancelled":
            claiming.set()
            await asyncio.Event().wait()
        return await original_claim(execution, worker_name)

    monkeypatch.setattr(Execution, "claim", claim_until_cancelled)

    async def reminder(label: str) -> None:
        calls.append(label)

    worker.concurrency = 1
    cancelled = await docket.add(reminder, key="cancelled")("cancelled")
    await docket.add(reminder, key="kept")("kept")
    run = asyncio.create_task(worker.run_until_finished())

    await asyncio.wait_for(claiming.wait(), timeout=5)
    await docket.cancel(cancelled.key)
    await asyncio.wait_for(run, timeout=5)

    assert calls == ["kept"]
    await cancelled.sync()
    assert cancelled.state == ExecutionState.CANCELLED


async def unstarted_task() -> None: ...  # pragma: no cover


async def test_cancelling_a_task_that_has_not_started_publishes_its_state(
    docket: Docket, now: Callable[[], datetime]
):
    """Cancelling a task that has not started publishes a cancelled state event."""
    execution = await docket.add(unstarted_task, when=now() + timedelta(hours=1))()
    subscribed = asyncio.Event()
    events: list[StateEvent] = []

    async def collect() -> None:
        async for event in execution.subscribe(ready=subscribed):  # pragma: no branch
            if event["type"] == "state":
                events.append(event)
                if event["state"] == ExecutionState.CANCELLED:
                    break

    collector = asyncio.create_task(collect())
    await asyncio.wait_for(subscribed.wait(), timeout=5)
    await docket.cancel(execution.key)
    await asyncio.wait_for(collector, timeout=5)

    assert [event["state"] for event in events] == [
        ExecutionState.SCHEDULED,
        ExecutionState.CANCELLED,
    ]
    assert events[-1]["completed_at"] is not None


@pytest.fixture
def subscribed(monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """Set once an Execution.subscribe() call has read the task's current state.

    get_result() subscribes internally, so a test cannot pass its own ``ready``
    event.  This fixture passes one on its behalf.
    """
    event = asyncio.Event()
    original_subscribe = Execution.subscribe

    def subscribe(
        execution: Execution, *, ready: asyncio.Event | None = None
    ) -> AsyncGenerator[StateEvent | ProgressEvent, None]:
        return original_subscribe(execution, ready=event)

    monkeypatch.setattr(Execution, "subscribe", subscribe)
    return event


@pytest.mark.parametrize(
    "delay", [timedelta(0), timedelta(hours=1)], ids=["queued", "scheduled"]
)
async def test_cancel_wakes_a_get_result_waiter(
    docket: Docket,
    subscribed: asyncio.Event,
    now: Callable[[], datetime],
    delay: timedelta,
):
    """A get_result() waiter on a task that has not started raises
    ExecutionCancelled as soon as the task is cancelled."""
    execution = await docket.add(unstarted_task, when=now() + delay)()
    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))

    await asyncio.wait_for(subscribed.wait(), timeout=5)
    await docket.cancel(execution.key)

    with pytest.raises(ExecutionCancelled):
        await waiter
