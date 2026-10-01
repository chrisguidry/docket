import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable

import pytest

from docket import Docket, Execution, ExecutionState, Worker
from tests.conftest import wait_until


@pytest.fixture
async def one_slot_worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    async with Worker(
        docket,
        concurrency=1,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        yield worker


async def test_future_task_cancelled_after_it_is_queued_does_not_run(
    docket: Docket,
    one_slot_worker: Worker,
    now: Callable[[], datetime],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    blocker_started = asyncio.Event()
    ran = asyncio.Event()

    async def blocker() -> None:
        blocker_started.set()
        await asyncio.sleep(60)

    async def future_task() -> None:
        ran.set()  # pragma: no cover

    # The blocker takes the worker's only slot, so the worker reads the
    # future task from the stream only after the blocker ends.
    blocking = await docket.add(blocker)()
    soon = now() + timedelta(milliseconds=100)
    execution = await docket.add(future_task, when=soon)()
    worker_run = asyncio.create_task(one_slot_worker.run_until_finished())
    await asyncio.wait_for(blocker_started.wait(), timeout=5.0)

    async def is_queued() -> bool:
        await execution.sync()
        return execution.state == ExecutionState.QUEUED

    await wait_until(is_queued, description="the scheduler to queue the task")
    await docket.cancel(execution.key)
    # Cancelling the blocker frees the slot.  The worker handles cancel
    # signals in the order they are published, so it handles the future
    # task's signal before it reads the task.  A signal that arrived after
    # the read would stop the task before its claim, and this test would
    # pass even if the claim accepted a cancelled task.
    await docket.cancel(blocking.key)
    await asyncio.wait_for(worker_run, timeout=5.0)

    await execution.sync()
    assert not ran.is_set()
    assert execution.state == ExecutionState.CANCELLED
    assert re.search(r"✗ future_task\(\)\S* \(cancelled\)", caplog.text)
    assert "(superseded)" not in caplog.text


async def test_cancelled_key_runs_when_added_again(
    docket: Docket, worker: Worker
) -> None:
    ran = asyncio.Event()

    async def task() -> None:
        ran.set()

    cancelled = await docket.add(task)()
    await docket.cancel(cancelled.key)
    execution = await docket.add(task, key=cancelled.key)()
    await worker.run_until_finished()

    await execution.sync()
    assert ran.is_set()
    assert execution.state == ExecutionState.COMPLETED


async def test_cancel_during_claim_leaves_the_worker_running(
    docket: Docket,
    one_slot_worker: Worker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ran: list[str] = []

    async def record(name: str) -> None:
        ran.append(name)

    real_claim = Execution.claim

    async def cancel_during_claim(self: Execution, worker: str) -> bool:
        # The first claim cancels its own task and then waits, so the
        # worker's cancellation listener cancels the task while _execute
        # still awaits claim().  It puts the real claim back first, so every
        # later task claims as usual.  The cancel and the wait run in one
        # gather, because the listener's cancel can arrive before
        # docket.cancel() returns.
        monkeypatch.setattr(Execution, "claim", real_claim)
        await asyncio.gather(docket.cancel(self.key), asyncio.sleep(10))
        return await real_claim(self, worker)  # pragma: no cover

    monkeypatch.setattr(Execution, "claim", cancel_during_claim)

    # The worker has one slot, so it reads the second task only after it
    # is done with the first.
    first = await docket.add(record)("first")
    await docket.add(record)("second")
    await asyncio.wait_for(one_slot_worker.run_until_finished(), timeout=5.0)

    assert ran == ["second"]

    await first.sync()
    assert first.state == ExecutionState.CANCELLED

    # docket.cancel() cannot acknowledge a message that the worker has read,
    # so the worker has to.
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key, groupname=docket.worker_group_name
        )
    assert pending_info["pending"] == 0
