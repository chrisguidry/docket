import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable

import pytest

from docket import Docket, ExecutionState, Worker
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
