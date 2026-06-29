"""Tests for one-shot worker completion semantics."""

import asyncio
import sys
from datetime import timedelta

from docket import Docket, Worker
from docket.dependencies import Perpetual, Retry
from tests.conftest import wait_until

if sys.version_info >= (3, 11):  # pragma: no cover
    from asyncio import timeout as async_timeout
else:  # pragma: no cover
    from async_timeout import timeout as async_timeout


async def test_run_until_finished_drains_delayed_retry_it_created(docket: Docket):
    attempts = 0

    async def flaky_task(
        retry: Retry = Retry(attempts=2, delay=timedelta(seconds=1.5)),
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("retry me")

    await docket.add(flaky_task, key="delayed-retry")()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        async with async_timeout(6.0):  # pragma: no branch
            await worker.run_until_finished()

    snapshot = await docket.snapshot()

    assert attempts == 2
    assert snapshot.total_tasks == 0


async def test_run_until_finished_stays_alive_for_delayed_perpetual_successor(
    docket: Docket,
):
    calls = 0

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=1.5)),
    ) -> None:
        nonlocal calls
        calls += 1
        if calls >= 2:
            perpetual.cancel()

    await docket.add(perpetual_task, key="delayed-perpetual")()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_until_finished())

        await wait_until(
            lambda: calls == 1,
            timeout=2.0,
            description="first perpetual call",
        )
        await asyncio.sleep(0.2)
        assert not worker_task.done()

        async with async_timeout(6.0):  # pragma: no branch
            await worker_task

    snapshot = await docket.snapshot()

    assert calls == 2
    assert snapshot.total_tasks == 0


async def test_run_until_finished_exits_when_created_retry_is_cancelled(
    docket: Docket,
):
    attempts = 0
    first_attempt_finished = asyncio.Event()

    async def flaky_task(
        retry: Retry = Retry(attempts=2, delay=timedelta(seconds=1.5)),
    ) -> None:
        nonlocal attempts
        attempts += 1
        first_attempt_finished.set()
        raise RuntimeError("cancel retry")

    await docket.add(flaky_task, key="cancelled-retry")()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_until_finished())
        await asyncio.wait_for(first_attempt_finished.wait(), timeout=2.0)

        async def retry_is_scheduled() -> bool:
            execution = await docket.get_execution("cancelled-retry")
            return execution is not None and execution.state.value == "scheduled"

        await wait_until(
            retry_is_scheduled,
            timeout=2.0,
            description="delayed retry schedule before cancellation",
        )
        await docket.cancel("cancelled-retry")
        async with async_timeout(6.0):  # pragma: no branch
            await worker_task

    snapshot = await docket.snapshot()

    assert attempts == 1
    assert snapshot.total_tasks == 0
