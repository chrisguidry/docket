"""A task parked on a concurrency limit shows no worker and no start time."""

import asyncio
import time

from docket import ConcurrencyLimit, Docket, Worker
from docket.execution import ExecutionState


async def _wait_for_xlen(docket: Docket, key: str, target: int) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        async with docket.redis() as redis:
            size = await redis.xlen(key)
        if size == target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(  # pragma: no cover
        f"XLEN({key}) did not reach {target} in time"
    )


async def test_a_parked_task_has_no_worker_or_start_time(
    docket: Docket, worker: Worker
):
    started = asyncio.Event()
    hold = asyncio.Event()

    async def holder(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        started.set()
        await hold.wait()

    await docket.add(holder)(customer_id=1)
    waiter = await docket.add(holder)(customer_id=1)

    worker_task = asyncio.create_task(worker.run_until_finished())
    await started.wait()
    await _wait_for_xlen(
        docket, f"{docket.prefix}:concurrency:customer_id:1:waiters", 1
    )

    await waiter.sync()
    parked = (waiter.state, waiter.worker, waiter.started_at)

    hold.set()
    await worker_task

    assert parked == (ExecutionState.SCHEDULED, None, None)
