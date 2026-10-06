"""A task parked on a concurrency limit shows no worker and no start time."""

import asyncio

from docket import ConcurrencyLimit, Docket, Worker
from docket.execution import ExecutionState

from tests.concurrency_limits.waiters import wait_for_xlen


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

    # The waiter goes in only after the holder has its slot, because the
    # worker may start either of two tasks it reads together.
    await docket.add(holder)(customer_id=1)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await started.wait()
    waiter = await docket.add(holder)(customer_id=1)
    await wait_for_xlen(docket, f"{docket.prefix}:concurrency:customer_id:1:waiters", 1)

    await waiter.sync()
    parked = (waiter.state, waiter.worker, waiter.started_at)

    hold.set()
    await worker_task

    assert parked == (ExecutionState.SCHEDULED, None, None)
