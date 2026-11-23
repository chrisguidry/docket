"""Each run of a task works with its own copy of the perpetual the task
declared."""

import asyncio
from datetime import timedelta

from docket import Docket, Perpetual, Worker


async def test_calling_a_perpetual_task_directly_does_not_affect_worker_runs(
    docket: Docket, worker: Worker
):
    """A direct call passes the declared default itself, so a cancel() or
    after() it makes must not carry into later runs on a worker."""
    worker_calls = 0

    async def perpetual_task(
        direct: bool = False,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ):
        nonlocal worker_calls
        if direct:
            perpetual.after(timedelta(hours=1))
            perpetual.cancel()
            return
        worker_calls += 1
        if worker_calls == 3:
            perpetual.cancel()

    await perpetual_task(direct=True)

    await docket.add(perpetual_task)()

    async with asyncio.timeout(5):
        await worker.run_until_finished()

    assert worker_calls == 3
