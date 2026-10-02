"""Tests for Docket.snapshot()."""

import asyncio

from docket import Docket, Worker


async def test_snapshot_reports_a_running_task_and_its_worker(
    docket: Docket, worker: Worker
):
    started = asyncio.Event()
    release = asyncio.Event()

    async def held_task() -> None:
        started.set()
        await asyncio.wait_for(release.wait(), timeout=5)

    await docket.add(held_task, key="held")()
    worker_run = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5)

    snapshot = await docket.snapshot()

    release.set()
    await asyncio.wait_for(worker_run, timeout=5)

    assert [(r.key, r.worker) for r in snapshot.running] == [("held", worker.name)]
