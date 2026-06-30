"""Tests for worker liveness announcements."""

import asyncio
from datetime import timedelta
from unittest.mock import patch

from docket import Docket, Worker


async def test_worker_context_without_processing_loop_is_not_announced(
    docket: Docket,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async def liveness_task() -> None: ...

    docket.register(liveness_task)

    async with Worker(docket, name="idle-worker"):
        await asyncio.sleep(heartbeat.total_seconds() * 2)

        snapshot = await docket.snapshot()

    assert {worker.name for worker in snapshot.workers} == set()


async def test_worker_waits_for_cancellation_readiness_before_announcement(
    docket: Docket,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    task_complete = False

    async def delayed_task() -> None:
        nonlocal task_complete
        task_complete = True

    await docket.add(delayed_task)()

    async with Worker(
        docket,
        name="not-ready-worker",
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        listener_ready = asyncio.Event()

        async def slow_listener() -> None:
            await asyncio.sleep(heartbeat.total_seconds() * 8)
            worker._cancellation_ready.set()  # pyright: ignore[reportPrivateUsage]
            listener_ready.set()
            await worker._worker_stopping.wait()  # pyright: ignore[reportPrivateUsage]

        with patch.object(worker, "_cancellation_listener", slow_listener):
            worker_run = asyncio.create_task(worker.run_until_finished())
            try:
                await asyncio.sleep(heartbeat.total_seconds() * 2)

                snapshot = await docket.snapshot()
                await asyncio.wait_for(listener_ready.wait(), timeout=1)
                await asyncio.wait_for(worker_run, timeout=5)
            finally:
                worker_run.cancel()
                await asyncio.gather(worker_run, return_exceptions=True)

    assert {worker.name for worker in snapshot.workers} == set()
    assert task_complete
