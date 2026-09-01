"""Tests for the fleet-wide lease that gates the scheduler's queue scan.

Every worker runs the scheduler loop, but only the worker that takes the lease
scans the scheduled-tasks queue each tick.  The move is idempotent, so the lease
changes no scheduling outcome; it only spares the fleet the redundant scans.  It
must never delay a due task.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Worker
from docket import worker as worker_module


async def test_a_scheduled_task_still_moves_and_runs_with_the_lease(
    docket: Docket, worker: Worker, the_task: AsyncMock, now: Callable[[], datetime]
):
    """A future task the scheduler must move to the stream still runs promptly.

    A single worker always wins its own lease, so the lease never keeps it from
    scanning.  The task lands in the queue, the scheduler moves it, and it runs.
    """
    when = now() + timedelta(milliseconds=50)
    await docket.add(the_task, when, key="scheduled")("a", b="b")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("a", b="b")


async def test_two_workers_scan_fewer_times_than_they_check(
    docket: Docket,
    the_task: AsyncMock,
    now: Callable[[], datetime],
    monkeypatch: pytest.MonkeyPatch,
):
    """Two workers check the lease every tick, but the fleet scans less often.

    Each worker attempts the lease on every scheduler tick; only the one that
    wins scans the queue.  So the fleet runs strictly fewer scans than checks,
    which is the whole point of the lease.  Every scheduled task still runs.
    """
    checks = 0
    scans = 0
    real_take_lease = worker_module.take_lease
    real_stream_due = worker_module._stream_due_tasks  # pyright: ignore[reportPrivateUsage]

    async def counting_take_lease(*args: object, **kwargs: object) -> bool:
        nonlocal checks
        checks += 1
        return await real_take_lease(*args, **kwargs)  # type: ignore[arg-type]

    async def counting_stream_due(*args: object, **kwargs: object) -> tuple[int, int]:
        nonlocal scans
        scans += 1
        return await real_stream_due(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(worker_module, "take_lease", counting_take_lease)
    monkeypatch.setattr(worker_module, "_stream_due_tasks", counting_stream_due)

    for i in range(6):
        when = now() + timedelta(milliseconds=40 * i)
        await docket.add(the_task, when, key=f"scheduled-{i}")()

    resolution = timedelta(milliseconds=20)
    worker1 = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=resolution,
    )
    worker2 = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=resolution,
    )
    async with worker1, worker2:
        run1 = asyncio.create_task(worker1.run_until_finished())
        run2 = asyncio.create_task(worker2.run_until_finished())
        await run1
        await run2

    assert the_task.await_count == 6
    assert scans >= 1
    assert scans < checks


async def test_a_replacement_worker_takes_over_when_the_lease_holder_dies(
    docket: Docket, the_task: AsyncMock, now: Callable[[], datetime]
):
    """A lease another worker holds only delays a scan, never blocks it.

    Another worker holds the scheduler lease when this worker starts, so its
    first ticks lose the lease and skip the scan.  The lease expires after one
    scheduling resolution, well before the test times out, so this worker then
    wins the lease, scans the queue, and runs the scheduled task.  The lease
    paces the scan; it never drops a due task.
    """
    when = now() + timedelta(milliseconds=10)
    await docket.add(the_task, when, key="scheduled")()

    async with docket.redis() as redis:
        await redis.set(docket.scheduler_lease_key, "other-worker", nx=True, px=100)

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=50),
    ) as worker:
        await worker.run_until_finished()

    the_task.assert_awaited_once_with()


@pytest.mark.parametrize(
    "resolution, expected_ttl_ms",
    [
        (timedelta(milliseconds=40), 40),
        (timedelta(minutes=5), 1000),
    ],
)
async def test_the_lease_follows_the_resolution_up_to_one_second(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
    resolution: timedelta,
    expected_ttl_ms: int,
):
    """The lease lasts one scheduling resolution, but never over a second.

    Without the cap, a worker configured with a long resolution could win the
    lease and pause the rest of the fleet's scanning for that long.
    """
    ttls: list[int] = []
    real_take_lease = worker_module.take_lease

    async def recording_take_lease(*args: object, **kwargs: object) -> bool:
        ttls.append(args[3])  # type: ignore[arg-type]
        return await real_take_lease(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(worker_module, "take_lease", recording_take_lease)

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=resolution,
    ) as worker:
        await worker.run_until_finished()

    assert set(ttls) == {expected_ttl_ms}


async def test_the_scheduler_lease_expires_within_one_resolution(docket: Docket):
    """The lease a worker takes expires within one scheduling resolution.

    A short lease lets a live holder win again on its own next tick, and lets a
    replacement start scanning within about one resolution after a holder dies.
    """
    resolution = timedelta(milliseconds=40)
    ttl_ms = int(resolution.total_seconds() * 1000)
    worker = Worker(docket, scheduling_resolution=resolution)

    async with docket.redis() as redis:
        won = await worker_module.take_lease(
            redis, docket.scheduler_lease_key, worker.name, ttl_ms
        )
        assert won is True

        deadline = datetime.now(timezone.utc) + timedelta(seconds=2)
        while (
            await worker_module.take_lease(
                redis, docket.scheduler_lease_key, "replacement", ttl_ms
            )
            is False
        ):
            assert datetime.now(timezone.utc) < deadline, "lease never expired"
            await asyncio.sleep(0.005)
