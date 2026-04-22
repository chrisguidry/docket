"""Tests for the concurrency-limit waiter queue (admission-blocked tasks park
in a Redis sorted set and are woken when capacity frees up, rather than
polling via the future queue).
"""

import asyncio
import time

from docket import (
    ConcurrencyLimit,
    Docket,
    Worker,
)


async def _wait_for_zcard(docket: Docket, key: str, target: int) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        async with docket.redis() as redis:
            size = await redis.zcard(key)  # type: ignore
        if size == target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"ZCARD({key}) did not reach {target} in time")  # pragma: no cover


async def test_blocked_task_parks_in_waiter_zset(docket: Docket, worker: Worker):
    """A task that can't acquire a slot lands in the waiter sorted set and its
    payload is stored in the parked hash -- not in the scheduler's future queue.
    """
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
    await docket.add(holder)(customer_id=1)

    worker_task = asyncio.create_task(worker.run_until_finished())
    await started.wait()

    waiters_key = f"{docket.name}:concurrency:customer_id:1:waiters"
    await _wait_for_zcard(docket, waiters_key, 1)

    async with docket.redis() as redis:
        waiters = await redis.zrange(waiters_key, 0, -1)  # type: ignore
        assert len(waiters) == 1
        parked_key = docket.parked_task_key(waiters[0].decode())
        parked = await redis.hgetall(parked_key)  # type: ignore
        assert parked[b"function"] == b"holder"
        # Not sitting in the future queue
        assert await redis.zcard(docket.queue_key) == 0  # type: ignore

    hold.set()
    await worker_task


async def test_release_wakes_oldest_waiter_first(docket: Docket, worker: Worker):
    """Waiters are woken in FIFO order based on the time they were parked."""
    order: list[int] = []
    holder_entered = asyncio.Event()
    release_holder = asyncio.Event()

    async def task(
        customer_id: int,
        task_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        if task_id == 0:
            holder_entered.set()
            await release_holder.wait()
        order.append(task_id)

    # Seed the slot holder first
    await docket.add(task)(customer_id=1, task_id=0)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await holder_entered.wait()

    # Now enqueue waiters one at a time so their park timestamps strictly order
    waiters_key = f"{docket.name}:concurrency:customer_id:1:waiters"
    for tid in (1, 2, 3):
        await docket.add(task)(customer_id=1, task_id=tid)
        await _wait_for_zcard(docket, waiters_key, tid)

    release_holder.set()
    await worker_task

    assert order == [0, 1, 2, 3]


async def test_blocked_task_makes_no_polling_retries(docket: Docket, worker: Worker):
    """The blocked task should park exactly once and run exactly once -- no
    polling loop through the scheduler queue.  We assert this by confirming
    its generation counter reaches exactly the value produced by a single
    park + wake cycle (add=1, park=2, wake=3)."""
    holder_entered = asyncio.Event()
    release_holder = asyncio.Event()
    generations_seen: list[int] = []

    async def watcher(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        holder_entered.set()
        await release_holder.wait()

    async def blocked(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        runs_key = f"{docket.prefix}:runs:{blocked_key}"
        async with docket.redis() as redis:
            gen: bytes | None = await redis.hget(runs_key, "generation")  # type: ignore[assignment]
        generations_seen.append(int(gen) if gen else -1)

    await docket.add(watcher)(customer_id=1)
    blocked_exec = await docket.add(blocked)(customer_id=1)
    blocked_key = blocked_exec.key

    worker_task = asyncio.create_task(worker.run_until_finished())
    await holder_entered.wait()

    # Wait for the blocked task to actually park -- otherwise release_holder
    # can fire before blocked reaches the concurrency gate, making it acquire
    # directly without parking (which would defeat the test's purpose).
    waiters_key = f"{docket.name}:concurrency:customer_id:1:waiters"
    await _wait_for_zcard(docket, waiters_key, 1)

    release_holder.set()
    await worker_task

    # add=1, park=2, wake=3.  Polling reschedule would bump further.
    assert generations_seen == [3], generations_seen


async def test_many_contending_tasks_drain_without_gaps(docket: Docket):
    """With N tasks contending for 1 slot, each release should wake the next
    waiter with latency bounded by the worker's stream-poll interval -- not
    the old 100ms ADMISSION_BLOCKED_RETRY_DELAY.
    """
    from datetime import timedelta

    starts: list[float] = []
    ends: list[float] = []

    async def serial_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        starts.append(time.monotonic())
        await asyncio.sleep(0.02)
        ends.append(time.monotonic())

    for _ in range(6):
        await docket.add(serial_task)(customer_id=1)

    async with Worker(
        docket,
        concurrency=4,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert len(starts) == 6
    starts.sort()
    ends.sort()

    # Each task after the first should start very soon after the previous ends.
    # With wake-on-release the gap is a Redis round-trip + a single stream-poll
    # cycle.  Under the old polling reschedule path the gap was bounded below
    # by ADMISSION_BLOCKED_RETRY_DELAY (100ms).
    for i in range(1, 6):
        gap = starts[i] - ends[i - 1]
        assert gap < 0.08, (
            f"Gap {gap:.3f}s between task {i - 1} end and task {i} start is too large"
        )
