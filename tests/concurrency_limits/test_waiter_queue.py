"""Tests for the concurrency-limit waiter stream (admission-blocked tasks are
re-XADD'd into a per-concurrency-key waiter stream and woken when capacity
frees up, rather than polling via the future queue).
"""

import asyncio
import time

from docket import (
    ConcurrencyLimit,
    Docket,
    Worker,
)


async def _wait_for_xlen(docket: Docket, key: str, target: int) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        async with docket.redis() as redis:
            size = await redis.xlen(key)  # type: ignore
        if size == target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(  # pragma: no cover
        f"XLEN({key}) did not reach {target} in time"
    )


async def test_blocked_task_parks_on_waiter_stream(docket: Docket, worker: Worker):
    """A task that can't acquire a slot is XADD'd onto the waiter stream with
    its full message payload -- no separate parked hash, no entry in the
    scheduler's future queue.
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

    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    await _wait_for_xlen(docket, waiters_stream, 1)

    async with docket.redis() as redis:
        entries = await redis.xrange(waiters_stream, "-", "+")  # type: ignore
        assert len(entries) == 1
        fields = entries[0][1]
        assert fields[b"function"] == b"holder"
        # Not sitting in the future queue
        assert await redis.zcard(docket.queue_key) == 0  # type: ignore

    hold.set()
    await worker_task


async def test_release_wakes_oldest_waiter_first(docket: Docket, worker: Worker):
    """Waiters are woken in FIFO order -- stream XADD IDs are monotonic."""
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

    # Enqueue waiters one at a time so their park order is strict
    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    for tid in (1, 2, 3):
        await docket.add(task)(customer_id=1, task_id=tid)
        await _wait_for_xlen(docket, waiters_stream, tid)

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
    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    await _wait_for_xlen(docket, waiters_stream, 1)

    release_holder.set()
    await worker_task

    # add=1, park=2, wake=3.  Polling reschedule would bump further.
    assert generations_seen == [3], generations_seen


async def test_many_contending_tasks_all_run_exactly_once(docket: Docket):
    """Stress test: N tasks contending for 1 slot all eventually run, each
    exactly once, without duplicates or drops.  The structural correctness
    of the wake-on-release loop matters more than wall-clock timing (which
    varies wildly on CI runners)."""
    ran: list[int] = []

    async def serial_task(
        customer_id: int,
        task_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        ran.append(task_id)
        await asyncio.sleep(0.01)

    n_tasks = 8
    for i in range(n_tasks):
        await docket.add(serial_task)(customer_id=1, task_id=i)

    async with Worker(docket, concurrency=4) as worker:
        await worker.run_until_finished()

    assert sorted(ran) == list(range(n_tasks)), ran
