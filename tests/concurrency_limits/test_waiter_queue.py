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


async def test_sweep_wakes_waiters_when_holder_died_without_releasing(
    docket: Docket,
):
    """The degenerate case: a concurrency slot is held by a worker that
    crashed without releasing, AND no new tasks are arriving to trigger
    the normal acquire-path scavenge.  The periodic sweep loop should
    detect the stale slot, free the capacity, and wake the parked waiter.

    We seed Redis directly (rather than orchestrating a crash) because
    the worker's own shutdown path would fire the release callback and
    drain waiters legitimately, defeating the whole point of the test.
    """
    from datetime import timedelta

    ran: list[int] = []

    async def task(
        customer_id: int,
        task_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        ran.append(task_id)

    # Use docket.add to register the task and build a valid message payload,
    # then manually move it off the main stream and onto a waiter stream.
    execution = await docket.add(task)(customer_id=1, task_id=42)
    slots_key = f"{docket.prefix}:concurrency:customer_id:1"
    waiters_stream = f"{slots_key}:waiters"

    async with docket.redis() as redis:
        # Pull the freshly-scheduled message out of the main stream.
        entries = await redis.xrange(docket.stream_key, "-", "+")  # type: ignore
        message_id, fields = entries[0]
        await redis.xdel(docket.stream_key, message_id)  # type: ignore

        # Re-park it on the waiter stream ourselves and wire up the state
        # that the acquire-or-park script would normally produce.
        await redis.xadd(waiters_stream, dict(fields))  # type: ignore
        await redis.hset(  # type: ignore
            docket.concurrency_waiter_registry_key,
            waiters_stream,
            "1",  # max_concurrent
        )
        await redis.hset(  # type: ignore
            f"{docket.prefix}:runs:{execution.key}",
            mapping={
                "state": "scheduled",
                "waiter_stream": waiters_stream,
            },
        )
        # Seed a stale slot so the sweep thinks a dead holder is blocking
        # the waiter.  Score of 0 is well below any live stale_threshold.
        await redis.zadd(slots_key, {"dead-holder": 0.0})  # type: ignore

        # Sanity: we really are in the "parked but nothing alive to wake us" state
        assert await redis.xlen(waiters_stream) == 1  # type: ignore
        assert await redis.xlen(docket.stream_key) == 0  # type: ignore
        assert await redis.zcard(slots_key) == 1  # type: ignore

    # Start a fresh worker with a short redelivery_timeout so the sweep
    # loop fires within the test window.  No new tasks are added.  The
    # worker's _concurrency_sweep_loop must scavenge the stale slot,
    # re-XADD the parked task into the main stream, and run it.
    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=5.0)

    assert ran == [42], ran

    # Registry and waiter stream should be drained by the sweep's cleanup
    async with docket.redis() as redis:
        assert await redis.hlen(docket.concurrency_waiter_registry_key) == 0  # type: ignore
        assert await redis.xlen(waiters_stream) == 0  # type: ignore


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
