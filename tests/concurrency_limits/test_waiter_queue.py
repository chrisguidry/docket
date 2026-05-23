"""Tests for the concurrency-limit waiter stream (admission-blocked tasks are
re-XADD'd into a per-concurrency-key waiter stream and woken when capacity
frees up, rather than polling via the future queue).
"""

import asyncio
import time
from datetime import datetime, timezone

from docket import (
    ConcurrencyLimit,
    Docket,
    Worker,
)


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
        entries = await redis.xrange(waiters_stream, "-", "+")
        assert len(entries) == 1
        fields = entries[0][1]
        assert fields[b"function"] == b"holder"
        # The waiter task itself is not in the future queue.  The only
        # queue entries are the safeguard backstops scheduled by the
        # ConcurrencyLimit dependency (`__safeguard__:<task_key>`).
        queue_keys = [k.decode() for k in await redis.zrange(docket.queue_key, 0, -1)]
        assert all(k.startswith("__safeguard__:") for k in queue_keys), queue_keys

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
            gen: bytes | None = await redis.hget(runs_key, "generation")
        generations_seen.append(int(gen) if gen else -1)

    # Add the slot holder first and wait until it's actually inside the
    # function body before adding ``blocked``.  Otherwise the worker can
    # pick up both messages in one read and ``blocked`` may win the race
    # for the single slot -- running directly without ever parking.
    await docket.add(watcher)(customer_id=1)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await holder_entered.wait()

    blocked_exec = await docket.add(blocked)(customer_id=1)
    blocked_key = blocked_exec.key

    # Wait for the blocked task to actually park on the waiter stream
    # before releasing the holder.
    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    await _wait_for_xlen(docket, waiters_stream, 1)

    release_holder.set()
    await worker_task

    # add=1, park=2, wake=3.  Polling reschedule would bump further.
    assert generations_seen == [3], generations_seen


async def test_safeguard_wakes_waiter_when_holder_died_without_releasing(
    docket: Docket,
):
    """The degenerate case: a concurrency slot is held by a worker that
    crashed without releasing, AND no new tasks are arriving to trigger
    the normal acquire-path scavenge.  The per-park safeguard task that
    ConcurrencyLimit schedules at park-time must fire, scavenge the stale
    slot, and wake the parked waiter.

    A real running holder won't work here -- its lease-renewal loop would
    overwrite any stale timestamp we forge.  Instead we seed the slot with
    a fake holder that has a fresh timestamp so the waiter parks legitimately,
    then later flip the timestamp to 0 (no live renewer to overwrite).
    """
    from datetime import timedelta

    ran_event = asyncio.Event()

    async def waiter(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        ran_event.set()

    slots_key = f"{docket.prefix}:concurrency:customer_id:1"
    waiters_stream = f"{slots_key}:waiters"

    # Pre-occupy the slot with a fresh timestamp so the acquire-path's
    # stale-scavenge can't see it as recoverable.
    async with docket.redis() as redis:
        fresh = datetime.now(timezone.utc).timestamp()
        await redis.zadd(slots_key, {"fake-dead-holder": fresh})

    # Short redelivery_timeout so the per-park safeguard fires inside the
    # test window without slowing the suite down.
    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        await docket.add(waiter)(customer_id=1)

        runner = asyncio.create_task(worker.run_forever())

        # Waiter parks because the (live-looking) fake slot is full.
        await _wait_for_xlen(docket, waiters_stream, 1)

        # Now forge a stale-holder state.  No lease renewer is touching
        # this slot, so the timestamp stays at 0 until the safeguard
        # scavenges it.
        async with docket.redis() as redis:
            await redis.zadd(slots_key, {"fake-dead-holder": 0.0})

        # The safeguard fires ~200ms after the park, scavenges the stale
        # slot, and the waiter is forwarded back to the main stream and
        # runs.
        await asyncio.wait_for(ran_event.wait(), timeout=5.0)

        runner.cancel()
        # Drain the cancelled run_forever() task; gather absorbs the
        # CancelledError so we don't leave it as an "unawaited" warning.
        await asyncio.gather(runner, return_exceptions=True)

    async with docket.redis() as redis:
        assert await redis.xlen(waiters_stream) == 0


async def test_cancel_of_parked_task_prevents_wake_and_run(
    docket: Docket, worker: Worker
):
    """Cancelling a concurrency-blocked task should remove it from the waiter
    stream so the next slot release does not revive and run it.  Regression
    guard: the previous queue-based reschedule path removed cancelled tasks
    from the queue naturally; the stream-based park path needs an explicit
    XDEL inside the cancel script."""
    blocked_ran = False
    holder_entered = asyncio.Event()
    release_holder = asyncio.Event()

    async def holder(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        holder_entered.set()
        await release_holder.wait()

    async def blocked(  # pragma: no cover -- test asserts this never executes
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal blocked_ran
        blocked_ran = True

    # Add the slot holder first and wait until it's actually inside the
    # function body before adding ``blocked``.  Otherwise the worker can
    # pick up both messages in one read and ``blocked`` may win the race
    # for the single slot -- running directly without ever parking.
    await docket.add(holder)(customer_id=1)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await holder_entered.wait()

    blocked_exec = await docket.add(blocked)(customer_id=1)

    # Make sure `blocked` is actually parked on the waiter stream
    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    await _wait_for_xlen(docket, waiters_stream, 1)

    # Cancel the parked task; release the holder; verify blocked never ran
    await docket.cancel(blocked_exec.key)
    release_holder.set()
    await worker_task

    assert not blocked_ran, "cancelled parked task was revived and ran"

    async with docket.redis() as redis:
        runs_key = f"{docket.prefix}:runs:{blocked_exec.key}"
        state = await redis.hget(runs_key, "state")
        assert state == b"cancelled", state
        # Waiter stream should be fully drained
        assert await redis.xlen(waiters_stream) == 0
        # The safeguard task this waiter scheduled at park time should also
        # be cleaned up so it doesn't sit in the future queue.
        safeguard_key = f"__safeguard__:{blocked_exec.key}"
        queue_keys = [k.decode() for k in await redis.zrange(docket.queue_key, 0, -1)]
        assert safeguard_key not in queue_keys, queue_keys


async def test_cancel_cleanup_script_drains_waiter_entry(docket: Docket):
    """Directly exercise the ``_cancel_cleanup`` wrapper.

    The wrapper is normally invoked from
    ``ConcurrencyLimit._cleanup_cancelled_waiter``, which the
    ``memory://`` backend can't actually drive end-to-end (its in-process
    Redis shim is missing ``hmget``).  Drive the script directly here to
    keep the optimisation honestly tested on every backend.
    """
    from docket.dependencies._concurrency import _cancel_cleanup  # pyright: ignore[reportPrivateUsage]

    waiter_stream = f"{docket.prefix}:concurrency:cleanup-direct:waiters"
    task_key = "cleanup-direct"
    runs_key = f"{docket.prefix}:runs:{task_key}"
    progress_key = f"{docket.prefix}:progress:{task_key}"

    async with docket.redis() as redis:
        entry_id = await redis.xadd(waiter_stream, {b"key": task_key.encode()})
        await redis.hset(runs_key, "waiter_stream", waiter_stream)
        await redis.hset(runs_key, "waiter_entry_id", entry_id.decode())
        await redis.hset(progress_key, "current", "0")

        assert await redis.xlen(waiter_stream) == 1
        assert await redis.exists(progress_key) == 1

        await _cancel_cleanup(
            redis,
            waiters_stream=waiter_stream,
            progress_key=progress_key,
            runs_key=runs_key,
            waiter_entry_id=entry_id.decode(),
        )

        assert await redis.xlen(waiter_stream) == 0
        assert await redis.exists(progress_key) == 0
        assert await redis.hget(runs_key, "waiter_stream") is None
        assert await redis.hget(runs_key, "waiter_entry_id") is None


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
