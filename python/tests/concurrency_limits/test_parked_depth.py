"""Tests for the count of tasks parked behind concurrency limits, which the
worker's heartbeat reports as ``docket_parked_depth``.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from opentelemetry.metrics import _Gauge as Gauge

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies._concurrency import (
    _scavenge_and_wake,  # pyright: ignore[reportPrivateUsage]
    parked_depth,
)

from tests.concurrency_limits.waiters import wait_for_xlen
from tests.conftest import wait_until


async def depth(docket: Docket) -> int:
    async with docket.redis() as redis:
        return await parked_depth(docket, redis)


async def indexed(docket: Docket) -> set[bytes]:
    async with docket.redis() as redis:
        return await redis.smembers(docket.waiter_index_key)


async def test_depth_follows_the_backlog_until_it_drains(
    docket: Docket, worker: Worker
):
    """The depth rises with each park, falls with each wake, and is zero
    with no index left behind once the backlog drains."""
    entered = [asyncio.Event() for _ in range(4)]
    release = [asyncio.Event() for _ in range(4)]

    async def task(
        customer_id: int,
        task_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        entered[task_id].set()
        await asyncio.wait_for(release[task_id].wait(), timeout=10)

    assert await depth(docket) == 0

    await docket.add(task)(customer_id=1, task_id=0)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(entered[0].wait(), timeout=5)
    assert await depth(docket) == 0

    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    for task_id in (1, 2, 3):
        await docket.add(task)(customer_id=1, task_id=task_id)
        await wait_for_xlen(docket, waiters_stream, task_id)
        assert await depth(docket) == task_id

    for task_id, remaining in ((0, 2), (1, 1), (2, 0)):
        release[task_id].set()
        await asyncio.wait_for(entered[task_id + 1].wait(), timeout=5)
        assert await depth(docket) == remaining

    assert await indexed(docket) == set()

    release[3].set()
    await asyncio.wait_for(worker_task, timeout=5)
    assert await depth(docket) == 0


async def test_depth_sums_every_waiter_stream(docket: Docket, worker: Worker):
    """Each concurrency key has its own waiter stream, and the depth counts
    the tasks on all of them."""
    entered = {1: asyncio.Event(), 2: asyncio.Event()}
    release = asyncio.Event()

    async def task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        entered[customer_id].set()
        await asyncio.wait_for(release.wait(), timeout=10)

    await docket.add(task)(customer_id=1)
    await docket.add(task)(customer_id=2)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(entered[1].wait(), timeout=5)
    await asyncio.wait_for(entered[2].wait(), timeout=5)

    streams = {
        customer_id: f"{docket.prefix}:concurrency:customer_id:{customer_id}:waiters"
        for customer_id in (1, 2)
    }
    await docket.add(task)(customer_id=1)
    await docket.add(task)(customer_id=1)
    await docket.add(task)(customer_id=2)
    await wait_for_xlen(docket, streams[1], 2)
    await wait_for_xlen(docket, streams[2], 1)

    assert await depth(docket) == 3
    assert await indexed(docket) == {stream.encode() for stream in streams.values()}

    release.set()
    await asyncio.wait_for(worker_task, timeout=5)
    assert await depth(docket) == 0
    assert await indexed(docket) == set()


async def test_depth_falls_when_a_parked_task_is_cancelled(
    docket: Docket, worker: Worker
):
    """Cancelling a parked task takes it out of the depth while the holder
    still has the slot, so no release could have removed it."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=10)

    await docket.add(holder)(customer_id=1)
    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(entered.wait(), timeout=5)

    waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
    kept = await docket.add(holder)(customer_id=1)
    await wait_for_xlen(docket, waiters_stream, 1)
    cancelled = await docket.add(holder)(customer_id=1)
    await wait_for_xlen(docket, waiters_stream, 2)
    assert await depth(docket) == 2

    await docket.cancel(cancelled.key)
    await wait_for_xlen(docket, waiters_stream, 1)
    assert await depth(docket) == 1
    assert await indexed(docket) == {waiters_stream.encode()}

    await docket.cancel(kept.key)
    await wait_for_xlen(docket, waiters_stream, 0)
    assert await depth(docket) == 0
    assert await indexed(docket) == set()

    release.set()
    await asyncio.wait_for(worker_task, timeout=5)


async def test_depth_falls_when_a_dead_holders_slot_is_scavenged(docket: Docket):
    """A task parked behind a holder that died counts until the safeguard
    scavenges the slot and wakes it."""
    ran = asyncio.Event()

    async def waiter(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        ran.set()

    slots_key = f"{docket.prefix}:concurrency:customer_id:1"
    waiters_stream = f"{slots_key}:waiters"

    # No running task holds this slot, so nothing renews it, and the test
    # decides when it goes stale.
    async with docket.redis() as redis:
        fresh = datetime.now(timezone.utc).timestamp()
        await redis.zadd(slots_key, {"dead-holder": fresh})

    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        await docket.add(waiter)(customer_id=1)
        runner = asyncio.create_task(worker.run_forever())

        await wait_for_xlen(docket, waiters_stream, 1)
        assert await depth(docket) == 1

        async with docket.redis() as redis:
            await redis.zadd(slots_key, {"dead-holder": 0.0})

        await asyncio.wait_for(ran.wait(), timeout=5)
        assert await depth(docket) == 0
        assert await indexed(docket) == set()

        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_depth_forgets_a_stream_that_another_release_deleted(docket: Docket):
    """A worker on a release without the index deletes a drained waiter
    stream and leaves its name in the index.  The count drops the name."""
    async with docket.redis() as redis:
        await redis.sadd(
            docket.waiter_index_key, f"{docket.prefix}:concurrency:gone:waiters"
        )

    assert await depth(docket) == 0
    assert await indexed(docket) == set()


async def test_wake_script_indexes_a_stream_that_another_release_parked_on(
    docket: Docket,
):
    """A worker on a release without the index parks a task without adding
    its stream to the index.  The next wake script that finds the stream
    with a task on it adds the stream, so the task is counted."""
    slots_key = f"{docket.prefix}:concurrency:mixed"
    waiters_stream = f"{slots_key}:waiters"

    async with docket.redis() as redis:
        now = datetime.now(timezone.utc).timestamp()
        await redis.zadd(slots_key, {"holder": now})
        await redis.xadd(waiters_stream, {b"key": b"parked-by-another-release"})
        assert await parked_depth(docket, redis) == 0

        woken = await _scavenge_and_wake(
            redis,
            slots_key=slots_key,
            waiters_stream=waiters_stream,
            stream_key=docket.stream_key,
            queue_key=docket.queue_key,
            waiter_index=docket.waiter_index_key,
            max_concurrent=1,
            stale_threshold=now - 60,
            runs_prefix=f"{docket.prefix}:runs:",
            state_prefix=f"{docket.prefix}:state:",
            parked_prefix=f"{docket.prefix}:",
        )
        assert woken == 0
        assert await parked_depth(docket, redis) == 1

        await redis.delete(slots_key, waiters_stream, docket.waiter_index_key)


@pytest.fixture
def PARKED_DEPTH(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_obj = Mock(spec=Gauge.set)
    monkeypatch.setattr("docket.instrumentation.PARKED_DEPTH.set", mock_obj)
    return mock_obj


async def test_heartbeat_reports_the_parked_depth(docket: Docket, PARKED_DEPTH: Mock):
    """The worker's heartbeat sets the gauge to the count of parked tasks,
    which the queue depth leaves out."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=10)

    labels = {"docket.name": docket.name}
    docket.heartbeat_interval = timedelta(milliseconds=50)
    async with Worker(docket) as worker:
        for _ in range(3):
            await docket.add(holder)(customer_id=1)
        worker_task = asyncio.create_task(worker.run_until_finished())
        await asyncio.wait_for(entered.wait(), timeout=5)

        waiters_stream = f"{docket.prefix}:concurrency:customer_id:1:waiters"
        await wait_for_xlen(docket, waiters_stream, 2)
        await wait_until(
            lambda: PARKED_DEPTH.call_args is not None
            and PARKED_DEPTH.call_args.args == (2, labels),
            description="the heartbeat to report two parked tasks",
        )

        release.set()
        await asyncio.wait_for(worker_task, timeout=5)
