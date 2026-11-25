"""Tests for the count of tasks parked behind concurrency limits, which the
worker's heartbeat reports as ``docket_parked_depth``.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from opentelemetry.metrics import _Gauge as Gauge
from redis.exceptions import RedisError

from docket import ConcurrencyLimit, Docket, Worker
from docket._redis import RedisClient
from docket.dependencies._concurrency import (
    PARKED_DEPTH_BATCH,
    _scavenge_and_wake,  # pyright: ignore[reportPrivateUsage]
    parked_depth,
)

from tests.concurrency_limits.waiters import wait_for_xlen
from tests.conftest import wait_until


async def depth(docket: Docket, cursor: int = 0) -> int:
    async with docket.redis() as redis:
        return await parked_depth(docket, redis, cursor)


async def indexed(docket: Docket) -> dict[bytes, float]:
    """The count that the docket's index holds for each waiter stream."""
    async with docket.redis() as redis:
        return dict(await redis.zrange(docket.waiter_index_key, 0, -1, withscores=True))


async def scavenge(docket: Docket, slots_key: str) -> int:
    """Run the safeguard's script, as a worker on this release does."""
    async with docket.redis() as redis:
        return await _scavenge_and_wake(
            redis,
            slots_key=slots_key,
            waiters_stream=f"{slots_key}:waiters",
            stream_key=docket.stream_key,
            queue_key=docket.queue_key,
            waiter_index=docket.waiter_index_key,
            parked_total=docket.parked_total_key,
            max_concurrent=1,
            stale_threshold=datetime.now(timezone.utc).timestamp() - 60,
            runs_prefix=f"{docket.prefix}:runs:",
            state_prefix=f"{docket.prefix}:state:",
            parked_prefix=f"{docket.prefix}:",
        )


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

    assert await indexed(docket) == {}

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
    assert await indexed(docket) == {
        streams[1].encode(): 2,
        streams[2].encode(): 1,
    }

    release.set()
    await asyncio.wait_for(worker_task, timeout=5)
    assert await depth(docket) == 0
    assert await indexed(docket) == {}


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
    assert await indexed(docket) == {waiters_stream.encode(): 1}

    await docket.cancel(kept.key)
    await wait_for_xlen(docket, waiters_stream, 0)
    assert await depth(docket) == 0
    assert await indexed(docket) == {}

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

    # No running task holds this slot, so nothing renews it.  Its time is in
    # the future, so it cannot go stale on a slow machine before the test
    # makes it stale.
    async with docket.redis() as redis:
        fresh = datetime.now(timezone.utc).timestamp() + 3600
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
        assert await indexed(docket) == {}

        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_script_corrects_the_count_that_another_release_left(docket: Docket):
    """A worker on a release that keeps no count wakes the tasks on a stream
    and deletes it, and the count for the stream stays as it was.  The next
    script on the stream corrects the count and the total."""
    slots_key = f"{docket.prefix}:concurrency:mixed"

    async with docket.redis() as redis:
        await redis.zadd(docket.waiter_index_key, {f"{slots_key}:waiters": 3})
        await redis.set(docket.parked_total_key, "3")

    assert await scavenge(docket, slots_key) == 0

    assert await indexed(docket) == {}
    assert await depth(docket) == 0


async def test_an_empty_index_means_no_parked_tasks(docket: Docket):
    """The total is the sum of the index's counts, so a total left behind
    without any index entries is cleared instead of reported."""
    async with docket.redis() as redis:
        await redis.set(docket.parked_total_key, "5")

    assert await depth(docket) == 0

    async with docket.redis() as redis:
        assert await redis.exists(docket.parked_total_key) == 0


async def test_reads_correct_the_counts_that_another_release_left(docket: Docket):
    """When no script runs on a stream again, the heartbeat's reads correct
    its count.  Each read checks one batch of streams, and reads with an
    advancing cursor reach all of them."""
    gone = PARKED_DEPTH_BATCH + 4
    kept = f"{docket.prefix}:concurrency:kept:waiters"

    async with docket.redis() as redis:
        await redis.xadd(kept, {b"key": b"parked"})
        await redis.zadd(docket.waiter_index_key, {kept: 1})
        for number in range(gone):
            stream = f"{docket.prefix}:concurrency:gone-{number}:waiters"
            await redis.zadd(docket.waiter_index_key, {stream: 2})
        await redis.set(docket.parked_total_key, str(1 + 2 * gone))

    first = await depth(docket, cursor=0)
    assert 1 < first < 1 + 2 * gone
    assert await depth(docket, cursor=PARKED_DEPTH_BATCH) == 1
    assert await indexed(docket) == {kept.encode(): 1}

    async with docket.redis() as redis:
        await redis.delete(kept, docket.waiter_index_key, docket.parked_total_key)


async def test_script_counts_the_task_that_another_release_parked(docket: Docket):
    """A worker on a release that keeps no count parks a task without
    counting it.  The next script on the stream counts it."""
    slots_key = f"{docket.prefix}:concurrency:mixed"
    waiters_stream = f"{slots_key}:waiters"

    async with docket.redis() as redis:
        now = datetime.now(timezone.utc).timestamp()
        await redis.zadd(slots_key, {"holder": now})
        await redis.xadd(waiters_stream, {b"key": b"parked-by-another-release"})
    assert await depth(docket) == 0

    assert await scavenge(docket, slots_key) == 0

    assert await depth(docket) == 1
    async with docket.redis() as redis:
        await redis.delete(
            slots_key,
            waiters_stream,
            docket.waiter_index_key,
            docket.parked_total_key,
        )


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


@pytest.fixture
def QUEUE_DEPTH(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_obj = Mock(spec=Gauge.set)
    monkeypatch.setattr("docket.instrumentation.QUEUE_DEPTH.set", mock_obj)
    return mock_obj


@pytest.fixture
def SCHEDULE_DEPTH(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_obj = Mock(spec=Gauge.set)
    monkeypatch.setattr("docket.instrumentation.SCHEDULE_DEPTH.set", mock_obj)
    return mock_obj


@pytest.fixture
def unreadable_parked_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    async def read(docket: Docket, redis: RedisClient, cursor: int) -> int:
        raise RedisError("the parked depth cannot be read")

    monkeypatch.setattr("docket.worker.parked_depth", read)


async def test_a_failed_parked_depth_read_leaves_the_other_depths_reported(
    docket: Docket,
    unreadable_parked_depth: None,
    QUEUE_DEPTH: Mock,
    SCHEDULE_DEPTH: Mock,
):
    """The queue and schedule depths are set before the parked depth is
    read, so a heartbeat whose read always fails still reports them."""
    labels = {"docket.name": docket.name}
    docket.heartbeat_interval = timedelta(milliseconds=50)
    async with Worker(docket) as worker:
        runner = asyncio.create_task(worker.run_forever())
        await wait_until(
            lambda: QUEUE_DEPTH.call_count >= 1 and SCHEDULE_DEPTH.call_count >= 1,
            description="a heartbeat to report the queue and schedule depths",
        )
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)

    QUEUE_DEPTH.assert_any_call(0, labels)
    SCHEDULE_DEPTH.assert_any_call(0, labels)


@pytest.fixture
def failing_first_read(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The cursor of each heartbeat's parked-depth read.  The first read
    fails and the later ones succeed."""
    cursors: list[int] = []

    async def read(docket: Docket, redis: RedisClient, cursor: int) -> int:
        cursors.append(cursor)
        if len(cursors) == 1:
            raise RedisError("the parked depth cannot be read")
        return await parked_depth(docket, redis, cursor)

    monkeypatch.setattr("docket.worker.parked_depth", read)
    return cursors


async def test_the_heartbeat_moves_on_from_a_batch_that_fails_to_read(
    docket: Docket, failing_first_read: list[int]
):
    """A batch whose read fails is not retried on the next beat, which would
    leave every later batch unread while the failure lasted."""
    docket.heartbeat_interval = timedelta(milliseconds=50)
    async with Worker(docket) as worker:
        runner = asyncio.create_task(worker.run_forever())
        await wait_until(
            lambda: len(failing_first_read) >= 2,
            description="a second heartbeat to read the parked depth",
        )
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)

    assert failing_first_read[:2] == [0, PARKED_DEPTH_BATCH]
