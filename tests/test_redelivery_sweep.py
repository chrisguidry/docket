"""Tests for how a worker takes messages off the stream.

The worker claims abandoned entries with XAUTOCLAIM. It sweeps the consumer
group's pending list in bounded batches on a timer, and it takes a fleet-wide
lease before each sweep, so these tests cover how far a sweep reaches, how
often one starts, and which worker runs it.  They also cover the cap on how
many new messages one XREADGROUP may return.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, AsyncGenerator, Generator
from unittest.mock import AsyncMock, patch

import pytest

from docket import Docket, Worker
from docket._redelivery import DELIVERY_BATCH, SWEEP_START, RedeliverySweep
from docket._redis import RedisClient


@dataclass
class StreamCalls:
    """The stream reads a worker made during a test."""

    claim_starts: list[str] = field(default_factory=list[str])
    claim_consumers: list[str] = field(default_factory=list[str])
    read_counts: list[int] = field(default_factory=list[int])
    reads: int = 0


@pytest.fixture
def stream_calls() -> Generator[StreamCalls, None, None]:
    """Record XAUTOCLAIM and XREADGROUP calls on every backend.

    The recorder wraps the client the worker actually uses, so it works on
    standalone, cluster, and memory alike.
    """
    calls = StreamCalls()

    class Recording:
        def __init__(self, wrapped: Any) -> None:
            self._wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            return getattr(self._wrapped, name)

        async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
            calls.claim_starts.append(str(kwargs["start_id"]))
            calls.claim_consumers.append(str(kwargs["consumername"]))
            return await self._wrapped.xautoclaim(*args, **kwargs)

        async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
            calls.reads += 1
            calls.read_counts.append(int(kwargs["count"]))
            return await self._wrapped.xreadgroup(*args, **kwargs)

    original_redis = Docket.redis

    @asynccontextmanager
    async def recording_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        async with original_redis(self) as client:
            yield Recording(client)  # type: ignore[arg-type]

    with patch.object(Docket, "redis", recording_redis):
        yield calls


async def abandon_every_stream_entry(docket: Docket) -> None:
    """Deliver every stream entry to a worker that never acks it."""
    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
    async with docket.redis() as redis:
        await redis.xreadgroup(
            groupname=docket.worker_group_name,
            consumername="worker-that-died",
            streams={docket.stream_key: ">"},
            count=1000,
        )


async def test_sweep_reaches_entries_past_the_first_batch(
    docket: Docket, stream_calls: StreamCalls
):
    """Every abandoned entry is redelivered, not only the first batch of them.

    One worker slot bounds each claim to a single entry, so the worker only
    reaches the third entry if it resumes the sweep from the cursor of the
    last claim.
    """
    executed: list[int] = []

    async def the_task(number: int) -> None:
        executed.append(number)

    for number in range(3):
        await docket.add(the_task, key=f"task-{number}")(number=number)

    await abandon_every_stream_entry(docket)

    await asyncio.sleep(0.25)  # longer than the redelivery timeout

    async with Worker(
        docket,
        concurrency=1,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert sorted(executed) == [0, 1, 2]

    # A three-entry pending list is small enough that a scan from the
    # beginning also reaches the last entry, so only the start of each claim
    # shows that the sweep resumed instead of starting over.
    assert [start for start in stream_calls.claim_starts if start != "0-0"]


async def test_sweep_rests_between_passes(docket: Docket, stream_calls: StreamCalls):
    """A finished sweep does not start again on the next pass of the loop."""

    async def slow_task() -> None:
        await asyncio.sleep(0.5)

    await docket.add(slow_task)()

    # Create the consumer group up front, so the worker's first claim does not
    # retry past a NOGROUP error and count as a second sweep.
    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

    async with Worker(
        docket,
        redelivery_timeout=timedelta(seconds=30),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert stream_calls.reads > 5, "the loop should have made many passes"
    assert stream_calls.claim_starts == ["0-0"], "one sweep, on the first pass"


def test_the_next_sweep_waits_a_jittered_interval():
    """A finished sweep schedules the next one 75% to 125% of the interval out.

    Twenty workers that start together would sweep the pending list at the
    same moment without the jitter.
    """
    sweep = RedeliverySweep(
        timedelta(seconds=40), lease_key="sweep-lease", worker_name="worker-one"
    )

    with patch("docket._redelivery.random.uniform", return_value=12.5) as uniform:
        sweep.advance(SWEEP_START)

    uniform.assert_called_once_with(7.5, 12.5)
    assert sweep.next_sweep - time.monotonic() == pytest.approx(12.5, abs=0.5)


async def test_only_the_lease_holder_sweeps(docket: Docket, stream_calls: StreamCalls):
    """Two workers in one interval run one sweep between them, not two.

    A sweep costs Redis a pass over the whole pending list, so a fleet that
    swept once per worker would multiply that cost by the size of the fleet.
    """

    async def slow_task() -> None:
        await asyncio.sleep(0.2)

    await docket.add(slow_task)()

    # Create the consumer group up front, so neither worker's first claim
    # retries past a NOGROUP error and counts as a second sweep.
    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

    def replacement(name: str) -> Worker:
        return Worker(
            docket,
            name=name,
            redelivery_timeout=timedelta(seconds=30),
            minimum_check_interval=timedelta(milliseconds=10),
            scheduling_resolution=timedelta(milliseconds=10),
        )

    async with replacement("worker-one") as one, replacement("worker-two") as two:
        await asyncio.gather(one.run_until_finished(), two.run_until_finished())

    assert len(stream_calls.claim_consumers) == 1
    assert stream_calls.claim_consumers[0] in {"worker-one", "worker-two"}


async def test_a_worker_that_loses_the_lease_sweeps_once_it_expires(docket: Docket):
    """A worker that replaces a dead one still picks up its abandoned message.

    The dead worker's lease outlives it.  Nobody deletes that lease, so the
    replacement worker waits for it to expire and then takes it.
    """
    executed: list[str] = []

    async def the_task() -> None:
        executed.append("ran")

    await docket.add(the_task)()
    await abandon_every_stream_entry(docket)

    async with docket.redis() as redis:
        await redis.set(docket.redelivery_sweep_key, "the-dead-worker", px=400)

    await asyncio.sleep(0.25)  # longer than the redelivery timeout

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert executed == ["ran"]


async def test_a_read_asks_for_no_more_than_the_delivery_batch(
    docket: Docket, stream_calls: StreamCalls, the_task: AsyncMock
):
    """A worker with more free slots than the batch still asks for the batch.

    Redis serializes every message it returns before it answers, and it blocks
    while it does that, so one huge read stalls every other client.
    """
    await docket.add(the_task)()

    async with Worker(
        docket,
        concurrency=DELIVERY_BATCH * 2,
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    the_task.assert_called_once()
    assert stream_calls.read_counts
    assert max(stream_calls.read_counts) == DELIVERY_BATCH


async def test_a_capped_read_still_drains_a_larger_backlog(
    docket: Docket, stream_calls: StreamCalls
):
    """A backlog bigger than one batch still drains, one batch per read."""
    executed: list[int] = []

    async def the_task(number: int) -> None:
        executed.append(number)

    for number in range(6):
        await docket.add(the_task, key=f"task-{number}")(number=number)

    with patch("docket.worker.DELIVERY_BATCH", 2):
        async with Worker(
            docket,
            concurrency=10,
            minimum_check_interval=timedelta(milliseconds=10),
            scheduling_resolution=timedelta(milliseconds=10),
        ) as worker:
            await worker.run_until_finished()

    assert sorted(executed) == [0, 1, 2, 3, 4, 5]
    assert max(stream_calls.read_counts) == 2
