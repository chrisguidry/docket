"""Tests for the bounded, cursor-kept redelivery sweep.

The sweep claims its consumer group's pending list with XAUTOCLAIM to find
messages another worker abandoned.  Each claim is bounded, and the cursor is
kept across claims so entries past the first window are still reached.  A
worker sweeps on a jittered timer rather than on every poll pass, and keeps
sweeping until the cursor comes back to the front of the list.  Once its timer
elapses a worker sweeps only if it holds the fleet-wide lease, so the fleet
runs at most one sweep per interval.  A worker that holds the lease keeps it:
it refreshes the lease on every pass of a sweep under way, and it does not lose
the lease to itself when its own timer fires.
"""

import asyncio
import inspect
import time
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Worker
from docket._redelivery import JITTER, SWEEP_START, RedeliverySweep


@pytest.fixture
def the_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    task.return_value = None
    return task


@pytest.fixture
def sweep(docket: Docket) -> RedeliverySweep:
    """A sweep that treats every pending message as abandoned.

    A zero idle time means XAUTOCLAIM claims whatever is pending, so a test
    does not have to wait for messages to go idle.  The timeout still sets a
    workable lease interval.
    """
    sweep = RedeliverySweep(
        docket,
        worker_name="the-sweeping-worker",
        redelivery_timeout=timedelta(seconds=8),
    )
    sweep.min_idle_time = 0
    return sweep


def test_a_fresh_sweep_starts_at_the_front(sweep: RedeliverySweep):
    """A sweep that has claimed nothing yet starts at the front of the list."""
    assert sweep.start_id == SWEEP_START


def a_sweep(
    docket: Docket,
    timeout: timedelta,
    worker_name: str = "the-sweeping-worker",
) -> RedeliverySweep:
    """A sweep whose redelivery timeout sets the interval between its sweeps."""
    return RedeliverySweep(
        docket,
        worker_name=worker_name,
        redelivery_timeout=timeout,
    )


def test_the_interval_is_a_quarter_of_the_timeout(docket: Docket):
    """A worker sweeps four times per redelivery timeout."""
    assert a_sweep(docket, timedelta(seconds=8)).interval == 2.0


async def test_the_first_sweep_is_due_immediately(docket: Docket):
    """A fresh worker sweeps at once, to reclaim a dead worker's messages."""
    sweep = a_sweep(docket, timedelta(seconds=8))

    async with docket.redis() as redis:
        assert await sweep.due(redis) is True


async def test_only_the_lease_holder_sweeps_in_an_interval(docket: Docket):
    """Two workers whose timers are due, but only the lease holder sweeps.

    A sweep costs O(pending list) no matter how many workers run, so only the
    worker that takes the lease sweeps this interval.  The other one skips.
    """
    worker_a = a_sweep(docket, timedelta(seconds=8), worker_name="worker-a")
    worker_b = a_sweep(docket, timedelta(seconds=8), worker_name="worker-b")

    async with docket.redis() as redis:
        assert await worker_a.due(redis) is True
        assert await worker_b.due(redis) is False
        assert await redis.get(docket.redelivery_sweep_key) == b"worker-a"


async def test_a_worker_that_loses_the_lease_arms_its_next_timer(docket: Docket):
    """A worker that loses the lease waits for its own next timer, not a spin.

    Losing the lease arms the jittered timer, so an immediate second check
    returns False rather than hammering Redis with another SET on every pass.
    """
    interval = 8.0
    holder = a_sweep(docket, timedelta(seconds=interval * 4), worker_name="holder")
    loser = a_sweep(docket, timedelta(seconds=interval * 4), worker_name="loser")

    async with docket.redis() as redis:
        assert await holder.due(redis) is True

        before = time.monotonic()
        assert await loser.due(redis) is False
        after = time.monotonic()

        low, high = JITTER
        assert before + low * interval <= loser.next_sweep <= after + high * interval
        assert loser.start_id == SWEEP_START
        # A second immediate check does not spin; the timer holds it off.
        assert await loser.due(redis) is False


async def abandon_many(docket: Docket, the_task: AsyncMock, count: int) -> None:
    """Leave ``count`` messages pending under a dead consumer.

    Immediate adds land on the stream.  Reading them into a consumer that never
    acks them mimics a worker that took the messages and then crashed, so the
    pending list holds every one for a later sweep to reclaim.
    """
    for i in range(count):
        await docket.add(the_task, key=f"abandoned-{i}")()

    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
    async with docket.redis() as redis:
        await redis.xreadgroup(
            groupname=docket.worker_group_name,
            consumername="dead-worker",
            streams={docket.stream_key: ">"},
            count=count,
        )


async def test_one_claim_asks_for_no_more_than_the_scan_batch(
    docket: Docket,
    sweep: RedeliverySweep,
    the_task: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
):
    """A worker with more free slots than one batch still caps its ask.

    Twenty free slots against a scan batch of five claims five, so Redis reads
    a bounded slice of the pending list however many slots are free.
    """
    monkeypatch.setattr("docket._redelivery.REDELIVERY_SCAN_BATCH", 5)

    await abandon_many(docket, the_task, count=20)

    async with docket.redis() as redis:
        claimed = await sweep.claim(redis, available_slots=20)

    assert len(claimed) == 5


async def test_the_kept_cursor_reaches_entries_past_the_first_window(
    docket: Docket,
    sweep: RedeliverySweep,
    the_task: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
):
    """A second claim picks up where the first one stopped.

    A sweep that restarted at the front every time would claim the same five
    entries forever, and the rest of the pending list would never be
    redelivered.
    """
    monkeypatch.setattr("docket._redelivery.REDELIVERY_SCAN_BATCH", 5)

    await abandon_many(docket, the_task, count=20)

    async with docket.redis() as redis:
        first = await sweep.claim(redis, available_slots=5)
        second = await sweep.claim(redis, available_slots=5)

    first_ids = {message_id for message_id, _ in first}
    second_ids = {message_id for message_id, _ in second}
    assert len(first_ids) == 5
    assert len(second_ids) == 5
    assert not first_ids & second_ids


async def test_a_sweep_under_way_stays_due_regardless_of_the_timer(
    docket: Docket,
    sweep: RedeliverySweep,
    the_task: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
):
    """While the cursor is past the front the lease holder sweeps on every pass."""
    monkeypatch.setattr("docket._redelivery.REDELIVERY_SCAN_BATCH", 5)

    await abandon_many(docket, the_task, count=20)

    async with docket.redis() as redis:
        assert await sweep.due(redis) is True
        await sweep.claim(redis, available_slots=5)

        assert sweep.start_id != SWEEP_START
        # Push the timer far into the future; the sweep under way is still due.
        sweep.next_sweep = time.monotonic() + 3600

        assert await sweep.due(redis) is True


async def test_a_sweep_under_way_refreshes_the_lease_on_every_pass(docket: Docket):
    """A sweep that outlasts one interval keeps its lease to the end.

    The lease is taken for one interval, but a walk of a long pending list can
    run longer than that.  Every pass refreshes the lease, so it covers the
    whole sweep and no other worker starts one alongside it.
    """
    interval = 0.25
    sweep = a_sweep(docket, timedelta(seconds=interval * 4))

    async with docket.redis() as redis:
        assert await sweep.due(redis) is True
        sweep.start_id = "1000-0"

        await asyncio.sleep(interval * 0.6)
        assert await sweep.due(redis) is True
        await asyncio.sleep(interval * 0.6)
        assert await sweep.due(redis) is True
        await asyncio.sleep(interval * 0.6)
        assert await sweep.due(redis) is True

        assert await redis.get(docket.redelivery_sweep_key) == b"the-sweeping-worker"


async def test_a_lone_worker_does_not_lose_the_lease_to_itself(docket: Docket):
    """A worker whose own lease is still live sweeps again when its timer fires.

    Nothing else holds the lease, so a plain SET NX would fail and the only
    worker in the fleet would rest for another interval.  Finding its own name
    on the lease, the worker keeps the lease and sweeps.
    """
    sweep = a_sweep(docket, timedelta(seconds=8))

    async with docket.redis() as redis:
        assert await sweep.due(redis) is True

        sweep.next_sweep = time.monotonic()
        assert await sweep.due(redis) is True
        assert await redis.get(docket.redelivery_sweep_key) == b"the-sweeping-worker"


async def test_a_sweep_that_lost_its_lease_keeps_its_place(docket: Docket):
    """A worker that stalls until its lease lapses keeps its place and rests.

    Another worker holds the lease by the time this one comes back, so this
    worker backs off without moving its cursor, so it resumes from where it
    stopped once it takes the lease again, instead of re-reading the front.
    It leaves the other worker's lease exactly as it found it.
    """
    sweep = a_sweep(docket, timedelta(seconds=8))
    sweep.start_id = "1000-0"

    async with docket.redis() as redis:
        await redis.set(docket.redelivery_sweep_key, "another-worker", px=5000)

        before = time.monotonic()
        assert await sweep.due(redis) is False

        assert sweep.start_id == "1000-0"
        assert sweep.next_sweep > before
        assert await redis.get(docket.redelivery_sweep_key) == b"another-worker"
        # Still the five seconds the other worker asked for, not this one's eight.
        assert 0 < await redis.ttl(docket.redelivery_sweep_key) <= 5


async def test_a_sweep_whose_lease_lapsed_retakes_it_and_resumes(docket: Docket):
    """A worker whose lease expired mid-walk takes it back and keeps going.

    Nobody else took the lease, so the worker must not sit resting: it retakes
    the lease on this pass and resumes from its kept place in the list.
    """
    sweep = a_sweep(docket, timedelta(seconds=8))
    sweep.start_id = "1000-0"

    async with docket.redis() as redis:
        assert await redis.get(docket.redelivery_sweep_key) is None

        assert await sweep.due(redis) is True

        assert sweep.start_id == "1000-0"
        assert (
            await redis.get(docket.redelivery_sweep_key) == sweep.worker_name.encode()
        )


async def test_reaching_the_end_starts_the_next_sweep_over(
    docket: Docket, sweep: RedeliverySweep, the_task: AsyncMock
):
    """Redis answers with the front of the list once the sweep has read it all."""
    await abandon_many(docket, the_task, count=5)

    async with docket.redis() as redis:
        claimed = await sweep.claim(redis, available_slots=20)

    assert len(claimed) == 5
    assert sweep.start_id == SWEEP_START


async def test_a_completed_sweep_is_not_due_until_the_timer_elapses(docket: Docket):
    """Between sweeps the worker skips the scan until the timer comes due."""
    sweep = a_sweep(docket, timedelta(seconds=8))

    async with docket.redis() as redis:
        await sweep.claim(redis, available_slots=10)

        assert sweep.start_id == SWEEP_START
        assert await sweep.due(redis) is False


@pytest.mark.parametrize("attempt", range(8))
async def test_the_next_wait_falls_inside_the_jitter_band(docket: Docket, attempt: int):
    """After a sweep reaches the front, the next wait lands within the band."""
    interval = 2.0
    sweep = a_sweep(docket, timedelta(seconds=interval * 4))

    before = time.monotonic()
    async with docket.redis() as redis:
        await sweep.claim(redis, available_slots=10)
    after = time.monotonic()

    low, high = JITTER
    assert before + low * interval <= sweep.next_sweep <= after + high * interval


async def test_a_missing_consumer_group_is_created_and_the_claim_retried(
    docket: Docket, sweep: RedeliverySweep
):
    """A docket that no worker has bootstrapped yet has no consumer group.

    Redis answers NOGROUP, and the sweep creates the group and claims again
    rather than failing the worker's poll.
    """
    async with docket.redis() as redis:
        assert await sweep.claim(redis, available_slots=10) == []

        groups = await redis.xinfo_groups(docket.stream_key)
        assert len(groups) == 1


async def test_the_worker_redelivers_every_abandoned_message(
    docket: Docket, the_task: AsyncMock, monkeypatch: pytest.MonkeyPatch
):
    """A worker reaches entries past a single bounded claim.

    Shrinking the scan batch to one caps each XAUTOCLAIM at about ten scanned
    entries, so no single claim can reach the whole list.  The kept cursor
    walks the rest, so every abandoned message is redelivered and run.

    The timeout leaves room for a poll pass inside a quarter of it, which is
    how long the sweep's lease runs.  A worker whose pass outlasts its own
    lease gives the walk up and starts over at the front, and can then reclaim
    a message it is still running.
    """
    monkeypatch.setattr("docket._redelivery.REDELIVERY_SCAN_BATCH", 1)

    await abandon_many(docket, the_task, count=25)

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert the_task.await_count == 25


async def test_an_abandoned_message_is_reclaimed_after_the_lease_expires(
    docket: Docket, the_task: AsyncMock
):
    """A lease another worker holds only delays a sweep, never blocks it.

    Another worker holds the sweep lease when this worker starts, so its first
    sweep attempts lose the lease and arm the next timer.  The lease expires
    after one interval, well before the message has been idle for the whole
    redelivery timeout, so this worker still wins the lease and reclaims the
    message.  The timeout, not the lease, bounds redelivery.
    """
    await abandon_many(docket, the_task, count=1)

    async with docket.redis() as redis:
        await redis.set(docket.redelivery_sweep_key, "other-worker", nx=True, px=100)

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=400),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    the_task.assert_awaited_once_with()
