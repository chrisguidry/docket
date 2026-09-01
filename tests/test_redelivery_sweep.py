"""Tests for the bounded, cursor-kept redelivery sweep.

The sweep claims its consumer group's pending list with XAUTOCLAIM to find
messages another worker abandoned.  Each claim is bounded, and the cursor is
kept across claims so entries past the first window are still reached.
"""

import inspect
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Worker
from docket._redelivery import SWEEP_START, RedeliverySweep


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

    A zero timeout means XAUTOCLAIM claims whatever is pending, so a test does
    not have to wait for messages to go idle.
    """
    return RedeliverySweep(
        docket,
        worker_name="the-sweeping-worker",
        redelivery_timeout=timedelta(0),
    )


def test_a_fresh_sweep_starts_at_the_front(sweep: RedeliverySweep):
    """A sweep that has claimed nothing yet starts at the front of the list."""
    assert sweep.start_id == SWEEP_START


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


async def test_reaching_the_end_starts_the_next_sweep_over(
    docket: Docket, sweep: RedeliverySweep, the_task: AsyncMock
):
    """Redis answers with the front of the list once the sweep has read it all."""
    await abandon_many(docket, the_task, count=5)

    async with docket.redis() as redis:
        claimed = await sweep.claim(redis, available_slots=20)

    assert len(claimed) == 5
    assert sweep.start_id == SWEEP_START


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
    """
    monkeypatch.setattr("docket._redelivery.REDELIVERY_SCAN_BATCH", 1)

    await abandon_many(docket, the_task, count=25)

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=50),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert the_task.await_count == 25
