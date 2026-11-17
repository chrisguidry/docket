"""Tests for the scheduler moving due tasks to the stream in bounded batches."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from docket import Docket, ExecutionState, Worker
from docket.worker import _stream_due_tasks  # pyright: ignore[reportPrivateUsage]


async def stream_due(docket: Docket, batch: int) -> tuple[int, int, int]:
    async with docket.redis() as redis:
        total, due, scanned = await _stream_due_tasks(
            redis,
            queue_key=docket.queue_key,
            stream_key=docket.stream_key,
            now_timestamp=datetime.now(timezone.utc).timestamp(),
            docket_prefix=docket.prefix,
            batch=batch,
            completed_at=datetime.now(timezone.utc).isoformat(),
            ttl_seconds=int(docket.execution_ttl.total_seconds()),
        )
        return int(total), int(due), int(scanned)


async def queued_keys(docket: Docket) -> list[str]:
    async with docket.redis() as redis:
        members = await redis.zrange(docket.queue_key, 0, -1)
    return [m.decode() for m in members]


async def stream_length(docket: Docket) -> int:
    async with docket.redis() as redis:
        return await redis.xlen(docket.stream_key)


async def test_one_call_moves_at_most_one_batch(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """A call moves one batch and leaves the other due tasks in the queue."""
    docket.register(the_task)
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=20)
    for n in range(5):
        await docket.add(the_task, when=soon, key=f"due-{n}")()
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    await docket.add(the_task, when=later, key="later")()
    await asyncio.sleep(0.05)

    assert await stream_due(docket, batch=2) == (6, 2, 2)
    assert await stream_length(docket) == 2
    assert sorted(await queued_keys(docket)) == ["due-2", "due-3", "due-4", "later"]

    assert await stream_due(docket, batch=2) == (4, 2, 2)
    assert await stream_due(docket, batch=2) == (2, 1, 1)
    assert await stream_length(docket) == 5
    assert await queued_keys(docket) == ["later"]

    await docket.cancel("later")
    await worker.run_until_finished()
    assert the_task.await_count == 5


async def test_entries_without_parked_data_are_removed(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Queue entries without parked data leave the queue, so a batch's worth
    of them cannot hold back the due tasks behind them."""
    docket.register(the_task)
    async with docket.redis() as redis:
        await redis.zadd(docket.queue_key, {"orphan-1": 1, "orphan-2": 2})
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=20)
    await docket.add(the_task, when=soon, key="real")()
    await asyncio.sleep(0.05)

    assert await stream_due(docket, batch=2) == (3, 0, 2)
    assert await queued_keys(docket) == ["real"]

    assert await stream_due(docket, batch=2) == (1, 1, 1)
    assert await stream_length(docket) == 1
    assert await queued_keys(docket) == []

    await worker.run_until_finished()
    assert the_task.await_count == 1


async def lose_parked_data(docket: Docket, key: str) -> None:
    async with docket.redis() as redis:
        await redis.delete(docket.parked_task_key(key))


async def test_a_run_without_parked_data_fails(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """A scheduled run whose parked data is gone can never start, so the
    scheduler fails it: a caller waiting on its result gets the reason, and
    the key accepts a new task."""
    docket.register(the_task)
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=20)
    execution = await docket.add(the_task, when=soon, key="lost")()
    await lose_parked_data(docket, "lost")
    await asyncio.sleep(0.05)

    waiting = asyncio.create_task(execution.get_result())
    await asyncio.sleep(0)
    await worker.run_until_finished()

    with pytest.raises(Exception, match="parked data .* was missing"):
        await asyncio.wait_for(waiting, timeout=5)
    the_task.assert_not_awaited()

    await docket.add(the_task, key="lost")()
    await worker.run_until_finished()
    assert the_task.await_count == 1


async def test_a_run_without_parked_data_leaves_no_record_without_a_ttl(
    zero_ttl_docket: Docket, the_task: AsyncMock
):
    """With no execution TTL, the failed run's record is deleted."""
    docket = zero_ttl_docket
    docket.register(the_task)
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=20)
    await docket.add(the_task, when=soon, key="lost")()
    await lose_parked_data(docket, "lost")
    await asyncio.sleep(0.05)

    assert await stream_due(docket, batch=2) == (1, 0, 1)

    assert await docket.get_execution("lost") is None


async def test_a_stale_queue_entry_leaves_a_queued_run_alone(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Only a scheduled run waits on parked data.  A queue entry for a key
    whose run is already on the stream is removed without touching that run."""
    docket.register(the_task)
    execution = await docket.add(the_task, key="queued")()
    async with docket.redis() as redis:
        await redis.zadd(docket.queue_key, {"queued": 1})

    assert await stream_due(docket, batch=2) == (1, 0, 1)

    await execution.sync()
    assert execution.state == ExecutionState.QUEUED
    await worker.run_until_finished()
    assert the_task.await_count == 1


async def test_one_scheduler_pass_moves_every_due_task(
    docket: Docket, the_task: AsyncMock, monkeypatch: pytest.MonkeyPatch
):
    """The scheduler calls the script again while each call fills a batch,
    so a backlog larger than one batch does not wait for later ticks."""
    monkeypatch.setattr("docket.worker.DUE_TASKS_BATCH", 2)
    docket.register(the_task)
    soon = datetime.now(timezone.utc) + timedelta(milliseconds=20)
    for n in range(5):
        await docket.add(the_task, when=soon, key=f"due-{n}")()
    await asyncio.sleep(0.05)

    # With an hour between ticks, only the first pass can move these tasks.
    async with Worker(docket, scheduling_resolution=timedelta(hours=1)) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=10)

    assert the_task.await_count == 5
