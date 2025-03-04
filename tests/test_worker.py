import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from redis import RedisError
from redis.exceptions import ConnectionError

from docket import CurrentWorker, Docket, Worker
from docket.docket import RedisMessage


async def test_worker_aenter_propagates_connection_errors():
    """The worker should propagate Redis connection errors"""

    docket = Docket(name="test-docket", url="redis://nonexistent-host:12345/0")
    worker = Worker(docket)
    with pytest.raises(RedisError):
        await worker.__aenter__()


async def test_worker_acknowledges_messages(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """The worker should acknowledge and drain messages as they're processed"""

    await docket.add(the_task)()

    await worker.run_until_finished()

    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=worker.consumer_group_name,
        )
        assert pending_info["pending"] == 0

        assert await redis.xlen(docket.stream_key) == 0


async def test_two_workers_split_work(docket: Docket):
    """Two workers should split the workload"""

    worker1 = Worker(docket)
    worker2 = Worker(docket)

    call_counts = {
        worker1: 0,
        worker2: 0,
    }

    async def the_task(worker: Worker = CurrentWorker()):
        call_counts[worker] += 1

    for _ in range(100):
        await docket.add(the_task)()

    async with worker1, worker2:
        await asyncio.gather(worker1.run_until_finished(), worker2.run_until_finished())

    assert call_counts[worker1] + call_counts[worker2] == 100
    assert call_counts[worker1] > 40
    assert call_counts[worker2] > 40


async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock
):
    """The worker should reconnect when the connection is lost"""
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=100))

    # Mock the _worker_loop method to fail once then succeed
    original_worker_loop = worker._worker_loop  # type: ignore[protected-access]
    call_count = 0

    async def mock_worker_loop(forever: bool = False):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated connection error")
        return await original_worker_loop(forever=forever)

    worker._worker_loop = mock_worker_loop

    await docket.add(the_task)()

    async with worker:
        await worker.run_until_finished()

    assert call_count == 2
    the_task.assert_called_once()


async def test_worker_respects_concurrency_limit(docket: Docket, worker: Worker):
    """Worker should not exceed its configured concurrency limit"""

    task_results: set[int] = set()

    currently_running = 0
    max_concurrency_observed = 0

    async def concurrency_tracking_task(index: int):
        nonlocal currently_running, max_concurrency_observed

        currently_running += 1
        max_concurrency_observed = max(max_concurrency_observed, currently_running)

        await asyncio.sleep(0.01)
        task_results.add(index)

        currently_running -= 1

    for i in range(50):
        await docket.add(concurrency_tracking_task)(index=i)

    worker.concurrency = 5
    await worker.run_until_finished()

    assert task_results == set(range(50))

    assert 1 < max_concurrency_observed <= 5


async def test_worker_handles_redeliveries_from_abandoned_workers(
    docket: Docket, the_task: AsyncMock
):
    """The worker should handle redeliveries from abandoned workers"""

    await docket.add(the_task)()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("Nope"))  # type: ignore[protected-access]
        with pytest.raises(Exception, match="Nope"):
            await worker_a.run_until_finished()

    the_task.assert_not_called()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_b:
        async with docket.redis() as redis:
            pending_info = await redis.xpending(
                docket.stream_key,
                worker_b.consumer_group_name,
            )
            assert pending_info["pending"] == 1, (
                "Expected one pending task in the stream"
            )

        await asyncio.sleep(0.125)  # longer than the redelivery timeout

        await worker_b.run_until_finished()

    the_task.assert_awaited_once_with()


async def test_redeliveries_abide_by_concurrency_limits(docket: Docket, worker: Worker):
    task_results: set[int] = set()

    currently_running = 0
    max_concurrency_observed = 0

    async def concurrency_tracking_task(index: int):
        nonlocal currently_running, max_concurrency_observed

        currently_running += 1
        max_concurrency_observed = max(max_concurrency_observed, currently_running)

        await asyncio.sleep(0.01)
        task_results.add(index)

        currently_running -= 1

    for i in range(50):
        await docket.add(concurrency_tracking_task)(index=i)

    async with Worker(
        docket, concurrency=5, redelivery_timeout=timedelta(milliseconds=100)
    ) as bad_worker:
        original_execute = bad_worker._execute  # type: ignore[protected-access]

        async def die_after_10_tasks(message: RedisMessage):
            if len(task_results) >= 10:
                raise Exception("Nope")
            return await original_execute(message)

        bad_worker._execute = die_after_10_tasks  # type: ignore[protected-access]
        with pytest.raises(Exception, match="Nope"):
            await bad_worker.run_until_finished()

    assert 1 < max_concurrency_observed <= 5
    assert len(task_results) == 10

    await asyncio.sleep(0.125)  # longer than the redelivery timeout

    worker.concurrency = 5
    worker.redelivery_timeout = timedelta(milliseconds=100)
    await worker.run_until_finished()

    assert task_results == set(range(50))

    assert 1 < max_concurrency_observed <= 5
