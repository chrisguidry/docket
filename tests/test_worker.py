import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from redis import RedisError
from redis.exceptions import ConnectionError

from docket import CurrentWorker, Docket, Worker


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
    original_worker_loop = worker._worker_loop
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
