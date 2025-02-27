import asyncio
from unittest.mock import AsyncMock

import pytest
from redis import RedisError

from docket import CurrentWorker, Docket, Worker


async def test_worker_aenter_propagates_connection_errors():
    """The worker should propagate Redis connection errors"""

    docket = Docket(name="test-docket", host="nonexistent-host", port=12345)
    worker = Worker(docket)
    with pytest.raises(RedisError):
        await worker.__aenter__()


@pytest.fixture
def the_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "the_task"
    return task


async def test_worker_acknowledges_messages(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """The worker should acknowledge and drain messages as they're processed"""

    await docket.add(the_task)()

    await worker.run_until_current()

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
        await asyncio.gather(worker1.run_until_current(), worker2.run_until_current())

    assert call_counts[worker1] + call_counts[worker2] == 100
    assert call_counts[worker1] > 40
    assert call_counts[worker2] > 40
