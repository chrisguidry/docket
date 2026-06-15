"""Tests for how the worker survives losing its Redis connection."""

from datetime import timedelta
from unittest.mock import AsyncMock, patch

from docket._redis import RedisClient
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import ConnectionError

from docket import Docket, Worker


async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock
):
    """The worker should reconnect when the connection is lost"""
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=100))

    # Mock the _worker_loop method to fail once then succeed
    original_worker_loop = worker._worker_loop  # type: ignore[protected-access]
    call_count = 0

    async def mock_worker_loop(redis: RedisClient, forever: bool = False):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated connection error")
        return await original_worker_loop(redis, forever=forever)  # type: ignore[arg-type]

    worker._worker_loop = mock_worker_loop  # type: ignore[protected-access]

    await docket.add(the_task)()

    async with worker:
        await worker.run_until_finished()

        assert call_count == 2
        the_task.assert_called_once()


async def test_worker_reconnects_when_main_loop_read_disconnects(
    docket: Docket, the_task: AsyncMock
):
    """A ConnectionError raised by the worker's own blocking read should
    reconnect, not kill the worker.

    The main poll loop runs inside a TaskGroup, which re-raises a body
    exception wrapped in an ExceptionGroup. A failover (or a plain server
    restart) drops a blocked XREADGROUP with a ConnectionError; the worker must
    treat that as a disconnection and reconnect rather than dying with an
    unhandled ExceptionGroup."""
    xreadgroup_calls = 0
    original_redis = Redis.xreadgroup
    original_cluster = RedisCluster.xreadgroup

    async def flaky_redis_xreadgroup(self: Redis, *args: object, **kwargs: object):
        nonlocal xreadgroup_calls
        xreadgroup_calls += 1
        if xreadgroup_calls == 1:
            raise ConnectionError("Simulated failover mid-XREADGROUP")
        return await original_redis(self, *args, **kwargs)  # type: ignore[arg-type]

    async def flaky_cluster_xreadgroup(  # pragma: no cover
        self: RedisCluster, *args: object, **kwargs: object
    ):
        nonlocal xreadgroup_calls
        xreadgroup_calls += 1
        if xreadgroup_calls == 1:
            raise ConnectionError("Simulated failover mid-XREADGROUP")
        return await original_cluster(self, *args, **kwargs)  # type: ignore[arg-type]

    await docket.add(the_task)()

    with (
        patch.object(Redis, "xreadgroup", flaky_redis_xreadgroup),
        patch.object(RedisCluster, "xreadgroup", flaky_cluster_xreadgroup),
    ):
        async with Worker(
            docket, reconnection_delay=timedelta(milliseconds=50)
        ) as worker:
            await worker.run_until_finished()

    the_task.assert_called_once()
    assert xreadgroup_calls >= 2
