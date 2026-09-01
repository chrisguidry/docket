"""Tests for how the worker survives losing its Redis connection."""

import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, patch

from docket._redis import RedisClient
from redis.exceptions import ConnectionError

from docket import Docket, Worker

if sys.version_info >= (3, 11):  # pragma: no cover
    from asyncio import timeout as async_timeout
else:  # pragma: no cover
    from async_timeout import timeout as async_timeout


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
    unhandled ExceptionGroup.

    The fault is injected by wrapping the client the worker actually uses, so
    this exercises the real reconnect path on every backend -- standalone,
    cluster, and memory alike."""
    reads = {"count": 0}

    class FailFirstRead:
        """Delegates to a real client but raises on its first xreadgroup."""

        def __init__(self, wrapped: Any):
            self._wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            return getattr(self._wrapped, name)

        async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
            reads["count"] += 1
            if reads["count"] == 1:
                raise ConnectionError("Simulated server loss mid-XREADGROUP")
            return await self._wrapped.xreadgroup(*args, **kwargs)

    original_redis = Docket.redis

    @asynccontextmanager
    async def flaky_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        async with original_redis(self) as r:
            yield FailFirstRead(r)  # type: ignore[arg-type]

    await docket.add(the_task)()

    with patch.object(Docket, "redis", flaky_redis):
        async with Worker(
            docket, reconnection_delay=timedelta(milliseconds=50)
        ) as worker:
            await worker.run_until_finished()

    the_task.assert_called_once()
    assert reads["count"] >= 2


async def test_worker_stops_when_the_docket_connection_closes(docket: Docket):
    """A worker whose docket closed underneath it stops instead of raising.

    Docket teardown closes the one client every caller shares.  The worker's
    own read fails, and the retry that follows has nothing to reconnect to,
    so the worker has to finish rather than spin or raise.
    """
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=10))

    async def disconnected_worker_loop(redis: RedisClient, forever: bool = False):
        await docket._redis.__aexit__(None, None, None)  # pyright: ignore[reportPrivateUsage]
        raise ConnectionError("Simulated server loss")

    worker._worker_loop = disconnected_worker_loop  # type: ignore[protected-access]

    async with worker:
        async with async_timeout(5):
            await worker.run_forever()

        await docket._redis.__aenter__()  # pyright: ignore[reportPrivateUsage]
