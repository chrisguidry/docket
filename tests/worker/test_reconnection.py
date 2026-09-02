"""Tests for how the worker survives losing its Redis connection.

redis-py reports a lost server two ways: a ConnectionError when the socket
breaks, and a TimeoutError when a read or a connect runs out of time.  Neither
is a subclass of the other, so these tests cover both.
"""

import asyncio
import sys
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from docket._redis import RedisClient
from redis.exceptions import ConnectionError, TimeoutError

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup

if sys.version_info >= (3, 11):  # pragma: no cover
    from asyncio import timeout as async_timeout
else:  # pragma: no cover
    from async_timeout import timeout as async_timeout


from docket import Docket, Perpetual, Worker
from tests.conftest import wait_until


@pytest.mark.parametrize("error", [ConnectionError, TimeoutError])
async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock, error: type[Exception]
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
            raise error("Simulated connection error")
        return await original_worker_loop(redis, forever=forever)  # type: ignore[arg-type]

    worker._worker_loop = mock_worker_loop  # type: ignore[protected-access]

    await docket.add(the_task)()

    async with worker:
        await worker.run_until_finished()

        assert call_count == 2
        the_task.assert_called_once()


@pytest.mark.parametrize("error", [ConnectionError, TimeoutError])
async def test_worker_reconnects_when_main_loop_read_disconnects(
    docket: Docket, the_task: AsyncMock, error: type[Exception]
):
    """A connection error raised by the worker's own blocking read should
    reconnect, not kill the worker.

    The main poll loop runs inside a TaskGroup, which re-raises a body
    exception wrapped in an ExceptionGroup. A failover, a plain server
    restart, or a Redis too busy to answer drops a blocked XREADGROUP; the
    worker must treat that as a disconnection and reconnect rather than dying
    with an unhandled ExceptionGroup.

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
                raise error("Simulated server loss mid-XREADGROUP")
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


async def test_heartbeat_resumes_after_reconnect(docket: Docket):
    """The worker heartbeats again once it has reconnected and is ready.

    A worker leaves the workers set while it drains after a disconnect, and
    it must come back once it can claim work again, or `docket.workers()` and
    every decision that reads that set treat a live worker as dead.  Here the
    cancellation listener fails once to resubscribe after the reconnect, so
    the worker is announced again only after that second attempt succeeds.
    """
    docket.heartbeat_interval = timedelta(milliseconds=20)

    fail_next_read = asyncio.Event()
    read_failed = asyncio.Event()
    read_ok = asyncio.Event()
    fail_pubsub = asyncio.Event()
    pubsub_failed = asyncio.Event()

    class FailNextRead:
        def __init__(self, wrapped: Any):
            self._wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            return getattr(self._wrapped, name)

        async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
            if fail_next_read.is_set() and not read_failed.is_set():
                read_failed.set()
                raise ConnectionError("Simulated server loss mid-XREADGROUP")
            result = await self._wrapped.xreadgroup(*args, **kwargs)
            read_ok.set()
            return result

    original_redis = Docket.redis
    original_pubsub = Docket._pubsub  # pyright: ignore[reportPrivateUsage]

    @asynccontextmanager
    async def flaky_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        async with original_redis(self) as redis:
            yield FailNextRead(redis)  # type: ignore[arg-type]

    @asynccontextmanager
    async def flaky_pubsub(self: Docket) -> AsyncGenerator[Any, None]:
        if fail_pubsub.is_set() and not pubsub_failed.is_set():
            pubsub_failed.set()
            raise ConnectionError("Simulated server loss on PSUBSCRIBE")
        async with original_pubsub(self) as pubsub:
            yield pubsub

    async def worker_is_visible(name: str) -> bool:
        return any(worker.name == name for worker in await docket.workers())

    async def worker_is_absent(name: str) -> bool:
        return not await worker_is_visible(name)

    with (
        patch.object(Docket, "redis", flaky_redis),
        patch.object(Docket, "_pubsub", flaky_pubsub),
    ):
        async with Worker(
            docket,
            name="reconnecting-worker",
            reconnection_delay=timedelta(milliseconds=500),
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            worker_run = asyncio.create_task(worker.run_forever())
            try:
                await wait_until(
                    lambda: worker_is_visible(worker.name),
                    timeout=10.0,
                    description="the first heartbeat",
                )

                await asyncio.wait_for(read_ok.wait(), timeout=10.0)

                fail_pubsub.set()
                fail_next_read.set()
                await asyncio.wait_for(read_failed.wait(), timeout=10.0)
                await wait_until(
                    lambda: worker_is_absent(worker.name),
                    timeout=10.0,
                    description="the draining worker to leave the workers set",
                )

                await wait_until(
                    lambda: worker_is_visible(worker.name),
                    timeout=10.0,
                    description="the heartbeat to resume after reconnecting",
                )
            finally:
                worker_run.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_run


@pytest.mark.parametrize("error", [ConnectionError, TimeoutError])
async def test_worker_reconnects_when_automatic_seeding_disconnects(
    docket: Docket, error: type[Exception]
):
    """Losing Redis while seeding automatic perpetuals should reconnect.

    The worker seeds its automatic perpetuals inside the TaskGroup that runs
    its infrastructure, so an error there comes back wrapped in an
    ExceptionGroup that `except DISCONNECTED` cannot match.  The worker has to
    reconnect and seed again, not die on its first blip."""
    calls = 0

    async def automatic_task(
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=30), automatic=True),
    ):
        nonlocal calls
        calls += 1

    docket.register(automatic_task)

    seedings = 0
    original_seeding = Worker._schedule_all_automatic_perpetual_tasks  # type: ignore[protected-access]

    async def flaky_seeding(self: Worker) -> None:
        nonlocal seedings
        seedings += 1
        if seedings == 1:
            raise error("Simulated server loss while seeding automatic perpetuals")
        await original_seeding(self)

    with patch.object(Worker, "_schedule_all_automatic_perpetual_tasks", flaky_seeding):
        async with Worker(
            docket, reconnection_delay=timedelta(milliseconds=50)
        ) as worker:
            await worker.run_at_most({"automatic_task": 1})

    assert seedings == 2
    assert calls == 1

    await docket.cancel("automatic_task")


async def test_worker_fails_when_automatic_seeding_raises_a_real_error(docket: Docket):
    """A bug in seeding still fails the worker.

    Only a lost connection earns a reconnect.  Anything else has to reach the
    caller, so the worker cannot swallow real errors while it handles a lost
    connection."""

    async def automatic_task(
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=30), automatic=True),
    ):
        pass  # pragma: no cover

    docket.register(automatic_task)

    async def broken_seeding(self: Worker) -> None:
        raise ValueError("a real bug while seeding automatic perpetuals")

    with patch.object(
        Worker, "_schedule_all_automatic_perpetual_tasks", broken_seeding
    ):
        async with Worker(
            docket, reconnection_delay=timedelta(milliseconds=50)
        ) as worker:
            with pytest.raises(ExceptionGroup) as caught:
                await worker.run_until_finished()

    assert [type(error) for error in caught.value.exceptions] == [ValueError]
