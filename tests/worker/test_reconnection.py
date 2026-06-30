"""Tests for how the worker survives losing its Redis connection."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError

from docket import Docket, Worker
from docket._redis import RedisClient


async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock
):
    """The worker should reconnect when the connection is lost"""
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=100))

    # Mock the _worker_loop method to fail once then succeed
    original_worker_loop = worker._worker_loop  # type: ignore[protected-access]
    call_count = 0

    async def mock_worker_loop(
        redis: RedisClient,
        *,
        run_state: Any,
        forever: bool = False,
    ):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated connection error")
        return await original_worker_loop(
            redis,
            run_state=run_state,
            forever=forever,
        )

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


async def test_worker_does_not_restart_when_stop_races_with_reconnect_timeout(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    """A stop request after reconnect timeout must prevent another loop."""

    async def reconnect_task() -> None: ...

    docket.register(reconnect_task)
    redis_calls = 0

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        nonlocal redis_calls
        redis_calls += 1
        if redis_calls == 1:
            raise ConnectionError("transient outage")

        raise AssertionError(
            "worker restarted after shutdown request"
        )  # pragma: no cover
        yield  # pragma: no cover

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(
        docket,
        reconnection_delay=timedelta(milliseconds=5),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        original_wait_for = asyncio.wait_for

        async def stop_as_reconnect_delay_expires(
            awaitable: Any,
            timeout: float | None = None,
        ) -> Any:
            if timeout == worker.reconnection_delay.total_seconds():
                awaitable.close()
                worker._worker_stop_requested.set()  # pyright: ignore[reportPrivateUsage]
                worker._worker_stopping.set()  # pyright: ignore[reportPrivateUsage]
                raise asyncio.TimeoutError

            return await original_wait_for(awaitable, timeout)  # pragma: no cover

        monkeypatch.setattr(asyncio, "wait_for", stop_as_reconnect_delay_expires)

        await worker.run_forever()

    assert redis_calls == 1
