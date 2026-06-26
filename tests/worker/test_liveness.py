"""Tests for worker liveness announcements and Redis recovery."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator, cast
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError

from docket import Docket, Worker
from docket._redis import RedisClient


async def test_worker_context_without_processing_loop_is_not_announced(
    docket: Docket,
    the_task: AsyncMock,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3
    docket.register(the_task)

    async with Worker(docket, name="idle-worker"):
        await asyncio.sleep(heartbeat.total_seconds() * 2)

        snapshot = await docket.snapshot()

    assert {worker.name for worker in snapshot.workers} == set()


async def test_worker_waits_for_slow_readiness_before_announcement(
    docket: Docket,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    task_complete = False

    async def delayed_task() -> None:
        nonlocal task_complete
        task_complete = True

    await docket.add(delayed_task)()

    async with Worker(
        docket,
        name="not-ready-worker",
        reconnection_delay=heartbeat,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        listener_ready = asyncio.Event()

        async def slow_listener() -> None:
            await asyncio.sleep(heartbeat.total_seconds() * 8)
            worker._cancellation_ready.set()  # pyright: ignore[reportPrivateUsage]
            listener_ready.set()
            await worker._worker_stopping.wait()  # pyright: ignore[reportPrivateUsage]

        with patch.object(worker, "_cancellation_listener", slow_listener):
            worker_task = asyncio.create_task(worker.run_until_finished())
            try:
                await asyncio.sleep(heartbeat.total_seconds() * 2)

                snapshot = await docket.snapshot()
                await asyncio.wait_for(listener_ready.wait(), timeout=1)
                await asyncio.wait_for(worker_task, timeout=5)
            finally:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)

    assert {worker.name for worker in snapshot.workers} == set()
    assert task_complete


async def test_running_worker_recovers_from_transient_heartbeat_connection_error(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async def liveness_task() -> None: ...

    docket.register(liveness_task)

    async with Worker(
        docket,
        name="heartbeat-worker",
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())
        try:
            await asyncio.sleep(heartbeat.total_seconds() * 2)
            assert {w.name for w in await docket.workers()} == {worker.name}

            original_redis = docket.redis
            failed_once = False

            @asynccontextmanager
            async def flaky_heartbeat_redis() -> AsyncGenerator[RedisClient, None]:
                nonlocal failed_once
                if not failed_once:
                    failed_once = True
                    raise ConnectionError("transient heartbeat outage")

                async with original_redis() as r:
                    yield r

            monkeypatch.setattr(docket, "redis", flaky_heartbeat_redis)

            await asyncio.sleep(heartbeat.total_seconds() * 4)

            workers = await docket.workers()
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    assert failed_once
    assert {worker.name for worker in workers} == {worker.name}


async def test_worker_recovers_from_transient_claim_connection_error(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    """A transient Redis outage while claiming work should not strand due tasks."""

    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    task_complete = False

    async def transient_task() -> None:
        nonlocal task_complete
        task_complete = True

    await docket.add(transient_task)()

    original_redis = docket.redis
    failed_once = False

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        nonlocal failed_once

        async with original_redis() as r:

            class FlakyClaimRedis:
                def __getattr__(self, name: str) -> Any:
                    return getattr(r, name)

                async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
                    nonlocal failed_once
                    if not failed_once:
                        failed_once = True
                        raise ConnectionError("transient outage")
                    return await r.xreadgroup(*args, **kwargs)

            yield cast(RedisClient, FlakyClaimRedis())

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(
        docket,
        reconnection_delay=heartbeat,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=5.0)

    snapshot = await docket.snapshot()

    assert failed_once
    assert task_complete
    assert snapshot.total_tasks == 0
    assert snapshot.running == []


@pytest.mark.parametrize("shutdown_delay", [0.0, 0.01])
async def test_worker_context_exit_stops_reconnect_retry(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
    shutdown_delay: float,
):
    """A retrying worker should not outlive its context and restart after cleanup."""

    async def reconnect_task() -> None: ...

    docket.register(reconnect_task)

    original_redis = docket.redis
    claim_failed = asyncio.Event()

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        async with original_redis() as r:

            class BrokenClaimRedis:
                def __getattr__(self, name: str) -> Any:
                    return getattr(r, name)

                async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
                    claim_failed.set()
                    raise ConnectionError("transient outage")

            yield cast(RedisClient, BrokenClaimRedis())

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(
        docket,
        reconnection_delay=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())
        await asyncio.wait_for(claim_failed.wait(), timeout=1)
        if shutdown_delay:
            await asyncio.sleep(shutdown_delay)

    await asyncio.wait_for(worker_task, timeout=1)


async def test_worker_context_exit_stops_retry_before_next_loop(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    """A shutdown request between reconnect attempts must not be cleared."""

    async def retry_task() -> None: ...

    docket.register(retry_task)

    original_redis = docket.redis
    redis_calls = 0
    retry_waiting_to_enter_loop = asyncio.Event()
    release_retry = asyncio.Event()

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        nonlocal redis_calls
        redis_calls += 1

        if redis_calls == 1:
            raise ConnectionError("transient outage")

        retry_waiting_to_enter_loop.set()
        await release_retry.wait()
        async with original_redis() as r:
            yield r

    monkeypatch.setattr(docket, "redis", mock_redis)

    async def exercise_shutdown_race() -> None:
        async with Worker(
            docket,
            reconnection_delay=timedelta(milliseconds=5),
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            worker_task = asyncio.create_task(worker.run_forever())

            async def release_retry_on_shutdown() -> None:
                while not worker._worker_stopping.is_set():  # pyright: ignore[reportPrivateUsage]
                    await asyncio.sleep(0)
                release_retry.set()

            release_task = asyncio.create_task(release_retry_on_shutdown())
            await retry_waiting_to_enter_loop.wait()

        await worker_task
        await release_task

    await asyncio.wait_for(exercise_shutdown_race(), timeout=1)


async def test_worker_context_exit_stops_readiness_wait(docket: Docket):
    """A worker should not hang on exit while listener readiness is pending."""

    async def readiness_task() -> None: ...

    docket.register(readiness_task)
    worker_task: asyncio.Task[None] | None = None

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:

        async def never_ready_listener() -> None:
            await asyncio.Event().wait()

        cast(Any, worker)._cancellation_listener = never_ready_listener
        worker_task = asyncio.create_task(worker.run_forever())
        await asyncio.sleep(0.01)

    assert worker_task is not None
    await asyncio.wait_for(worker_task, timeout=1)
