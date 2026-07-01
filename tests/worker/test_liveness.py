"""Tests for worker liveness announcements."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import Any, AsyncGenerator
from unittest.mock import patch

from docket import Docket, Worker
from docket._redis import RedisClient
from redis.exceptions import ConnectionError
from tests.conftest import wait_until


async def test_worker_context_without_processing_loop_is_not_announced(
    docket: Docket,
):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async def liveness_task() -> None: ...

    docket.register(liveness_task)

    async with Worker(docket, name="idle-worker"):
        await asyncio.sleep(heartbeat.total_seconds() * 2)

        snapshot = await docket.snapshot()

    assert {worker.name for worker in snapshot.workers} == set()


async def test_worker_waits_for_cancellation_readiness_before_announcement(
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
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        listener_ready = asyncio.Event()

        async def slow_listener(
            stop_event: asyncio.Event,
            ready_event: asyncio.Event,
        ) -> None:
            await asyncio.sleep(heartbeat.total_seconds() * 8)
            ready_event.set()
            listener_ready.set()
            await stop_event.wait()

        with patch.object(worker, "_cancellation_listener", slow_listener):
            worker_run = asyncio.create_task(worker.run_until_finished())
            try:
                await asyncio.sleep(heartbeat.total_seconds() * 2)

                snapshot = await docket.snapshot()
                await asyncio.wait_for(listener_ready.wait(), timeout=1)
                await asyncio.wait_for(worker_run, timeout=5)
            finally:
                worker_run.cancel()
                await asyncio.gather(worker_run, return_exceptions=True)

    assert {worker.name for worker in snapshot.workers} == set()
    assert task_complete


async def test_context_exit_cancels_blocked_processing_heartbeat(docket: Docket):
    heartbeat_started = asyncio.Event()
    heartbeat_cancelled = asyncio.Event()

    async def liveness_task() -> None: ...

    docket.register(liveness_task)

    async def exercise() -> None:
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:

            async def blocked_heartbeat(stop_event: asyncio.Event) -> None:
                heartbeat_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    heartbeat_cancelled.set()
                    raise

            with patch.object(worker, "_heartbeat", blocked_heartbeat):
                worker_run = asyncio.create_task(worker.run_forever())
                await heartbeat_started.wait()

        await worker_run

    await asyncio.wait_for(exercise(), timeout=2.0)

    assert heartbeat_cancelled.is_set()


async def test_worker_draining_after_disconnect_is_not_announced(docket: Docket):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 100

    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    fail_next_read = asyncio.Event()
    read_failed = asyncio.Event()
    later_task_finished = asyncio.Event()

    async def long_task() -> None:
        task_started.set()
        await finish_task.wait()

    async def later_task() -> None:
        later_task_finished.set()

    class FailNextRead:
        def __init__(self, wrapped: Any):
            self._wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            return getattr(self._wrapped, name)

        async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
            if fail_next_read.is_set() and not read_failed.is_set():
                read_failed.set()
                raise ConnectionError("Simulated server loss mid-XREADGROUP")
            return await self._wrapped.xreadgroup(*args, **kwargs)

    original_redis = Docket.redis

    @asynccontextmanager
    async def flaky_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        async with original_redis(self) as redis:
            yield FailNextRead(redis)  # type: ignore[arg-type]

    async def worker_is_visible(worker_name: str) -> bool:
        return any(worker.name == worker_name for worker in await docket.workers())

    async def worker_is_absent(worker_name: str) -> bool:
        return not await worker_is_visible(worker_name)

    await docket.add(long_task)()

    with patch.object(Docket, "redis", flaky_redis):
        async with Worker(
            docket,
            name="draining-worker",
            concurrency=2,
            reconnection_delay=timedelta(milliseconds=100),
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
            schedule_automatic_tasks=False,
        ) as worker:
            worker_run = asyncio.create_task(worker.run_forever())
            try:
                await asyncio.wait_for(task_started.wait(), timeout=2.0)
                await wait_until(
                    lambda: worker_is_visible(worker.name),
                    timeout=1.0,
                    description="worker to publish initial heartbeat",
                )

                fail_next_read.set()
                await asyncio.wait_for(read_failed.wait(), timeout=2.0)
                await wait_until(
                    lambda: worker_is_absent(worker.name),
                    timeout=0.5,
                    description="draining worker to stop being announced",
                )

                finish_task.set()
                await docket.add(later_task)()
                await wait_until(
                    later_task_finished.is_set,
                    timeout=2.0,
                    description="worker to claim work after reconnect",
                )
            finally:
                worker_run.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_run


async def test_heartbeat_recovers_from_connection_error(docket: Docket):
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    async def liveness_task() -> None: ...

    docket.register(liveness_task)
    redis_calls = 0
    original_redis = Docket.redis

    @asynccontextmanager
    async def flaky_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        nonlocal redis_calls
        redis_calls += 1
        if redis_calls == 1:
            raise ConnectionError("simulated heartbeat connection error")
        async with original_redis(self) as redis:
            yield redis

    async with Worker(docket, name="heartbeat-worker") as worker:
        stop_event = asyncio.Event()
        with patch.object(Docket, "redis", flaky_redis):
            heartbeat_task = asyncio.create_task(
                worker._heartbeat(stop_event)  # pyright: ignore[reportPrivateUsage]
            )
            try:
                await wait_until(
                    lambda: redis_calls >= 2,
                    timeout=1.0,
                    description="heartbeat retry after connection error",
                )
            finally:
                stop_event.set()
                await heartbeat_task


async def test_remove_heartbeat_suppresses_cleanup_errors(docket: Docket):
    @asynccontextmanager
    async def broken_redis(self: Docket) -> AsyncGenerator[RedisClient, None]:
        raise RuntimeError("simulated cleanup error")
        yield  # pragma: no cover

    async with Worker(docket) as worker:
        with patch.object(Docket, "redis", broken_redis):
            await worker._remove_heartbeat()  # pyright: ignore[reportPrivateUsage]
