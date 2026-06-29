"""Tests for lease renewal while worker infrastructure reconnects."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator, cast

import pytest
from redis.exceptions import ConnectionError

from docket import Docket, Worker
from docket._redis import RedisClient
from docket.dependencies import current_worker
from tests.conftest import wait_until


async def test_worker_renews_active_task_lease_during_scheduler_reconnect(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    """A scheduler reconnect must not let another worker steal active work."""

    redelivery_timeout = timedelta(milliseconds=200)
    task_started = asyncio.Event()
    release_task = asyncio.Event()
    scheduler_failed = asyncio.Event()
    executions: list[str] = []

    async def slow_task() -> None:
        executions.append(current_worker.get().name)
        task_started.set()
        await release_task.wait()

    await docket.add(slow_task, key="slow-task")()

    original_redis = docket.redis
    failed_once = False

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        async with original_redis() as r:

            class FlakySchedulerRedis:
                def __getattr__(self, name: str) -> Any:
                    return getattr(r, name)

                async def evalsha(
                    self,
                    sha: str,
                    numkeys: int,
                    *args: Any,
                ) -> Any:
                    nonlocal failed_once
                    try:
                        worker_name = current_worker.get().name
                    except LookupError:  # pragma: no cover
                        worker_name = None

                    is_stream_due_tasks = (
                        numkeys == 2
                        and len(args) >= 2
                        and args[0] == docket.queue_key
                        and args[1] == docket.stream_key
                    )
                    if (
                        worker_name == "lease-holder"
                        and task_started.is_set()
                        and is_stream_due_tasks
                        and not failed_once
                    ):
                        failed_once = True
                        scheduler_failed.set()
                        raise ConnectionError("transient scheduler outage")

                    return await r.evalsha(sha, numkeys, *args)

            yield cast(RedisClient, FlakySchedulerRedis())

    monkeypatch.setattr(docket, "redis", mock_redis)

    worker_tasks: list[asyncio.Task[None]] = []
    observed_executions: list[str] = []
    async with Worker(
        docket,
        name="lease-holder",
        redelivery_timeout=redelivery_timeout,
        reconnection_delay=timedelta(milliseconds=50),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker_a:
        try:
            worker_tasks.append(asyncio.create_task(worker_a.run_forever()))
            await wait_until(
                lambda: len(executions) == 1,
                timeout=2.0,
                description="first execution to start",
            )

            async with Worker(
                docket,
                name="competitor",
                redelivery_timeout=redelivery_timeout,
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            ) as worker_b:
                try:
                    worker_tasks.append(asyncio.create_task(worker_b.run_forever()))
                    await asyncio.wait_for(scheduler_failed.wait(), timeout=1.0)
                    await asyncio.sleep(redelivery_timeout.total_seconds() * 3)
                    observed_executions = list(executions)
                finally:
                    release_task.set()
                    for task in worker_tasks:
                        task.cancel()
                    await asyncio.gather(*worker_tasks, return_exceptions=True)

        finally:
            release_task.set()

    assert failed_once
    assert observed_executions == ["lease-holder"]


async def test_worker_listens_for_cancellation_during_scheduler_reconnect_drain(
    docket: Docket,
    monkeypatch: pytest.MonkeyPatch,
):
    """Reconnect drains should still consume cancellation signals for active work."""

    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()
    release_task = asyncio.Event()
    scheduler_failed = asyncio.Event()

    async def slow_task() -> None:
        task_started.set()
        try:
            await release_task.wait()
            pytest.fail("task should be cancelled before release")  # pragma: no cover
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    await docket.add(slow_task, key="cancel-during-drain")()

    original_redis = docket.redis
    failed_once = False

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[RedisClient, None]:
        async with original_redis() as r:

            class FlakySchedulerRedis:
                def __getattr__(self, name: str) -> Any:
                    return getattr(r, name)

                async def evalsha(
                    self,
                    sha: str,
                    numkeys: int,
                    *args: Any,
                ) -> Any:
                    nonlocal failed_once
                    try:
                        worker_name = current_worker.get().name
                    except LookupError:  # pragma: no cover
                        worker_name = None

                    is_stream_due_tasks = (
                        numkeys == 2
                        and len(args) >= 2
                        and args[0] == docket.queue_key
                        and args[1] == docket.stream_key
                    )
                    if (
                        worker_name == "drain-cancel-listener"
                        and task_started.is_set()
                        and is_stream_due_tasks
                        and not failed_once
                    ):
                        failed_once = True
                        scheduler_failed.set()
                        raise ConnectionError("transient scheduler outage")

                    return await r.evalsha(sha, numkeys, *args)

            yield cast(RedisClient, FlakySchedulerRedis())

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(
        docket,
        name="drain-cancel-listener",
        redelivery_timeout=timedelta(milliseconds=200),
        reconnection_delay=timedelta(milliseconds=50),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())

        async def worker_is_not_advertised() -> bool:
            return worker.name not in {w.name for w in await docket.workers()}

        async def cancel_until_observed() -> None:
            while not task_cancelled.is_set():  # pragma: no branch
                await asyncio.sleep(0.01)
                await docket.cancel("cancel-during-drain")

        try:
            await asyncio.wait_for(task_started.wait(), timeout=2.0)
            await asyncio.wait_for(scheduler_failed.wait(), timeout=1.0)
            await wait_until(
                worker_is_not_advertised,
                timeout=2.0,
                description="worker heartbeat removal before reconnect drain",
            )

            cancel_task = asyncio.create_task(cancel_until_observed())
            try:
                await asyncio.wait_for(task_cancelled.wait(), timeout=2.0)
                await asyncio.wait_for(cancel_task, timeout=1.0)
            finally:
                cancel_task.cancel()
                await asyncio.gather(cancel_task, return_exceptions=True)
        finally:
            release_task.set()
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    execution = await docket.get_execution("cancel-during-drain")

    assert failed_once
    assert task_cancelled.is_set()
    assert execution is not None
    assert execution.state.value == "cancelled"
