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
