import fcntl
import logging
import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import AsyncGenerator, Callable, Generator, Iterable, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import ConnectionPool, Redis

from docket import Docket, Worker

from tests._key_leak_checker import KeyCountChecker

REDIS_VERSION = os.environ.get("REDIS_VERSION", "7.4")


@pytest.fixture(autouse=True)
def log_level(caplog: pytest.LogCaptureFixture) -> Generator[None, None, None]:
    with caplog.at_level(logging.DEBUG):
        yield


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


@contextmanager
def _sync_redis(url: str) -> Generator[Redis, None, None]:  # pragma: no cover
    pool: ConnectionPool | None = None  # pragma: no cover
    redis = Redis.from_url(url)  # type: ignore  # pragma: no cover
    try:  # pragma: no cover
        with redis:  # pragma: no cover
            pool = redis.connection_pool  # type: ignore  # pragma: no cover
            yield redis  # pragma: no cover
    finally:  # pragma: no cover
        if pool:  # pragma: no branch
            pool.disconnect()  # pragma: no cover


@contextmanager
def _adminitrative_redis(port: int) -> Generator[Redis, None, None]:  # pragma: no cover
    with _sync_redis(f"redis://localhost:{port}/15") as r:  # pragma: no cover
        yield r  # pragma: no cover


def _wait_for_redis(port: int) -> None:  # pragma: no cover
    while True:  # pragma: no cover
        try:  # pragma: no cover
            with _adminitrative_redis(port) as r:  # pragma: no cover
                success = r.ping()  # type: ignore  # pragma: no cover
                if success:  # pragma: no branch
                    return  # pragma: no cover
        except redis.exceptions.ConnectionError:  # pragma: no cover
            time.sleep(0.1)  # pragma: no cover


@pytest.fixture(scope="session")
def redis_server(
    testrun_uid: str, worker_id: str
) -> Generator[Container | None, None, None]:
    # Skip Redis container setup for memory backend
    if REDIS_VERSION == "memory":  # pragma: no branch
        yield None  # pragma: no cover
        return  # pragma: no cover

    client = DockerClient.from_env()  # pragma: no cover

    container: Container | None = None  # pragma: no cover
    lock_file_name = f"/tmp/docket-unit-tests-{testrun_uid}-startup"  # pragma: no cover

    with open(lock_file_name, "w+") as lock_file:  # pragma: no cover
        fcntl.flock(lock_file, fcntl.LOCK_EX)  # pragma: no cover

        containers: Iterable[Container] = cast(  # pragma: no cover
            Iterable[Container],  # pragma: no cover
            client.containers.list(  # type: ignore  # pragma: no cover
                all=True,  # pragma: no cover
                filters={"label": "source=docket-unit-tests"},  # pragma: no cover
            ),  # pragma: no cover
        )  # pragma: no cover
        for c in containers:  # pragma: no cover
            if c.labels.get("testrun_uid") == testrun_uid:  # type: ignore  # pragma: no cover
                container = c  # pragma: no cover
            else:  # pragma: no cover
                c.remove(force=True)  # pragma: no cover

        if not container:  # pragma: no cover
            with socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            ) as s:  # pragma: no cover
                s.bind(("", 0))  # pragma: no cover
                redis_port = s.getsockname()[1]  # pragma: no cover

            image = f"redis:{REDIS_VERSION}"  # pragma: no cover
            if REDIS_VERSION.startswith("valkey-"):  # pragma: no branch
                image = f"valkey/valkey:{REDIS_VERSION.replace('valkey-', '')}"  # pragma: no cover

            container = client.containers.run(  # pragma: no cover
                image,  # pragma: no cover
                detach=True,  # pragma: no cover
                ports={"6379/tcp": redis_port},  # pragma: no cover
                labels={  # pragma: no cover
                    "source": "docket-unit-tests",  # pragma: no cover
                    "testrun_uid": testrun_uid,  # pragma: no cover
                },  # pragma: no cover
                auto_remove=True,  # pragma: no cover
            )  # pragma: no cover

            _wait_for_redis(redis_port)  # pragma: no cover
        else:  # pragma: no cover
            port_bindings = container.attrs["HostConfig"]["PortBindings"][
                "6379/tcp"
            ]  # pragma: no cover
            redis_port = int(port_bindings[0]["HostPort"])  # pragma: no cover

        with _adminitrative_redis(redis_port) as r:  # pragma: no cover
            r.sadd(f"docket-unit-tests:{testrun_uid}", worker_id)  # pragma: no cover

    try:  # pragma: no cover
        yield container  # pragma: no cover
    finally:  # pragma: no cover
        with _adminitrative_redis(redis_port) as r:  # pragma: no cover
            with r.pipeline() as pipe:  # type: ignore  # pragma: no cover
                pipe.srem(
                    f"docket-unit-tests:{testrun_uid}", worker_id
                )  # pragma: no cover
                pipe.scard(f"docket-unit-tests:{testrun_uid}")  # pragma: no cover
                count: int  # pragma: no cover
                _, count = pipe.execute()  # type: ignore  # pragma: no cover

        if count == 0:  # pragma: no cover
            container.stop()  # pragma: no cover
            os.remove(lock_file_name)  # pragma: no cover


@pytest.fixture
def redis_port(redis_server: Container | None) -> int:
    if redis_server is None:  # pragma: no branch
        # Memory backend - return dummy port
        return 0  # pragma: no cover
    port_bindings = redis_server.attrs["HostConfig"]["PortBindings"][
        "6379/tcp"
    ]  # pragma: no cover
    return int(port_bindings[0]["HostPort"])  # pragma: no cover


@pytest.fixture(scope="session")
def redis_db(worker_id: str) -> int:
    if not worker_id or "gw" not in worker_id:
        return 0  # pragma: no cover
    else:
        return 0 + int(worker_id.replace("gw", ""))  # pragma: no cover


@pytest.fixture
def redis_url(redis_port: int, redis_db: int, worker_id: str) -> str:
    if REDIS_VERSION == "memory":  # pragma: no branch
        # Use memory:// URL for in-memory backend
        # All memory:// URLs share the same FakeServer; dockets are isolated by name
        return "memory://"  # pragma: no cover

    url = f"redis://localhost:{redis_port}/{redis_db}"  # pragma: no cover
    with _sync_redis(url) as r:  # pragma: no cover
        r.flushdb()  # type: ignore  # pragma: no cover
    return url  # pragma: no cover


@pytest.fixture
async def docket(redis_url: str) -> AsyncGenerator[Docket, None]:
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        yield docket


@pytest.fixture
async def worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        yield worker


@pytest.fixture
def the_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    task.return_value = None
    return task


@pytest.fixture
def another_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "another_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    return task


@pytest.fixture(autouse=True)
async def key_leak_checker(
    redis_url: str, docket: Docket
) -> AsyncGenerator[KeyCountChecker, None]:
    """Automatically verify no keys without TTL leak in any test.

    This autouse fixture runs for every test and ensures that no Redis keys
    without TTL are created during test execution, preventing memory leaks in
    long-running Docket deployments.

    Tests can add exemptions for specific keys:
    - key_leak_checker.add_exemption(f"{docket.name}:special-key")
    """
    checker = KeyCountChecker(docket)

    # Prime infrastructure with a temporary worker that exits immediately
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as temp_worker:
        await temp_worker.run_until_finished()
        # Clean up heartbeat data to avoid polluting tests that check worker counts
        async with docket.redis() as r:
            await r.zrem(docket.workers_set, temp_worker.name)
            for task_name in docket.tasks:
                await r.zrem(docket.task_workers_set(task_name), temp_worker.name)
            await r.delete(docket.worker_tasks_set(temp_worker.name))

    await checker.capture_baseline()

    yield checker

    # Verify no leaks after test completes
    await checker.verify_remaining_keys_have_ttl()
