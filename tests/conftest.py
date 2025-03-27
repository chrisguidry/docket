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

REDIS_VERSION = os.environ.get("REDIS_VERSION", "7.4")


@pytest.fixture(autouse=True)
def log_level(caplog: pytest.LogCaptureFixture) -> Generator[None, None, None]:
    with caplog.at_level(logging.DEBUG):
        yield


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


@contextmanager
def _sync_redis(url: str) -> Generator[Redis, None, None]:
    pool: ConnectionPool | None = None
    redis = Redis.from_url(url)  # type: ignore
    try:
        with redis:
            pool = redis.connection_pool  # type: ignore
            yield redis
    finally:
        if pool:  # pragma: no branch
            pool.disconnect()


@contextmanager
def _adminitrative_redis(port: int) -> Generator[Redis, None, None]:
    with _sync_redis(f"redis://localhost:{port}/15") as r:
        yield r


def _wait_for_redis(port: int) -> None:
    while True:
        try:
            with _adminitrative_redis(port) as r:
                success = r.ping()  # type: ignore
                if success:  # pragma: no branch
                    return
        except redis.exceptions.ConnectionError:  # pragma: no cover
            time.sleep(0.1)


@pytest.fixture(scope="session")
def redis_server(testrun_uid: str, worker_id: str) -> Generator[Container, None, None]:
    client = DockerClient.from_env()

    container: Container | None = None
    lock_file_name = f"/tmp/docket-unit-tests-{testrun_uid}-startup"

    with open(lock_file_name, "w+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        containers: Iterable[Container] = cast(
            Iterable[Container],
            client.containers.list(  # type: ignore
                all=True,
                filters={"label": "source=docket-unit-tests"},
            ),
        )
        for c in containers:
            if c.labels.get("testrun_uid") == testrun_uid:  # type: ignore
                container = c
            else:
                c.remove(force=True)  # pragma: no cover

        if not container:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                redis_port = s.getsockname()[1]

            container = client.containers.run(
                f"redis:{REDIS_VERSION}",
                detach=True,
                ports={"6379/tcp": redis_port},
                labels={
                    "source": "docket-unit-tests",
                    "testrun_uid": testrun_uid,
                },
                auto_remove=True,
            )

            _wait_for_redis(redis_port)
        else:
            port_bindings = container.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
            redis_port = int(port_bindings[0]["HostPort"])

        with _adminitrative_redis(redis_port) as r:
            r.sadd(f"docket-unit-tests:{testrun_uid}", worker_id)

    try:
        yield container
    finally:
        with _adminitrative_redis(redis_port) as r:
            with r.pipeline() as pipe:  # type: ignore
                pipe.srem(f"docket-unit-tests:{testrun_uid}", worker_id)
                pipe.scard(f"docket-unit-tests:{testrun_uid}")
                count: int
                _, count = pipe.execute()  # type: ignore

        if count == 0:
            container.stop()
            os.remove(lock_file_name)


@pytest.fixture
def redis_port(redis_server: Container) -> int:
    port_bindings = redis_server.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
    return int(port_bindings[0]["HostPort"])


@pytest.fixture(scope="session")
def redis_db(worker_id: str) -> int:
    if not worker_id or "gw" not in worker_id:
        return 0  # pragma: no cover
    else:
        return 0 + int(worker_id.replace("gw", ""))  # pragma: no cover


@pytest.fixture
def redis_url(redis_port: int, redis_db: int) -> str:
    url = f"redis://localhost:{redis_port}/{redis_db}"
    with _sync_redis(url) as r:
        r.flushdb()  # type: ignore
    return url


@pytest.fixture
async def docket(redis_url: str, aiolib: str) -> AsyncGenerator[Docket, None]:
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
    task = AsyncMock()
    task.__name__ = "the_task"
    return task


@pytest.fixture
def another_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "another_task"
    return task
