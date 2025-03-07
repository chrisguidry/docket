import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
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


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


# @pytest.fixture(scope="session")
# def redis_port(unused_tcp_port_factory: Callable[[], int]) -> int:
#     return unused_tcp_port_factory()


@pytest.fixture(scope="session")
def redis_port() -> int:
    import socket

    # Find an unused port by creating a socket, binding to port 0 (which lets the OS choose),
    # getting the assigned port number, and then closing the socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@contextmanager
def _sync_redis(url: str) -> Generator[Redis, None, None]:
    pool: ConnectionPool | None = None
    redis = Redis.from_url(url, single_connection_client=True)  # type: ignore
    try:
        with redis:
            pool = redis.connection_pool  # type: ignore
            yield redis
    finally:
        if pool:  # pragma: no branch
            pool.disconnect()


@pytest.fixture(scope="session")
def redis_server(redis_port: int) -> Generator[Container, None, None]:
    client = DockerClient.from_env()

    container: Container

    # Find and remove any containers from previous test runs
    containers: Iterable[Container] = cast(
        Iterable[Container],
        client.containers.list(all=True, filters={"label": "source=docket-unit-tests"}),  # type: ignore
    )
    for container in containers:  # pragma: no cover
        container.remove(force=True)

    container = client.containers.run(
        f"redis:{REDIS_VERSION}",
        detach=True,
        ports={"6379/tcp": redis_port},
        labels={"source": "docket-unit-tests"},
        auto_remove=True,
    )

    url = f"redis://localhost:{redis_port}/0"

    while True:
        try:
            with _sync_redis(url) as r:
                success = r.ping()  # type: ignore
                if success:  # pragma: no branch
                    break
        except redis.exceptions.ConnectionError:  # pragma: no cover
            time.sleep(0.1)

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
def redis_url(redis_server: Container, redis_port: int) -> str:
    url = f"redis://localhost:{redis_port}/0"
    with _sync_redis(url) as r:
        r.flushdb()  # type: ignore
    return url


@pytest.fixture
async def docket(redis_url: str, aiolib: str) -> AsyncGenerator[Docket, None]:
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        yield docket


@pytest.fixture
async def worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    async with Worker(docket) as worker:
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
