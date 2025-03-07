import os
import time
from datetime import datetime, timezone
from functools import partial
from typing import Any, AsyncGenerator, Callable, Generator, Iterable, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import Redis

from docket import Docket, Worker

REDIS_VERSION = os.environ.get("REDIS_VERSION", "7.4")


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


@pytest.fixture(scope="session")
def redis_port(unused_tcp_port_factory: Callable[[], int]) -> int:
    return unused_tcp_port_factory()


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
            with Redis.from_url(url, single_connection_client=True) as r:  # type: ignore
                if r.ping():  # type: ignore
                    r.connection_pool.disconnect()
                    break
        except redis.exceptions.ConnectionError:  # pragma: no cover
            pass

        time.sleep(0.1)

    try:
        yield container
    finally:
        with Redis.from_url(url, single_connection_client=True) as r:  # type: ignore
            info: dict[str, Any] = r.info()  # type: ignore
            r.connection_pool.disconnect()

        container.stop()

        # By the time the test suite finishes, there should have been no more open
        # Redis connections (just the one that we used to ask about client connections).
        assert info["connected_clients"] == 1, (
            f"Expected 1 connected clients, but found {info['connected_clients']}"
        )


@pytest.fixture
def redis_url(redis_server: Container, redis_port: int) -> str:
    with Redis.from_url(  # type: ignore
        f"redis://localhost:{redis_port}/0",
        single_connection_client=True,
    ) as r:
        r.flushdb()  # type: ignore
        r.connection_pool.disconnect()

    return f"redis://localhost:{redis_port}/0"


@pytest.fixture
async def docket(redis_url: str) -> AsyncGenerator[Docket, None]:
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
