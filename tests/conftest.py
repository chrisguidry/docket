import time
from datetime import datetime, timezone
from functools import partial
from typing import AsyncGenerator, Callable, Generator, Iterable, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import Redis

from docket import Docket, Worker


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
        "redis:7.4.2",
        detach=True,
        ports={"6379/tcp": redis_port},
        labels={"source": "docket-unit-tests"},
        auto_remove=True,
    )

    while True:
        try:
            with Redis.from_url(f"redis://localhost:{redis_port}/0") as r:  # type: ignore
                if r.ping():  # type: ignore
                    break
        except redis.exceptions.ConnectionError:
            pass

        time.sleep(0.1)

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
def redis_url(redis_server: Container, redis_port: int) -> str:
    with Redis.from_url(f"redis://localhost:{redis_port}/0") as r:  # type: ignore
        r.flushdb()  # type: ignore

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
