from datetime import datetime, timezone
from functools import partial
from typing import AsyncGenerator, Callable
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from testcontainers.redis import RedisContainer

from docket import Docket, Worker


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


@pytest.fixture(scope="session")
async def redis_server() -> AsyncGenerator[RedisContainer, None]:
    container = RedisContainer("redis:7.4.2")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
async def docket(redis_server: RedisContainer) -> AsyncGenerator[Docket, None]:
    async with Docket(
        name=f"test-docket-{uuid4()}",
        host=redis_server.get_container_host_ip(),
        port=redis_server.get_exposed_port(6379),
        db=0,
    ) as docket:
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
