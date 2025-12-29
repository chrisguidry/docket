"""Integration tests for Redis Cluster support.

These tests verify cluster-specific behavior that cannot be tested with mocks.
They require a running Redis Cluster:

    docker compose -f tests/cluster/docker-compose.yml up -d
    docker compose -f tests/cluster/docker-compose.yml --profile test run --rm test-runner
    docker compose -f tests/cluster/docker-compose.yml down
"""

import os
import uuid
from datetime import timedelta
from typing import AsyncGenerator
from urllib.parse import urlparse

import pytest
from redis.asyncio.cluster import RedisCluster

from docket import Docket, Worker
from docket._redis import get_cluster_client

CLUSTER_URL = os.environ.get("CLUSTER_URL", "redis+cluster://localhost:7001")


def cluster_available() -> bool:
    """Check if Redis cluster is running and accessible."""
    import socket

    parsed = urlparse(CLUSTER_URL.replace("redis+cluster://", "redis://"))
    host = parsed.hostname or "localhost"
    port = parsed.port or 7001

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not cluster_available(),
    reason="Redis Cluster not available",
)


@pytest.fixture
async def docket() -> AsyncGenerator[Docket, None]:
    """Create a Docket connected to the cluster."""
    unique_name = f"cluster-test-{uuid.uuid4().hex[:8]}"
    async with Docket(name=unique_name, url=CLUSTER_URL) as d:
        yield d
        # Cleanup
        client = await get_cluster_client(CLUSTER_URL)
        keys: list[bytes] = []
        async for key in client.scan_iter(match=f"{{{unique_name}}}:*"):  # type: ignore[union-attr]
            keys.append(key)  # type: ignore[arg-type]
        if keys:
            await client.delete(*keys)  # type: ignore[arg-type]


@pytest.fixture
async def worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    """Create a Worker for the cluster-connected Docket."""
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as w:
        yield w


class TestClusterConnection:
    """Verify basic cluster connectivity."""

    async def test_cluster_is_healthy(self) -> None:
        """Cluster should be accessible and healthy."""
        client = await get_cluster_client(CLUSTER_URL)
        assert isinstance(client, RedisCluster)
        result: bool = await client.ping()  # type: ignore[reportUnknownMemberType]
        assert result is True

        info = await client.cluster_info()
        assert info["cluster_state"] == "ok"


class TestClusterKeySlots:
    """Verify hash tags ensure keys land in same slot."""

    async def test_hash_tag_format(self, docket: Docket) -> None:
        """Cluster mode should use braced hash tags."""
        assert docket.hash_tag == f"{{{docket.name}}}"
        assert docket.queue_key.startswith(f"{{{docket.name}}}")
        assert docket.stream_key.startswith(f"{{{docket.name}}}")

    async def test_all_keys_same_slot(self, docket: Docket) -> None:
        """All docket keys should hash to the same cluster slot."""
        client = await get_cluster_client(CLUSTER_URL)

        keys = [
            docket.queue_key,
            docket.stream_key,
            docket.workers_set,
            docket.runs_key("test"),
            docket.parked_task_key("test"),
        ]

        slots = [await client.cluster_keyslot(k) for k in keys]
        assert len(set(slots)) == 1, (
            f"Keys in different slots: {list(zip(keys, slots))}"
        )


class TestClusterLuaScripts:
    """Verify multi-key Lua scripts work with cluster."""

    async def test_scheduler_moves_tasks(self, docket: Docket, worker: Worker) -> None:
        """Scheduler Lua script should atomically move tasks across keys."""
        executed: list[int] = []

        async def task(n: int) -> None:
            executed.append(n)

        docket.register(task)

        # Schedule multiple tasks - scheduler uses multi-key Lua
        for i in range(3):
            await docket.add(task, key=f"task-{i}")(i)

        await worker.run_until_finished()
        assert sorted(executed) == [0, 1, 2]
