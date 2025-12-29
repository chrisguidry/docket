"""Integration tests for Redis Cluster support.

These tests require a running Redis Cluster. Start the cluster with:

    docker compose -f tests/cluster/docker-compose.yml up -d

Then run the tests (Linux, or inside Docker container):

    pytest tests/cluster/test_cluster.py -v

On macOS/Windows, run tests inside Docker:

    docker compose -f tests/cluster/docker-compose.yml run --rm test-runner

To stop the cluster:

    docker compose -f tests/cluster/docker-compose.yml down
"""

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator
from urllib.parse import urlparse

import pytest
from redis.asyncio.cluster import RedisCluster

from docket import Docket, Worker
from docket._redis import get_cluster_client, is_cluster_url, normalize_cluster_url

# Cluster URL for testing - can be overridden via environment variable
# Default connects to localhost:7001 (for Linux/direct host access)
# Inside Docker, set CLUSTER_URL=redis+cluster://172.30.0.11:6379
CLUSTER_URL = os.environ.get("CLUSTER_URL", "redis+cluster://localhost:7001")


def cluster_available() -> bool:
    """Check if Redis cluster is running and accessible."""
    import socket

    # Parse the cluster URL to get host and port
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
    reason="Redis Cluster not available. Start with: docker compose -f tests/cluster/docker-compose.yml up -d",
)


class TestClusterURLParsing:
    """Tests for cluster URL detection and parsing."""

    def test_is_cluster_url_positive(self) -> None:
        """redis+cluster:// URLs should be detected as cluster URLs."""
        assert is_cluster_url("redis+cluster://localhost:7001")
        assert is_cluster_url("redis+cluster://localhost:7001,localhost:7002")

    def test_is_cluster_url_negative(self) -> None:
        """Regular redis:// URLs should not be detected as cluster URLs."""
        assert not is_cluster_url("redis://localhost:6379")
        assert not is_cluster_url("memory://")

    def test_normalize_cluster_url(self) -> None:
        """Cluster URLs should be normalized to redis:// for redis-py."""
        assert (
            normalize_cluster_url("redis+cluster://localhost:7001")
            == "redis://localhost:7001"
        )
        assert (
            normalize_cluster_url("redis://localhost:6379") == "redis://localhost:6379"
        )


class TestClusterClient:
    """Tests for cluster client creation."""

    @pytest.mark.asyncio
    async def test_get_cluster_client(self) -> None:
        """Should be able to get a cluster client."""
        client = await get_cluster_client(CLUSTER_URL)
        assert isinstance(client, RedisCluster)

        # Verify we can ping the cluster
        result: bool = await client.ping()  # type: ignore[assignment]
        assert result is True

    @pytest.mark.asyncio
    async def test_cluster_info(self) -> None:
        """Should be able to get cluster info."""
        client = await get_cluster_client(CLUSTER_URL)

        # Get cluster info
        info = await client.cluster_info()
        assert info["cluster_state"] == "ok"
        assert int(info["cluster_known_nodes"]) >= 6


class TestDocketClusterBasics:
    """Basic tests for Docket with Redis Cluster."""

    @pytest.fixture
    async def docket(self) -> AsyncGenerator[Docket, None]:
        """Create a Docket instance connected to the cluster with unique name."""
        # Use unique name per test for isolation
        unique_name = f"cluster-test-{uuid.uuid4().hex[:8]}"
        async with Docket(
            name=unique_name,
            url=CLUSTER_URL,
        ) as docket:
            yield docket
            # Cleanup: delete all keys for this docket
            client = await get_cluster_client(CLUSTER_URL)
            # Scan for keys with this docket's hash tag
            keys: list[bytes] = []
            async for key in client.scan_iter(match=f"{{{unique_name}}}:*"):  # type: ignore[union-attr]
                keys.append(key)  # type: ignore[arg-type]
            if keys:
                await client.delete(*keys)  # type: ignore[arg-type]

    @pytest.fixture
    async def worker(self, docket: Docket) -> AsyncGenerator[Worker, None]:
        """Create a Worker for the cluster-connected Docket."""
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            yield worker

    @pytest.mark.asyncio
    async def test_hash_tag_format(self, docket: Docket) -> None:
        """Docket keys should use hash tag format for cluster slot consistency."""
        # Verify hash_tag format uses the docket name with curly braces
        assert docket.hash_tag == f"{{{docket.name}}}"

        # Verify all key methods use hash_tag prefix
        hash_tag = docket.hash_tag
        assert docket.queue_key == f"{hash_tag}:queue"
        assert docket.stream_key == f"{hash_tag}:stream"
        assert docket.workers_set == f"{hash_tag}:workers"
        assert docket.runs_key("test") == f"{hash_tag}:runs:test"
        assert docket.progress_key("test") == f"{hash_tag}:progress:test"
        assert docket.parked_task_key("test") == f"{hash_tag}:test"
        assert docket.known_task_key("test") == f"{hash_tag}:known:test"
        assert docket.stream_id_key("test") == f"{hash_tag}:stream-id:test"

    @pytest.mark.asyncio
    async def test_immediate_task(self, docket: Docket, worker: Worker) -> None:
        """Should be able to schedule and execute immediate tasks on cluster."""
        executed = asyncio.Event()

        async def immediate_task() -> str:
            executed.set()
            return "cluster-result"

        docket.register(immediate_task)
        execution = await docket.add(immediate_task)()

        await worker.run_until_finished()

        assert executed.is_set()
        # Sync execution to get updated state from Redis
        await execution.sync()
        assert execution.state.value == "completed"

    @pytest.mark.asyncio
    async def test_scheduled_task(self, docket: Docket, worker: Worker) -> None:
        """Should be able to schedule and execute future tasks on cluster."""
        executed = asyncio.Event()

        async def scheduled_task() -> str:
            executed.set()
            return "scheduled-result"

        docket.register(scheduled_task)

        # Schedule for 100ms in the future
        when = datetime.now(timezone.utc) + timedelta(milliseconds=100)
        execution = await docket.add(scheduled_task, when=when)()

        await worker.run_until_finished()

        assert executed.is_set()
        # Sync execution to get updated state from Redis
        await execution.sync()
        assert execution.state.value == "completed"

    @pytest.mark.asyncio
    async def test_task_with_args(self, docket: Docket, worker: Worker) -> None:
        """Should be able to pass arguments to tasks on cluster."""
        result_holder: list[tuple[str, int, str]] = []

        async def task_with_args(a: str, b: int, c: str = "default") -> None:
            result_holder.append((a, b, c))

        docket.register(task_with_args)
        await docket.add(task_with_args)("hello", 42, c="world")

        await worker.run_until_finished()

        assert result_holder == [("hello", 42, "world")]

    @pytest.mark.asyncio
    async def test_task_cancellation(self, docket: Docket, worker: Worker) -> None:
        """Should be able to cancel tasks on cluster."""

        async def cancellable_task() -> None:
            await asyncio.sleep(10)

        docket.register(cancellable_task)

        # Schedule for the future
        when = datetime.now(timezone.utc) + timedelta(seconds=10)
        execution = await docket.add(cancellable_task, when=when)()

        # Cancel before execution
        await docket.cancel(execution.key)

        await worker.run_until_finished()

        # Verify cancellation
        retrieved = await docket.get_execution(execution.key)
        assert retrieved is not None
        assert retrieved.state.value == "cancelled"


class TestClusterMultiKeyOperations:
    """Tests for multi-key operations that require hash slot consistency."""

    @pytest.fixture
    async def docket(self) -> AsyncGenerator[Docket, None]:
        """Create a Docket instance connected to the cluster with unique name."""
        unique_name = f"multikey-test-{uuid.uuid4().hex[:8]}"
        async with Docket(
            name=unique_name,
            url=CLUSTER_URL,
        ) as docket:
            yield docket
            # Cleanup: delete all keys for this docket
            client = await get_cluster_client(CLUSTER_URL)
            keys: list[bytes] = []
            async for key in client.scan_iter(match=f"{{{unique_name}}}:*"):  # type: ignore[union-attr]
                keys.append(key)  # type: ignore[arg-type]
            if keys:
                await client.delete(*keys)  # type: ignore[arg-type]

    @pytest.fixture
    async def worker(self, docket: Docket) -> AsyncGenerator[Worker, None]:
        """Create a Worker for the cluster-connected Docket."""
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            yield worker

    @pytest.mark.asyncio
    async def test_scheduler_lua_script(self, docket: Docket, worker: Worker) -> None:
        """Scheduler Lua script should work with cluster (all keys in same slot)."""
        executed_count = 0

        async def scheduled_task() -> None:
            nonlocal executed_count
            executed_count += 1

        docket.register(scheduled_task)

        # Schedule multiple tasks for immediate execution
        for i in range(5):
            when = datetime.now(timezone.utc) + timedelta(milliseconds=10 * (i + 1))
            await docket.add(scheduled_task, when=when, key=f"task-{i}")()

        await worker.run_until_finished()

        assert executed_count == 5

    @pytest.mark.asyncio
    async def test_replace_task(self, docket: Docket, worker: Worker) -> None:
        """Replace operation should work with cluster (multi-key Lua script)."""
        executions: list[str] = []

        async def replaceable_task(value: str) -> None:
            executions.append(value)

        docket.register(replaceable_task)

        # Schedule initial task
        when = datetime.now(timezone.utc) + timedelta(milliseconds=500)
        await docket.add(replaceable_task, when=when, key="replaceable")("first")

        # Replace with new task
        when = datetime.now(timezone.utc) + timedelta(milliseconds=100)
        await docket.replace(replaceable_task, when=when, key="replaceable")("second")

        await worker.run_until_finished()

        # Only the second value should be executed
        assert executions == ["second"]

    @pytest.mark.asyncio
    async def test_strike_and_restore(self, docket: Docket, worker: Worker) -> None:
        """Strike and restore should work with cluster."""
        executions: list[str] = []

        async def strikeable_task(customer_id: str) -> None:
            executions.append(customer_id)

        docket.register(strikeable_task)

        # Strike a specific customer
        await docket.strike(strikeable_task, "customer_id", "==", "blocked")

        # Schedule tasks for different customers
        await docket.add(strikeable_task)("blocked")
        await docket.add(strikeable_task)("allowed")

        await worker.run_until_finished()

        # Only the allowed customer should execute
        assert "allowed" in executions
        assert "blocked" not in executions

        # Restore and try again
        await docket.restore(strikeable_task, "customer_id", "==", "blocked")

        await docket.add(strikeable_task)("blocked")
        await worker.run_until_finished()

        assert "blocked" in executions


class TestClusterWorkerOperations:
    """Tests for worker operations with cluster."""

    @pytest.fixture
    async def docket(self) -> AsyncGenerator[Docket, None]:
        """Create a Docket instance connected to the cluster with unique name."""
        unique_name = f"worker-test-{uuid.uuid4().hex[:8]}"
        async with Docket(
            name=unique_name,
            url=CLUSTER_URL,
        ) as docket:
            yield docket
            # Cleanup: delete all keys for this docket
            client = await get_cluster_client(CLUSTER_URL)
            keys: list[bytes] = []
            async for key in client.scan_iter(match=f"{{{unique_name}}}:*"):  # type: ignore[union-attr]
                keys.append(key)  # type: ignore[arg-type]
            if keys:
                await client.delete(*keys)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_multiple_workers(self, docket: Docket) -> None:
        """Multiple workers should be able to process tasks on cluster."""
        executed_by: dict[str, str] = {}
        lock = asyncio.Lock()

        async def worker_task(task_id: str) -> None:
            # Simulate work
            await asyncio.sleep(0.01)
            async with lock:
                # Get worker name from execution context
                executed_by[task_id] = "worker"

        docket.register(worker_task)

        # Create multiple workers
        async with (
            Worker(docket, name="worker-1") as worker1,
            Worker(docket, name="worker-2") as worker2,
        ):
            # Schedule tasks
            for i in range(10):
                await docket.add(worker_task)(f"task-{i}")

            # Run both workers concurrently
            await asyncio.gather(
                worker1.run_until_finished(),
                worker2.run_until_finished(),
            )

        # All tasks should be executed
        assert len(executed_by) == 10

    @pytest.mark.asyncio
    async def test_worker_heartbeat(self, docket: Docket) -> None:
        """Worker heartbeat should work with cluster."""

        async def dummy_task() -> None:
            pass

        docket.register(dummy_task)

        async with Worker(docket, name="heartbeat-worker") as worker:
            await docket.add(dummy_task)()

            # Wait for heartbeat
            await asyncio.sleep(0.5)

            # Check workers list
            workers = await docket.workers()
            assert any(w.name == "heartbeat-worker" for w in workers)

            await worker.run_until_finished()
