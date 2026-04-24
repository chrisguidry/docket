# pyright: reportUnknownVariableType=false

import asyncio

from docket import Docket, Worker
from docket._redis import (
    MemoryRedisClient,
    RedisConnection,
    clear_memory_servers,
    get_memory_server,
)


async def _get_memory_client(url: str) -> MemoryRedisClient:
    async with RedisConnection(url) as connection:
        assert connection.memory_client is not None
        return connection.memory_client


async def test_docket_memory_backend():
    """Test using in-memory backend via memory:// URL."""
    async with Docket(name="test-memory-docket", url="memory://") as docket:
        result_value = None

        async def simple_task(value: str) -> str:
            nonlocal result_value
            result_value = value
            return value

        docket.register(simple_task)

        # Add and run a task
        execution = await docket.add(simple_task)("test-value")
        assert execution.key

        # Run the task with a worker to exercise the memory backend polling path
        async with Worker(docket, concurrency=1) as worker:
            await worker.run_until_finished()

        # Verify the task actually ran
        assert result_value == "test-value"

        # Verify snapshot works
        snapshot = await docket.snapshot()
        assert snapshot.total_tasks == 0  # All tasks should be done


async def test_multiple_memory_dockets():
    """Test that multiple in-memory dockets can coexist with separate data."""
    async with (
        Docket(name="docket-1", url="memory://") as docket1,
        Docket(name="docket-2", url="memory://") as docket2,
    ):
        result1 = None
        result2 = None

        async def task1(value: str) -> str:
            nonlocal result1
            result1 = value
            return value

        async def task2(value: str) -> str:
            nonlocal result2
            result2 = value
            return value

        docket1.register(task1)
        docket2.register(task2)

        # Add tasks to separate dockets
        await docket1.add(task1)("docket1-value")
        await docket2.add(task2)("docket2-value")

        # Run workers for each docket
        async with Worker(docket1, concurrency=1) as worker1:
            await worker1.run_until_finished()

        async with Worker(docket2, concurrency=1) as worker2:
            await worker2.run_until_finished()

        # Verify tasks ran with correct values
        assert result1 == "docket1-value"
        assert result2 == "docket2-value"


async def test_memory_backend_reuses_server():
    """Test that identical memory:// URLs share the same BurnerRedis instance."""
    result = None

    async def shared_task(value: str) -> str:
        nonlocal result
        result = value
        return value

    # Create first docket and run a task
    async with Docket(name="docket-shared", url="memory://") as docket1:
        docket1.register(shared_task)
        await docket1.add(shared_task)("shared-value")

        async with Worker(docket1, concurrency=1) as worker:
            await worker.run_until_finished()

        assert result == "shared-value"

    # Create another docket - identical memory:// URLs share the same server
    # but dockets are isolated by name-based key prefixes
    async with Docket(name="docket-shared-2", url="memory://") as docket2:
        # Verify we can interact with the shared server
        # This docket's tasks are isolated from docket1's by name prefix
        snapshot = await docket2.snapshot()
        assert snapshot.total_tasks == 0


def test_memory_backend_cache_is_scoped_to_event_loop():
    """Identical memory:// URLs reuse clients only within the same event loop."""
    url = "memory://loop-scoped"
    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()
    client2 = None

    try:
        client1 = loop1.run_until_complete(_get_memory_client(url))
        client1_again = loop1.run_until_complete(_get_memory_client(url))
        client2 = loop2.run_until_complete(_get_memory_client(url))

        assert client1_again is client1
        assert client2 is not client1
    finally:
        loop1.run_until_complete(clear_memory_servers())
        if client2 is not None:  # pragma: no branch
            loop2.run_until_complete(client2.aclose())
        loop1.close()
        loop2.close()


def test_memory_backend_recreates_server_after_event_loop_closes():
    """A cached memory client from a closed loop is not reused."""
    url = "memory://closed-loop"
    loop1 = asyncio.new_event_loop()
    client1 = loop1.run_until_complete(_get_memory_client(url))
    loop1.run_until_complete(client1.aclose())
    loop1.close()

    loop2 = asyncio.new_event_loop()
    try:
        client2 = loop2.run_until_complete(_get_memory_client(url))

        assert client2 is not client1
    finally:
        loop2.run_until_complete(clear_memory_servers())
        loop2.close()


async def test_different_memory_urls_are_isolated():
    """Test that different memory:// URLs get completely separate BurnerRedis instances."""
    result1 = None
    result2 = None

    async def task_for_server1(value: str) -> str:
        nonlocal result1
        result1 = value
        return value

    async def task_for_server2(value: str) -> str: ...

    # Create two dockets with different memory:// URLs
    async with (
        Docket(name="test", url="memory://server1") as docket1,
        Docket(name="test", url="memory://server2") as docket2,
    ):
        docket1.register(task_for_server1)
        docket2.register(task_for_server2)

        # Add task only to server1
        await docket1.add(task_for_server1)("value-for-server1")

        # Verify server2 sees no tasks (they're on different BurnerRedis instances)
        snapshot2 = await docket2.snapshot()
        assert snapshot2.total_tasks == 0, "server2 should have no tasks"

        # Verify server1 has the task
        snapshot1 = await docket1.snapshot()
        assert snapshot1.total_tasks == 1, "server1 should have one task"

        # Run the worker for server1
        async with Worker(docket1, concurrency=1) as worker1:
            await worker1.run_until_finished()

        assert result1 == "value-for-server1"
        assert result2 is None  # task_for_server2 was never called

    # Verify we created two separate BurnerRedis instances
    server1 = get_memory_server("memory://server1")
    server2 = get_memory_server("memory://server2")
    assert server1 is not None
    assert server2 is not None
    assert server1 is not server2


async def test_memory_url_with_path_isolation():
    """Test that memory:// URLs with different paths are isolated."""
    async with (
        Docket(name="test", url="memory://localhost/db1") as docket1,
        Docket(name="test", url="memory://localhost/db2") as docket2,
    ):

        async def dummy_task() -> None: ...

        docket1.register(dummy_task)

        # Add task to db1
        await docket1.add(dummy_task)()

        # db2 should be empty (different server)
        snapshot2 = await docket2.snapshot()
        assert snapshot2.total_tasks == 0

        # db1 should have the task
        snapshot1 = await docket1.snapshot()
        assert snapshot1.total_tasks == 1

    db1_server = get_memory_server("memory://localhost/db1")
    db2_server = get_memory_server("memory://localhost/db2")
    assert db1_server is not None
    assert db2_server is not None
    assert db1_server is not db2_server
