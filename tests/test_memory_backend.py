from unittest.mock import patch

import pytest

from docket import Docket, Worker

# Skip all tests in this file if fakeredis is not installed
pytest.importorskip("fakeredis")


async def test_docket_memory_backend():
    """Test using in-memory backend via memory:// URL."""
    async with Docket(name="test-memory-docket", url="memory://test") as docket:
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


async def test_docket_memory_backend_missing_dependency():
    """Test that using memory:// URL without fakeredis installed raises helpful error."""
    # Mock the import to simulate fakeredis not being installed
    with patch.dict("sys.modules", {"fakeredis.aioredis": None}):
        with pytest.raises(
            ImportError, match="fakeredis is required for memory:// URLs"
        ):
            async with Docket(name="test-memory-docket", url="memory://test"):
                pass  # pragma: no cover


async def test_multiple_memory_dockets():
    """Test that multiple in-memory dockets can coexist with separate data."""
    async with (
        Docket(name="docket-1", url="memory://one") as docket1,
        Docket(name="docket-2", url="memory://two") as docket2,
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
    """Test that dockets with the same memory:// URL share the same FakeServer."""
    result = None

    async def shared_task(value: str) -> str:
        nonlocal result
        result = value
        return value

    # Create first docket and run a task
    async with Docket(name="docket-shared", url="memory://shared") as docket1:
        docket1.register(shared_task)
        await docket1.add(shared_task)("shared-value")

        async with Worker(docket1, concurrency=1) as worker:
            await worker.run_until_finished()

        assert result == "shared-value"

    # Now create another docket with the same URL - should reuse the server
    async with Docket(name="docket-shared-2", url="memory://shared") as docket2:
        # The server should be reused (hitting the cached branch in docket.py)
        # Verify we can still interact with the shared server
        snapshot = await docket2.snapshot()
        assert snapshot.total_tasks == 0  # All tasks from docket1 are complete
