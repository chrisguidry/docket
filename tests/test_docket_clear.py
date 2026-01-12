"""Tests for docket.clear() and stream/consumer group bootstrap."""

from datetime import datetime, timedelta, timezone
from typing import Callable, cast
from unittest.mock import AsyncMock

import pytest

from docket.docket import Docket


# Tests for docket.clear()


async def test_clear_empty_docket(docket: Docket):
    """Clearing an empty docket should succeed without error"""
    result = await docket.clear()
    assert result == 0


async def test_clear_with_immediate_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear immediate tasks from the stream"""
    docket.register(the_task)

    await docket.add(the_task)("arg1", kwarg1="value1")
    await docket.add(the_task)("arg2", kwarg1="value2")
    await docket.add(the_task)("arg3", kwarg1="value3")

    snapshot_before = await docket.snapshot()
    assert len(snapshot_before.future) == 3

    result = await docket.clear()
    assert result == 3

    snapshot_after = await docket.snapshot()
    assert len(snapshot_after.future) == 0
    assert len(snapshot_after.running) == 0


async def test_clear_with_scheduled_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear scheduled future tasks from the queue"""
    docket.register(the_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future)("arg1")
    await docket.add(the_task, when=future + timedelta(seconds=1))("arg2")

    snapshot_before = await docket.snapshot()
    assert len(snapshot_before.future) == 2

    result = await docket.clear()
    assert result == 2

    snapshot_after = await docket.snapshot()
    assert len(snapshot_after.future) == 0
    assert len(snapshot_after.running) == 0


async def test_clear_with_mixed_tasks(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    """Should clear both immediate and scheduled tasks"""
    docket.register(the_task)
    docket.register(another_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)

    await docket.add(the_task)("immediate1")
    await docket.add(another_task)("immediate2")
    await docket.add(the_task, when=future)("scheduled1")
    await docket.add(another_task, when=future + timedelta(seconds=1))("scheduled2")

    snapshot_before = await docket.snapshot()
    assert len(snapshot_before.future) == 4

    result = await docket.clear()
    assert result == 4

    snapshot_after = await docket.snapshot()
    assert len(snapshot_after.future) == 0
    assert len(snapshot_after.running) == 0


async def test_clear_with_parked_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear parked tasks (tasks with specific keys)"""
    docket.register(the_task)

    await docket.add(the_task, key="task1")("arg1")
    await docket.add(the_task, key="task2")("arg2")

    snapshot_before = await docket.snapshot()
    assert len(snapshot_before.future) == 2

    result = await docket.clear()
    assert result == 2

    snapshot_after = await docket.snapshot()
    assert len(snapshot_after.future) == 0


async def test_clear_preserves_strikes(docket: Docket, the_task: AsyncMock):
    """Should not affect strikes when clearing"""
    docket.register(the_task)

    await docket.strike("the_task")
    await docket.add(the_task)("arg1")

    # Check that the task wasn't scheduled due to the strike
    snapshot_before = await docket.snapshot()
    assert len(snapshot_before.future) == 0  # Task was stricken, so not scheduled

    result = await docket.clear()
    assert result == 0  # Nothing to clear since task was stricken

    # Strikes should still be in effect - clear doesn't affect strikes
    snapshot_after = await docket.snapshot()
    assert len(snapshot_after.future) == 0


async def test_clear_returns_total_count(docket: Docket, the_task: AsyncMock):
    """Should return the total number of tasks cleared"""
    docket.register(the_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)

    await docket.add(the_task)("immediate1")
    await docket.add(the_task)("immediate2")
    await docket.add(the_task, when=future)("scheduled1")
    await docket.add(the_task, key="keyed1")("keyed1")

    result = await docket.clear()
    assert result == 4


async def test_clear_no_redis_key_leaks(docket: Docket, the_task: AsyncMock):
    """Should not leak Redis keys when clearing tasks"""
    docket.register(the_task)

    await docket.add(the_task)("immediate1")
    await docket.add(the_task)("immediate2")
    await docket.add(the_task, key="keyed1")("keyed_task")

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future)("scheduled1")
    await docket.add(the_task, when=future + timedelta(seconds=1))("scheduled2")

    async with docket.redis() as r:
        keys_before = cast(list[str], await r.keys("*"))  # type: ignore
        keys_before_count = len(keys_before)

    result = await docket.clear()
    assert result == 5

    async with docket.redis() as r:
        keys_after = cast(list[str], await r.keys("*"))  # type: ignore
        keys_after_count = len(keys_after)

    assert keys_after_count <= keys_before_count

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0
    assert len(snapshot.running) == 0


async def test_clear_with_execution_ttl_zero(the_task: AsyncMock):
    """Should delete runs hashes immediately when execution_ttl=0."""
    async with Docket(
        name="test-docket-ttl-zero", url="memory://", execution_ttl=timedelta(0)
    ) as docket:
        docket.register(the_task)

        # Add both immediate and scheduled tasks
        await docket.add(the_task, key="immediate1")("arg1")
        future = datetime.now(timezone.utc) + timedelta(seconds=60)
        await docket.add(the_task, when=future, key="scheduled1")("arg2")

        result = await docket.clear()
        assert result == 2

        # Verify runs hashes were deleted (not just expired)
        async with docket.redis() as redis:
            immediate_runs = await redis.exists(f"{docket.name}:runs:immediate1")
            scheduled_runs = await redis.exists(f"{docket.name}:runs:scheduled1")
            assert immediate_runs == 0
            assert scheduled_runs == 0


# Tests for stream/consumer group bootstrap


async def test_stream_not_created_on_docket_init(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Stream and consumer group should NOT be created when Docket is initialized.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)
    async with docket:
        async with docket.redis() as redis:
            stream_exists = await redis.exists(docket.stream_key)
            assert not stream_exists, "Stream should not exist on Docket init"


async def test_ensure_stream_and_group_is_idempotent(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Calling _ensure_stream_and_group multiple times should not raise errors.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)
    async with docket:
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1
            assert groups[0]["name"] == docket.worker_group_name.encode()


async def test_docket_without_worker_does_not_create_group(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """A Docket used only for adding tasks should not create consumer group.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)

    async def dummy_task(): ...

    async with docket:
        docket.register(dummy_task)

        for _ in range(5):
            await docket.add(dummy_task)()

        async with docket.redis() as redis:
            assert await redis.exists(docket.stream_key)
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0, "Consumer group should not exist without worker"


@pytest.mark.parametrize("redis_url", ["real"], indirect=True)
async def test_snapshot_handles_nogroup_with_real_redis(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Snapshot should handle NOGROUP error and create group automatically.

    Issue #206: Lazy stream/consumer group bootstrap.

    This test uses real Redis (not memory://) to verify the NOGROUP error
    handling path in snapshot(), since the memory:// backend proactively
    creates the group to work around a fakeredis bug.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)

    async def dummy_task(): ...

    async with docket:
        docket.register(dummy_task)

        # Add a task to create the stream (but not the consumer group)
        await docket.add(dummy_task)()

        # Verify stream exists but group doesn't
        async with docket.redis() as redis:
            assert await redis.exists(docket.stream_key)
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0

        # Calling snapshot() should trigger NOGROUP and handle it
        snapshot = await docket.snapshot()

        # Snapshot should succeed after creating the group
        assert snapshot.total_tasks == 1

        # Group should now exist
        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1
