from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import AsyncMock

import pytest
import redis.exceptions

from docket.docket import Docket


async def test_docket_aenter_propagates_connection_errors():
    """The docket should propagate Redis connection errors"""

    docket = Docket(name="test-docket", url="redis://nonexistent-host:12345/0")
    with pytest.raises(redis.exceptions.RedisError):
        await docket.__aenter__()

    await docket.__aexit__(None, None, None)


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
