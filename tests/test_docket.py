from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import AsyncMock

import pytest
import redis.exceptions

from docket.docket import Docket
from docket.state import ProgressInfo, TaskStateStore


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


async def test_get_progress_nonexistent(docket: Docket):
    """Getting progress for nonexistent task should return None."""
    progress = await docket.get_progress("nonexistent-key")
    assert progress is None


async def test_get_progress(docket: Docket, the_task: AsyncMock):
    """Getting progress for a task should return ProgressInfo."""
    docket.register(the_task)
    execution = await docket.add(the_task, key="test-key")()

    # Create progress for this task
    store = TaskStateStore(docket, docket.record_ttl)
    await store.create_task_state(execution.key)
    await store.set_task_progress(execution.key, ProgressInfo(current=50, total=100))

    # Get progress via docket method
    progress = await docket.get_progress(execution.key)
    assert progress is not None
    assert progress.current == 50
    assert progress.total == 100


async def test_snapshot_with_progress(docket: Docket, the_task: AsyncMock):
    """Snapshot should include progress info when available."""
    docket.register(the_task)
    execution = await docket.add(the_task, key="test-key")()

    # Create progress for this task
    store = TaskStateStore(docket, docket.record_ttl)
    await store.create_task_state(execution.key)
    await store.set_task_progress(execution.key, ProgressInfo(current=75, total=100))

    # Get snapshot
    snapshot = await docket.snapshot()

    # Find our execution in the snapshot
    found = False
    for exec in snapshot.future:
        if exec.key == execution.key:  # pragma: no cover
            found = True
            assert exec.progress is not None
            assert exec.progress.current == 75
            assert exec.progress.total == 100
            break

    assert found, "Execution with progress should be in snapshot"
