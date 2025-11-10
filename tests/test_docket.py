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


async def test_docket_schedule_method_with_immediate_task(
    docket: Docket, the_task: AsyncMock
):
    """Test direct scheduling via docket.schedule(execution) for immediate execution."""
    from docket import Execution

    # Register task so snapshot can look it up
    docket.register(the_task)

    execution = Execution(
        docket, the_task, ("arg",), {}, "test-key", datetime.now(timezone.utc), 1
    )

    await docket.schedule(execution)

    # Verify task was scheduled
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 1


async def test_docket_schedule_with_stricken_task(docket: Docket, the_task: AsyncMock):
    """Test that docket.schedule respects strike list."""
    from docket import Execution

    # Register task
    docket.register(the_task)

    # Strike the task
    await docket.strike("the_task")

    execution = Execution(
        docket, the_task, (), {}, "test-key", datetime.now(timezone.utc), 1
    )

    # Try to schedule - should be blocked
    await docket.schedule(execution)

    # Verify task was NOT scheduled
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0


async def test_get_execution_nonexistent_key(docket: Docket):
    """get_execution should return None for non-existent key."""
    execution = await docket.get_execution("nonexistent-key")
    assert execution is None


async def test_get_execution_for_scheduled_task(docket: Docket, the_task: AsyncMock):
    """get_execution should return execution for scheduled task with correct data."""
    docket.register(the_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future, key="scheduled-task")(
        "arg1", kwarg1="value1"
    )

    execution = await docket.get_execution("scheduled-task")
    assert execution is not None
    assert execution.key == "scheduled-task"
    assert execution.function == the_task
    assert execution.args == ("arg1",)
    assert execution.kwargs == {"kwarg1": "value1"}


async def test_get_execution_for_queued_task(docket: Docket, the_task: AsyncMock):
    """get_execution should return execution for immediate (queued) task."""
    docket.register(the_task)

    await docket.add(the_task, key="immediate-task")("arg1", kwarg1="value1")

    execution = await docket.get_execution("immediate-task")
    assert execution is not None
    assert execution.key == "immediate-task"
    assert execution.function == the_task
    assert execution.args == ("arg1",)
    assert execution.kwargs == {"kwarg1": "value1"}


async def test_get_execution_function_not_registered(
    docket: Docket, the_task: AsyncMock
):
    """get_execution should create placeholder when function not registered in current docket."""
    # Schedule a task with the function registered
    docket.register(the_task)
    await docket.add(the_task, key="task-key")("arg1")

    # Create a new docket instance that doesn't have the task registered
    # (simulates CLI accessing a task without having all functions imported)
    async with Docket(name=docket.name, url=docket.url) as new_docket:
        # Try to get execution without having the function registered
        # Should return execution with placeholder function
        execution = await new_docket.get_execution("task-key")
        assert execution is not None
        assert execution.function.__name__ == "the_task"
        assert execution.args == ("arg1",)


async def test_get_execution_with_complex_args(docket: Docket, the_task: AsyncMock):
    """get_execution should handle complex args and kwargs."""
    docket.register(the_task)

    complex_arg = {"nested": {"data": [1, 2, 3]}, "key": "value"}
    complex_kwarg = {"items": [{"id": 1}, {"id": 2}]}

    await docket.add(the_task, key="complex-task")(complex_arg, data=complex_kwarg)

    execution = await docket.get_execution("complex-task")
    assert execution is not None
    assert execution.args == (complex_arg,)
    assert execution.kwargs == {"data": complex_kwarg}


async def test_get_execution_claim_check_pattern(docket: Docket, the_task: AsyncMock):
    """Demonstrate the claim check pattern: schedule task, get key, retrieve later."""
    docket.register(the_task)

    # Schedule a task and get the key
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    original_execution = await docket.add(
        the_task, when=future, key="claim-check-task"
    )("important-data", priority="high")
    task_key = original_execution.key

    # Later, retrieve the execution using just the key
    retrieved_execution = await docket.get_execution(task_key)
    assert retrieved_execution is not None
    assert retrieved_execution.key == task_key
    assert retrieved_execution.function == the_task
    assert retrieved_execution.args == ("important-data",)
    assert retrieved_execution.kwargs == {"priority": "high"}


async def test_get_execution_with_incomplete_data(docket: Docket):
    """get_execution should return None when runs hash has incomplete data."""
    # Manually create runs hash with missing fields
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:incomplete-task"
        # Only set state, missing function/args/kwargs
        await redis.hset(runs_key, mapping={"state": "scheduled"})  # type: ignore[misc]

    execution = await docket.get_execution("incomplete-task")
    assert execution is None


async def test_get_execution_with_missing_when(docket: Docket, the_task: AsyncMock):
    """get_execution should return None when runs hash is missing when field."""
    import cloudpickle  # type: ignore[import-untyped]

    docket.register(the_task)

    # Manually create runs hash with function/args/kwargs but no when
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:no-when-task"
        await redis.hset(  # type: ignore[misc]
            runs_key,
            mapping={
                "state": "scheduled",
                "function": "the_task",
                "args": cloudpickle.dumps(()),  # type: ignore[attr-defined]
                "kwargs": cloudpickle.dumps({}),  # type: ignore[attr-defined]
                # Missing "when" field
            },
        )

    execution = await docket.get_execution("no-when-task")
    assert execution is None


async def test_get_execution_with_unregistered_function_creates_placeholder(
    docket: Docket,
):
    """get_execution should create placeholder function when not registered."""
    import cloudpickle  # type: ignore[import-untyped]

    # Manually create runs hash with unregistered function
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:unregistered-task"
        await redis.hset(  # type: ignore[misc]
            runs_key,
            mapping={
                "state": "scheduled",
                "function": "unknown_function",
                "args": cloudpickle.dumps(("arg1",)),  # type: ignore[attr-defined]
                "kwargs": cloudpickle.dumps({"key": "value"}),  # type: ignore[attr-defined]
                "when": str(datetime.now(timezone.utc).timestamp()),
            },
        )

    execution = await docket.get_execution("unregistered-task")
    assert execution is not None
    assert execution.function.__name__ == "unknown_function"
    assert execution.args == ("arg1",)
    assert execution.kwargs == {"key": "value"}


async def test_get_execution_fallback_to_parked_hash(
    docket: Docket, the_task: AsyncMock
):
    """get_execution should fallback to parked hash for 0.13.0 compatibility."""
    import cloudpickle  # type: ignore[import-untyped]

    docket.register(the_task)

    # Simulate a 0.13.0 task: runs hash without function/args/kwargs, data in parked hash
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:legacy-task"
        parked_key = docket.parked_task_key("legacy-task")
        when = datetime.now(timezone.utc)

        # Old style runs hash (0.13.0) - no function/args/kwargs
        await redis.hset(  # type: ignore[misc]
            runs_key,
            mapping={
                "state": "scheduled",
                "when": str(when.timestamp()),
                "known": str(when.timestamp()),
            },
        )

        # Task data in parked hash (0.13.0 behavior)
        await redis.hset(  # type: ignore[misc]
            parked_key,
            mapping={
                "key": "legacy-task",
                "function": "the_task",
                "args": cloudpickle.dumps(("legacy-arg",)),  # type: ignore[attr-defined]
                "kwargs": cloudpickle.dumps({"legacy": "kwarg"}),  # type: ignore[attr-defined]
                "when": when.isoformat(),
                "attempt": "1",
            },
        )

    # Should successfully retrieve execution using parked hash fallback
    execution = await docket.get_execution("legacy-task")
    assert execution is not None
    assert execution.function == the_task
    assert execution.args == ("legacy-arg",)
    assert execution.kwargs == {"legacy": "kwarg"}
