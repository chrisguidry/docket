import asyncio
from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import AsyncMock

import pytest
import redis.exceptions

from docket.docket import Docket
from docket.execution import ExecutionState
from docket.worker import Worker
from tests._key_leak_checker import KeyCountChecker


class TestPrefixAndKeyMethods:
    """Tests for the prefix property and key() method."""

    def test_prefix_returns_name(self):
        """prefix property should return the docket name."""
        docket = Docket(name="my-docket", url="memory://")
        assert docket.prefix == "my-docket"

    def test_key_builds_correct_key(self):
        """key() should build keys with the prefix."""
        docket = Docket(name="my-docket", url="memory://")
        assert docket.key("queue") == "my-docket:queue"
        assert docket.key("stream") == "my-docket:stream"
        assert docket.key("runs:task-123") == "my-docket:runs:task-123"

    def test_queue_key_uses_key_method(self):
        """queue_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.queue_key == "test:queue"

    def test_stream_key_uses_key_method(self):
        """stream_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.stream_key == "test:stream"

    def test_workers_set_uses_key_method(self):
        """workers_set should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.workers_set == "test:workers"

    def test_known_task_key_uses_key_method(self):
        """known_task_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.known_task_key("task-123") == "test:known:task-123"

    def test_parked_task_key_uses_key_method(self):
        """parked_task_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.parked_task_key("task-123") == "test:task-123"

    def test_stream_id_key_uses_key_method(self):
        """stream_id_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.stream_id_key("task-123") == "test:stream-id:task-123"

    def test_runs_key_uses_key_method(self):
        """runs_key should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.runs_key("task-123") == "test:runs:task-123"

    def test_cancel_channel_uses_key_method(self):
        """cancel_channel should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.cancel_channel("task-123") == "test:cancel:task-123"

    def test_results_collection_uses_key_method(self):
        """results_collection should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.results_collection == "test:results"

    def test_worker_tasks_set_uses_key_method(self):
        """worker_tasks_set should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.worker_tasks_set("worker-1") == "test:worker-tasks:worker-1"

    def test_task_workers_set_uses_key_method(self):
        """task_workers_set should use the key() method."""
        docket = Docket(name="test", url="memory://")
        assert docket.task_workers_set("my_task") == "test:task-workers:my_task"


async def test_docket_propagates_connection_errors_on_operation():
    """Connection errors should propagate when operations are attempted."""
    docket = Docket(name="test-docket", url="redis://nonexistent-host:12345/0")

    # __aenter__ succeeds because it doesn't actually connect to Redis
    # (connection is lazy - happens when operations are performed)
    await docket.__aenter__()

    # But actual operations should fail with connection errors
    async def some_task(): ...

    docket.register(some_task)
    with pytest.raises(redis.exceptions.RedisError):
        await docket.add(some_task)()

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


async def test_get_execution_with_incomplete_data(
    docket: Docket, key_leak_checker: KeyCountChecker
):
    """get_execution should return None when runs hash has incomplete data."""
    # This test manually creates incomplete test data
    key_leak_checker.add_exemption(f"{docket.name}:runs:incomplete-task")

    # Manually create runs hash with missing fields
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:incomplete-task"
        # Only set state, missing function/args/kwargs
        await redis.hset(runs_key, mapping={"state": "scheduled"})  # type: ignore[misc]

    execution = await docket.get_execution("incomplete-task")
    assert execution is None


async def test_get_execution_with_missing_when(
    docket: Docket, the_task: AsyncMock, key_leak_checker: KeyCountChecker
):
    """get_execution should return None when runs hash is missing when field."""
    import cloudpickle  # type: ignore[import-untyped]

    docket.register(the_task)

    # This test manually creates incomplete test data
    key_leak_checker.add_exemption(f"{docket.name}:runs:no-when-task")

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
    key_leak_checker: KeyCountChecker,
):
    """get_execution should create placeholder function when not registered."""
    import cloudpickle  # type: ignore[import-untyped]

    # This test manually creates incomplete test data
    key_leak_checker.add_exemption(f"{docket.name}:runs:unregistered-task")

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


async def test_cancelled_state_creates_tombstone(docket: Docket, the_task: AsyncMock):
    """Cancelling a task should create a tombstone with CANCELLED state."""
    docket.register(the_task)

    # Schedule a future task
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    execution = await docket.add(the_task, when=future, key="task-to-cancel")(
        "arg1", kwarg1="value1"
    )

    # Cancel the task
    await docket.cancel(execution.key)

    # Retrieve execution - should have CANCELLED state
    retrieved = await docket.get_execution(execution.key)
    assert retrieved is not None
    assert retrieved.state == ExecutionState.CANCELLED
    assert retrieved.key == "task-to-cancel"
    assert retrieved.function == the_task
    assert retrieved.args == ("arg1",)
    assert retrieved.kwargs == {"kwarg1": "value1"}


async def test_cancelled_state_respects_ttl(docket: Docket, the_task: AsyncMock):
    """Cancelled task tombstone should have TTL set from execution_ttl."""
    docket.register(the_task)

    # Schedule a task
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    execution = await docket.add(the_task, when=future, key="ttl-task")("test")

    # Cancel the task
    await docket.cancel(execution.key)

    # Check that the runs hash has TTL set
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:{execution.key}"
        ttl = await redis.ttl(runs_key)

        # TTL should be set (not -1 which means no expiry)
        # Should be close to execution_ttl (default 15 minutes = 900 seconds)
        assert ttl > 0
        assert ttl <= int(docket.execution_ttl.total_seconds())


async def test_cancelled_state_with_ttl_zero(docket: Docket, the_task: AsyncMock):
    """Cancelled task with execution_ttl=0 should delete tombstone immediately."""
    # Create docket with TTL=0
    async with Docket(
        name=f"{docket.name}-ttl-zero",
        url=docket.url,
        execution_ttl=timedelta(0),
    ) as zero_ttl_docket:
        zero_ttl_docket.register(the_task)

        # Schedule and cancel a task
        future = datetime.now(timezone.utc) + timedelta(seconds=60)
        execution = await zero_ttl_docket.add(
            the_task, when=future, key="zero-ttl-task"
        )("test")
        await zero_ttl_docket.cancel(execution.key)

        # Tombstone should be deleted immediately
        retrieved = await zero_ttl_docket.get_execution(execution.key)
        assert retrieved is None


async def test_get_execution_after_cancel(docket: Docket, the_task: AsyncMock):
    """get_execution should retrieve cancelled task state."""
    docket.register(the_task)

    # Schedule task
    execution = await docket.add(the_task, key="cancelled-task")("data")

    # Cancel it
    await docket.cancel(execution.key)

    # Should be able to retrieve it with CANCELLED state
    retrieved = await docket.get_execution("cancelled-task")
    assert retrieved is not None
    assert retrieved.state == ExecutionState.CANCELLED
    assert retrieved.key == "cancelled-task"


async def test_replace_does_not_set_cancelled_state(
    docket: Docket, the_task: AsyncMock
):
    """replace() should not create CANCELLED state - it's a replacement, not cancellation."""
    docket.register(the_task)

    # Schedule a task
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future, key="replace-task")("original")

    # Replace it
    await docket.replace(the_task, when=future, key="replace-task")("replaced")

    # The new execution should be SCHEDULED, not CANCELLED
    retrieved = await docket.get_execution("replace-task")
    assert retrieved is not None
    assert retrieved.state == ExecutionState.SCHEDULED
    assert retrieved.args == ("replaced",)  # New args


async def test_cancellation_idempotent_with_tombstone(
    docket: Docket, the_task: AsyncMock
):
    """Cancelling twice should be idempotent - second cancel sees the tombstone."""
    docket.register(the_task)

    # Schedule a task
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    execution = await docket.add(the_task, when=future, key="idempotent-task")("test")

    # Cancel it twice - both should succeed
    await docket.cancel(execution.key)
    await docket.cancel(execution.key)  # Should be no-op

    # Should still have CANCELLED tombstone
    retrieved = await docket.get_execution(execution.key)
    assert retrieved is not None
    assert retrieved.state == ExecutionState.CANCELLED


# Tests for task registration before __aenter__


def test_standard_tasks_available_after_init():
    """Standard tasks (trace, fail, sleep) should be available after __init__."""
    docket = Docket(name="test-standard-tasks", url="memory://")

    assert "trace" in docket.tasks
    assert "fail" in docket.tasks
    assert "sleep" in docket.tasks


def test_register_task_before_aenter():
    """Tasks can be registered before entering the async context manager."""
    docket = Docket(name="test-pre-register", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


async def test_registered_task_usable_after_aenter():
    """Tasks registered before __aenter__ should be usable inside the context."""
    docket = Docket(name="test-pre-register-usable", url="memory://")

    async def my_task(value: str) -> None: ...

    docket.register(my_task)

    async with docket:
        assert "my_task" in docket.tasks
        execution = await docket.add(my_task)("test-value")
        assert execution.function is my_task
        assert execution.args == ("test-value",)


async def test_tasks_persist_after_aexit():
    """Task registry should persist after exiting the async context."""
    docket = Docket(name="test-persist-after-exit", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    async with docket:
        ...

    # Tasks should still be there after exit
    assert "my_task" in docket.tasks
    assert "trace" in docket.tasks


async def test_docket_reentry_preserves_tasks():
    """Re-entering the docket should preserve both user and standard tasks."""
    docket = Docket(name="test-reentry", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    # First entry/exit
    async with docket:
        assert "my_task" in docket.tasks
        assert "trace" in docket.tasks

    # Re-entry should still have all tasks
    async with docket:
        assert "my_task" in docket.tasks
        assert "trace" in docket.tasks


def test_register_task_with_custom_name():
    """Tasks can be registered under a custom name instead of __name__."""
    docket = Docket(name="test-custom-name", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=["custom_name"])

    assert "custom_name" in docket.tasks
    assert docket.tasks["custom_name"] is my_task
    # Original name should NOT be registered
    assert "my_task" not in docket.tasks


def test_register_task_with_multiple_names():
    """Tasks can be registered under multiple names."""
    docket = Docket(name="test-multiple-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=["alias_a", "alias_b", "alias_c"])

    assert "alias_a" in docket.tasks
    assert "alias_b" in docket.tasks
    assert "alias_c" in docket.tasks
    assert docket.tasks["alias_a"] is my_task
    assert docket.tasks["alias_b"] is my_task
    assert docket.tasks["alias_c"] is my_task
    # Original name should NOT be registered
    assert "my_task" not in docket.tasks


def test_register_task_with_empty_names_defaults_to_function_name():
    """Empty names list should default to function.__name__."""
    docket = Docket(name="test-empty-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=[])

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


def test_register_task_with_none_names_defaults_to_function_name():
    """None names should default to function.__name__."""
    docket = Docket(name="test-none-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=None)

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


async def test_schedule_task_by_alias(docket: Docket, worker: Worker):
    """Tasks can be scheduled by their alias name."""
    results: list[str] = []

    async def my_task(value: str) -> None:
        results.append(value)

    docket.register(my_task, names=["task_alias"])

    await docket.add("task_alias")("hello")
    await worker.run_until_finished()

    assert results == ["hello"]


async def test_alias_appears_in_worker_announcements(docket: Docket):
    """Alias names should appear in worker task announcements."""

    async def my_task() -> None: ...

    docket.register(my_task, names=["custom_alias"])

    async with Worker(docket) as w:
        await asyncio.sleep(0.1)  # Let heartbeat fire
        workers = await docket.task_workers("custom_alias")
        assert len(workers) == 1
        assert w.name in {worker.name for worker in workers}


async def test_stream_not_created_on_docket_init(redis_url: str):
    """Stream and consumer group should NOT be created when Docket is initialized.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    from uuid import uuid4

    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)
    async with docket:
        async with docket.redis() as redis:
            stream_exists = await redis.exists(docket.stream_key)
            assert not stream_exists, "Stream should not exist on Docket init"


async def test_ensure_stream_and_group_is_idempotent(redis_url: str):
    """Calling _ensure_stream_and_group multiple times should not raise errors.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    from uuid import uuid4

    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)
    async with docket:
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1
            assert groups[0]["name"] == docket.worker_group_name.encode()


async def test_docket_without_worker_does_not_create_group(redis_url: str):
    """A Docket used only for adding tasks should not create consumer group.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    from uuid import uuid4

    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)

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
async def test_snapshot_handles_nogroup_with_real_redis(redis_url: str):
    """Snapshot should handle NOGROUP error and create group automatically.

    Issue #206: Lazy stream/consumer group bootstrap.

    This test uses real Redis (not memory://) to verify the NOGROUP error
    handling path in snapshot(), since the memory:// backend proactively
    creates the group to work around a fakeredis bug.
    """
    from uuid import uuid4

    docket = Docket(name=f"fresh-docket-{uuid4()}", url=redis_url)

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
