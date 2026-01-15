"""Tests for Docket execution retrieval, scheduling, and cancellation."""

from datetime import datetime, timedelta, timezone
from typing import Callable
from unittest.mock import AsyncMock

from docket import Execution
from docket.docket import Docket
from docket.execution import ExecutionState
from tests._key_leak_checker import KeyCountChecker


async def test_docket_schedule_method_with_immediate_task(
    docket: Docket, the_task: AsyncMock
):
    """Test direct scheduling via docket.schedule(execution) for immediate execution."""
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
    runs_key = docket.runs_key("incomplete-task")
    key_leak_checker.add_exemption(runs_key)

    # Manually create runs hash with missing fields
    async with docket.redis() as redis:
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
    runs_key = docket.runs_key("no-when-task")
    key_leak_checker.add_exemption(runs_key)

    # Manually create runs hash with function/args/kwargs but no when
    async with docket.redis() as redis:
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
    runs_key = docket.runs_key("unregistered-task")
    key_leak_checker.add_exemption(runs_key)

    # Manually create runs hash with unregistered function
    async with docket.redis() as redis:
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


# Tests for cancellation


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
        runs_key = docket.runs_key(execution.key)
        ttl = await redis.ttl(runs_key)

        # TTL should be set (not -1 which means no expiry)
        # Should be close to execution_ttl (default 15 minutes = 900 seconds)
        assert ttl > 0
        assert ttl <= int(docket.execution_ttl.total_seconds())


async def test_cancelled_state_with_ttl_zero(
    docket: Docket, the_task: AsyncMock, make_docket_name: Callable[[], str]
):
    """Cancelled task with execution_ttl=0 should delete tombstone immediately."""
    # Create docket with TTL=0
    async with Docket(
        name=make_docket_name(),
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
