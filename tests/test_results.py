"""Tests for task result persistence."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from docket import Docket, Worker
from docket.execution import Execution, ExecutionState


class CustomError(Exception):
    """Custom exception for testing."""

    def __init__(self, message: str, code: int):
        super().__init__(message, code)
        self.message = message
        self.code = code


async def test_result_storage_for_int_return(docket: Docket, worker: Worker):
    """Test that int results are stored and retrievable."""
    result_value = 42

    async def returns_int() -> int:
        return result_value

    docket.register(returns_int)
    execution = await docket.add(returns_int)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_str_return(docket: Docket, worker: Worker):
    """Test that string results are stored and retrievable."""
    result_value = "hello world"

    async def returns_str() -> str:
        return result_value

    docket.register(returns_str)
    execution = await docket.add(returns_str)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_dict_return(docket: Docket, worker: Worker):
    """Test that dict results are stored and retrievable."""
    result_value = {"key": "value", "number": 123}

    async def returns_dict() -> dict[str, Any]:
        return result_value

    docket.register(returns_dict)
    execution = await docket.add(returns_dict)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_object_return(docket: Docket, worker: Worker):
    """Test that object results are stored and retrievable."""

    class CustomObject:
        def __init__(self, value: int):
            self.value = value

        def __eq__(self, other: Any) -> bool:
            return isinstance(other, CustomObject) and self.value == other.value

    result_value = CustomObject(42)

    async def returns_object() -> CustomObject:
        return result_value

    docket.register(returns_object)
    execution = await docket.add(returns_object)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_no_storage_for_none_annotated_task(docket: Docket, worker: Worker):
    """Test that tasks annotated with -> None don't store results."""

    async def returns_none_annotated() -> None:
        pass

    docket.register(returns_none_annotated)
    execution = await docket.add(returns_none_annotated)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    assert execution.result_key is None

    # get_result should return None
    result = await execution.get_result()
    assert result is None


async def test_no_storage_for_runtime_none(docket: Docket, worker: Worker):
    """Test that tasks returning None at runtime don't store results."""

    async def returns_none_runtime() -> int | None:
        return None

    docket.register(returns_none_runtime)
    execution = await docket.add(returns_none_runtime)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    assert execution.result_key is None

    # get_result should return None
    result = await execution.get_result()
    assert result is None


async def test_exception_storage_and_retrieval(docket: Docket, worker: Worker):
    """Test that exceptions are stored and re-raised."""
    error_msg = "Test error"
    error_code = 500

    async def raises_error() -> int:
        raise CustomError(error_msg, error_code)

    docket.register(raises_error)
    execution = await docket.add(raises_error)()
    await worker.run_until_finished()

    # Verify execution failed
    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.result_key is not None

    # get_result should raise the stored exception
    with pytest.raises(CustomError) as exc_info:
        await execution.get_result()

    # Verify exception details are preserved
    assert exc_info.value.message == error_msg
    assert exc_info.value.code == error_code


async def test_get_result_waits_for_completion(docket: Docket, worker: Worker):
    """Test that get_result waits for execution to complete."""
    result_value = 123

    async def slow_task() -> int:
        await asyncio.sleep(0.1)
        return result_value

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    # Start worker in background
    worker_task = asyncio.create_task(worker.run_until_finished())

    # get_result should wait for completion
    result = await execution.get_result()
    assert result == result_value

    await worker_task


async def test_get_result_timeout(docket: Docket, worker: Worker):
    """Test that get_result respects timeout."""
    event = asyncio.Event()  # Never set, simulates hung task

    async def hung_task():
        await event.wait()

    docket.register(hung_task)
    execution = await docket.add(hung_task)()

    # Start worker in background
    worker_task = asyncio.create_task(worker.run_until_finished())

    # get_result should timeout
    deadline = datetime.now(timezone.utc) + timedelta(seconds=1)
    with pytest.raises(TimeoutError):
        await execution.get_result(deadline=deadline)

    # Let the task complete so worker can finish
    event.set()
    await worker_task


async def test_result_ttl_matches_execution_ttl(docket: Docket, worker: Worker):
    """Test that result TTL matches execution TTL."""
    # Create docket with short TTL
    execution_ttl = timedelta(seconds=2)
    async with Docket(
        name=f"test-ttl-{docket.name}", url=docket.url, execution_ttl=execution_ttl
    ) as ttl_docket:

        async def returns_value() -> int:
            return 42

        ttl_docket.register(returns_value)

        async with Worker(
            ttl_docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as ttl_worker:
            execution = await ttl_docket.add(returns_value)()
            await ttl_worker.run_until_finished()

            # Result should be available immediately
            result = await execution.get_result()
            assert result == 42

            # Wait for TTL to expire
            await asyncio.sleep(3)

            # Result should be gone (expired)
            await execution.sync()
            result_data = await ttl_docket.result_storage.get(execution.key)
            assert result_data is None


async def test_multiple_concurrent_get_result_calls(docket: Docket, worker: Worker):
    """Test that multiple concurrent get_result calls work correctly."""
    result_value = 999

    async def returns_value() -> int:
        await asyncio.sleep(0.05)
        return result_value

    docket.register(returns_value)
    execution = await docket.add(returns_value)()

    # Start worker in background
    worker_task = asyncio.create_task(worker.run_until_finished())

    # Multiple concurrent get_result calls
    results = await asyncio.gather(
        execution.get_result(),
        execution.get_result(),
        execution.get_result(),
    )

    # All should return the same result
    assert all(r == result_value for r in results)

    await worker_task


async def test_get_result_on_already_completed_task(docket: Docket, worker: Worker):
    """Test get_result on an already completed task."""
    result_value = 555

    async def returns_value() -> int:
        return result_value

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    # Wait for completion
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # get_result should return immediately
    result = await execution.get_result()
    assert result == result_value


async def test_get_result_on_already_failed_task(docket: Docket, worker: Worker):
    """Test get_result on an already failed task."""

    async def raises_error() -> int:
        raise ValueError("test error")

    docket.register(raises_error)
    execution = await docket.add(raises_error)()
    await worker.run_until_finished()

    # Wait for failure
    await execution.sync()
    assert execution.state == ExecutionState.FAILED

    # get_result should raise immediately
    with pytest.raises(ValueError):
        await execution.get_result()


async def test_result_key_stored_in_execution_record(docket: Docket, worker: Worker):
    """Test that result key is stored in execution record."""

    async def returns_value() -> int:
        return 123

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    # Sync and check result field
    await execution.sync()
    assert execution.result_key == execution.key


async def test_get_result_with_expired_timeout(docket: Docket):
    """Test that get_result raises immediately if timeout already expired."""
    from unittest.mock import AsyncMock

    from docket.execution import Execution

    # Create execution in non-terminal state
    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )
    execution.state = ExecutionState.RUNNING

    # Set deadline to 1 second in the past
    deadline = datetime.now(timezone.utc) - timedelta(seconds=1)

    # Should raise TimeoutError immediately without waiting
    with pytest.raises(TimeoutError) as exc_info:
        await execution.get_result(deadline=deadline)

    assert "Timeout waiting for execution" in str(exc_info.value)


async def test_get_result_failed_task_without_result_key(docket: Docket):
    """Test get_result on failed task without stored exception."""
    from unittest.mock import AsyncMock

    from docket.execution import Execution

    # Create execution in FAILED state without result_key
    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )
    execution.state = ExecutionState.FAILED
    execution.error = "Something went wrong"
    execution.result_key = None  # No exception stored

    # Should raise generic Exception with error message
    with pytest.raises(Exception) as exc_info:
        await execution.get_result()

    assert str(exc_info.value) == "Something went wrong"


async def test_get_result_with_malformed_result_data(docket: Docket, worker: Worker):
    """Test get_result gracefully handles malformed result data."""
    from unittest.mock import patch

    async def returns_value() -> int:
        return 123

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    assert execution.result_key is not None

    # Mock result_storage.get() to return data without "data" field
    mock_result = {"some_field": "but_no_data"}
    with patch.object(
        docket.result_storage, "get", return_value=mock_result
    ) as mock_get:
        result = await execution.get_result()

    # Should return None when data field is missing
    assert result is None
    mock_get.assert_called_once_with(execution.result_key)


async def test_get_result_failed_task_with_missing_exception_data(
    docket: Docket, worker: Worker
):
    """Test get_result on failed task when exception data is missing from storage."""
    from unittest.mock import patch

    async def raises_error() -> int:
        raise ValueError("test error")

    docket.register(raises_error)
    execution = await docket.add(raises_error)()
    await worker.run_until_finished()

    # Verify execution failed with result_key
    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.result_key is not None

    # Mock result_storage.get() to return None (exception data missing)
    with patch.object(docket.result_storage, "get", return_value=None):
        # Should fall back to generic error with error message
        with pytest.raises(Exception) as exc_info:
            await execution.get_result()

    # Should use the error message from execution.error
    assert "test error" in str(exc_info.value)


async def test_get_result_with_timeout_timedelta(docket: Docket, worker: Worker):
    """Test get_result using timeout parameter (timedelta)."""

    async def returns_value() -> int:
        return 42

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    result = await execution.get_result(timeout=timedelta(seconds=1))
    assert result == 42


async def test_get_result_with_deadline_datetime(docket: Docket, worker: Worker):
    """Test get_result using deadline parameter (datetime)."""

    async def returns_value() -> int:
        return 42

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    deadline = datetime.now(timezone.utc) + timedelta(seconds=1)
    result = await execution.get_result(deadline=deadline)
    assert result == 42


async def test_get_result_with_both_timeout_and_deadline_raises(
    docket: Docket,
):
    """Test that specifying both timeout and deadline raises ValueError."""
    from unittest.mock import AsyncMock

    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )
    execution.state = ExecutionState.COMPLETED

    with pytest.raises(ValueError) as exc_info:
        await execution.get_result(
            timeout=timedelta(seconds=1),
            deadline=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

    assert "Cannot specify both timeout and deadline" in str(exc_info.value)


async def test_get_result_timeout_on_pending_task(docket: Docket, worker: Worker):
    """Test get_result with timeout (timedelta) on pending task."""
    event = asyncio.Event()

    async def waits_forever() -> int:
        await event.wait()
        return 42

    docket.register(waits_forever)
    execution = await docket.add(waits_forever)()

    worker_task = asyncio.create_task(worker.run_until_finished())

    with pytest.raises(TimeoutError):
        await execution.get_result(timeout=timedelta(seconds=0.1))

    event.set()
    await worker_task
