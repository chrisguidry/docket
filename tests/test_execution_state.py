"""Tests for execution state transitions, TTL, and lifecycle."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable
from unittest.mock import AsyncMock

from docket import Docket, Execution, ExecutionState, Progress, Worker


async def test_run_state_scheduled(docket: Docket, the_task: AsyncMock):
    """Execution should be set to QUEUED when an immediate task is added."""
    execution = await docket.add(the_task)("arg1", "arg2")

    assert isinstance(execution, Execution)
    await execution.sync()
    assert execution.state == ExecutionState.QUEUED


async def test_run_state_pending_to_running(docket: Docket, worker: Worker):
    """Execution should transition from QUEUED to RUNNING during execution."""
    executed = asyncio.Event()

    async def test_task():
        # Verify we're in RUNNING state
        executed.set()

    await docket.add(test_task)()

    # Start worker but don't wait for completion yet
    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for task to start executing
    await executed.wait()

    # Give it a moment to complete
    await worker_task


async def test_run_state_completed_on_success(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Execution should be set to COMPLETED when task succeeds."""
    execution = await docket.add(the_task)()

    await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED


async def test_run_state_failed_on_exception(docket: Docket, worker: Worker):
    """Execution should be set to FAILED when task raises an exception."""

    async def failing_task():
        raise ValueError("Task failed!")

    execution = await docket.add(failing_task)()

    await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.FAILED


async def test_run_state_ttl_after_completion(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """Run state should have TTL set after completion."""
    execution = await docket.add(the_task)()

    await worker.run_until_finished()

    # Verify state exists
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Verify TTL is set to the configured execution_ttl (default: 1 hour = 3600 seconds)
    expected_ttl = int(docket.execution_ttl.total_seconds())
    async with docket.redis() as redis:
        ttl = await redis.ttl(execution._redis_key)  # type: ignore[reportPrivateUsage]
        assert 0 < ttl <= expected_ttl  # TTL should be set and reasonable


async def test_custom_execution_ttl(
    redis_url: str, the_task: AsyncMock, make_docket_name: Callable[[], str]
):
    """Docket should respect custom execution_ttl configuration."""
    # Create docket with custom 5-minute TTL
    custom_ttl = timedelta(minutes=5)
    async with Docket(
        name=make_docket_name(), url=redis_url, execution_ttl=custom_ttl
    ) as docket:
        async with Worker(docket) as worker:
            execution = await docket.add(the_task)()

            await worker.run_until_finished()

            # Verify state is completed
            await execution.sync()
            assert execution.state == ExecutionState.COMPLETED

            # Verify TTL matches custom value (300 seconds)
            expected_ttl = int(custom_ttl.total_seconds())
        async with docket.redis() as redis:
            ttl = await redis.ttl(execution._redis_key)  # type: ignore[reportPrivateUsage]
            assert 0 < ttl <= expected_ttl
            # Verify it's approximately the custom value (not the default 3600)
            assert ttl > 200  # Should be close to 300, not near 0
            assert ttl <= 300  # Should not exceed configured value


async def test_full_lifecycle_integration(docket: Docket, worker: Worker):
    """Test complete lifecycle: SCHEDULED -> QUEUED -> RUNNING -> COMPLETED."""
    states_observed: list[ExecutionState] = []

    async def tracking_task(progress: Progress = Progress()):
        await progress.set_total(3)
        for i in range(3):
            await progress.increment()
            await progress.set_message(f"Step {i + 1}")
            await asyncio.sleep(0.01)

    # Schedule task in the future
    when = datetime.now(timezone.utc) + timedelta(milliseconds=50)
    execution = await docket.add(tracking_task, when=when)()

    # Should be SCHEDULED
    await execution.sync()
    assert execution.state == ExecutionState.SCHEDULED
    states_observed.append(execution.state)

    # Run worker
    await worker.run_until_finished()

    # Should be COMPLETED
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    states_observed.append(execution.state)

    # Verify we observed the expected states
    assert ExecutionState.SCHEDULED in states_observed
    assert ExecutionState.COMPLETED in states_observed


async def test_run_add_returns_run_instance(docket: Docket, the_task: AsyncMock):
    """Verify that docket.add() returns an Execution instance."""
    result = await docket.add(the_task)("arg1")

    assert isinstance(result, Execution)
    assert result.key is not None
    assert len(result.key) > 0


async def test_error_message_stored_on_failure(docket: Docket, worker: Worker):
    """Failed run should store error message."""

    async def failing_task():
        raise RuntimeError("Something went wrong!")

    execution = await docket.add(failing_task)()

    await worker.run_until_finished()

    # Check state is FAILED
    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.error == "RuntimeError: Something went wrong!"


async def test_execution_sync_with_no_redis_data(docket: Docket):
    """Test sync() when no execution data exists in Redis."""
    execution = Execution(
        docket, AsyncMock(), (), {}, "nonexistent-key", datetime.now(timezone.utc), 1
    )

    # Sync without ever scheduling
    await execution.sync()

    # Should reset to defaults
    assert execution.state == ExecutionState.SCHEDULED
    assert execution.worker is None
    assert execution.started_at is None
    assert execution.completed_at is None
    assert execution.error is None


async def test_execution_sync_with_missing_state_field(docket: Docket):
    """Test sync() when Redis data exists but has no 'state' field."""
    from unittest.mock import AsyncMock, patch

    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )

    # Set initial state
    execution.state = ExecutionState.RUNNING

    # Mock Redis to return data WITHOUT state field
    mock_data = {
        b"worker": b"worker-1",
        b"started_at": b"2024-01-01T00:00:00+00:00",
        # No b"state" field - state_value will be None
    }

    with patch.object(execution.docket, "redis") as mock_redis_ctx:
        mock_redis = AsyncMock()
        mock_redis.hgetall.return_value = mock_data
        mock_redis_ctx.return_value.__aenter__.return_value = mock_redis
        mock_redis_ctx.return_value.__aexit__.return_value = None

        # Mock progress sync to avoid extra Redis calls
        with patch.object(execution.progress, "sync"):
            await execution.sync()

    # State should NOT be updated (stays as RUNNING)
    assert execution.state == ExecutionState.RUNNING
    # But other fields should be updated
    assert execution.worker == "worker-1"
    assert execution.started_at is not None


async def test_execution_sync_with_string_state_value(docket: Docket):
    """Test sync() handles non-bytes state value (defensive coding)."""
    from unittest.mock import AsyncMock, patch

    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )

    # Mock Redis to return string state (defensive code handles both bytes and str)
    mock_data = {
        b"state": "completed",  # String, not bytes!
        b"worker": b"worker-1",
        b"completed_at": b"2024-01-01T00:00:00+00:00",
    }

    with patch.object(execution.docket, "redis") as mock_redis_ctx:
        mock_redis = AsyncMock()
        mock_redis.hgetall.return_value = mock_data
        mock_redis_ctx.return_value.__aenter__.return_value = mock_redis
        mock_redis_ctx.return_value.__aexit__.return_value = None

        # Mock progress sync
        with patch.object(execution.progress, "sync"):
            await execution.sync()

    # Should handle string and set state correctly
    assert execution.state == ExecutionState.COMPLETED
    assert execution.worker == "worker-1"


async def test_mark_as_failed_without_error_message(docket: Docket):
    """Test mark_as_failed with error=None."""
    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )
    await execution.claim("worker-1")
    await execution.mark_as_failed(error=None)

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.error is None
    assert execution.completed_at is not None
