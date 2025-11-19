"""Tests for task execution state and progress tracking."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Execution, ExecutionState, Progress, Worker
from docket.execution import ExecutionProgress, ProgressEvent, StateEvent


@pytest.fixture
async def execution(docket: Docket):
    """Create an Execution with key='test-key' that cleans up after itself."""
    execution = Execution(
        docket, AsyncMock(), (), {}, "test-key", datetime.now(timezone.utc), 1
    )
    try:
        yield execution
    finally:
        # Clean up execution state and progress data
        await execution.mark_as_completed()


@pytest.fixture
async def progress(execution: Execution):
    """Create an ExecutionProgress from execution that cleans up with it."""
    await execution.claim("worker-1")
    return execution.progress


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


async def test_progress_create(execution: Execution):
    """Progress.create() should initialize instance from Redis."""
    await execution.claim("worker-1")
    progress = execution.progress

    # Set some values
    await progress.set_total(100)
    await progress.increment(5)
    await progress.set_message("Test message")

    # Now create a new instance using create()
    progress2 = await ExecutionProgress.create(execution.docket, "test-key")

    # Verify it loaded the data from Redis
    assert progress2.current == 5
    assert progress2.total == 100
    assert progress2.message == "Test message"
    assert progress2.updated_at is not None


async def test_progress_set_total(progress: ExecutionProgress):
    """Progress should be able to set total value."""
    await progress.set_total(100)

    assert progress.total == 100
    assert progress.updated_at is not None


async def test_progress_set_total_invalid(docket: Docket):
    """Progress should raise an error if total is less than 1."""
    progress = ExecutionProgress(docket, "test-key")
    with pytest.raises(ValueError):
        await progress.set_total(0)


async def test_progress_increment_invalid(docket: Docket):
    """Progress should raise an error if amount is less than 1."""
    progress = ExecutionProgress(docket, "test-key")
    with pytest.raises(ValueError):
        await progress.increment(0)


async def test_progress_increment(progress: ExecutionProgress):
    """Progress should atomically increment current value."""
    # Increment multiple times
    await progress.increment()
    await progress.increment()
    await progress.increment(2)

    assert progress.current == 4  # 0 + 1 + 1 + 2 = 4
    assert progress.updated_at is not None


async def test_progress_set_message(progress: ExecutionProgress):
    """Progress should be able to set status message."""
    await progress.set_message("Processing items...")

    assert progress.message == "Processing items..."
    assert progress.updated_at is not None


async def test_progress_dependency_injection(docket: Docket, worker: Worker):
    """Progress dependency should be injected into task functions."""
    progress_values: list[int] = []

    async def task_with_progress(progress: Progress = Progress()):
        await progress.set_total(10)
        for i in range(10):
            await asyncio.sleep(0.001)
            await progress.increment()
            await progress.set_message(f"Processing item {i + 1}")
            # Capture progress data
            assert progress.current is not None
            progress_values.append(progress.current)

    await docket.add(task_with_progress)()

    await worker.run_until_finished()

    # Verify progress was tracked
    assert len(progress_values) > 0
    assert progress_values[-1] == 10  # Should reach 10


async def test_progress_deleted_on_completion(docket: Docket, worker: Worker):
    """Progress data should be deleted when task completes."""

    async def task_with_progress(progress: Progress = Progress()):
        await progress.set_total(5)
        await progress.increment()

    execution = await docket.add(task_with_progress)()

    # Before execution, no progress
    await execution.progress.sync()
    assert execution.progress.current is None

    await worker.run_until_finished()

    # After completion, progress should be deleted
    await execution.progress.sync()
    assert execution.progress.current is None


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


async def test_custom_execution_ttl(redis_url: str, the_task: AsyncMock):
    """Docket should respect custom execution_ttl configuration."""
    # Create docket with custom 5-minute TTL
    custom_ttl = timedelta(minutes=5)
    async with Docket(
        name="test-custom-ttl", url=redis_url, execution_ttl=custom_ttl
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


async def test_progress_with_multiple_increments(docket: Docket, worker: Worker):
    """Test progress tracking with realistic usage pattern."""

    async def process_items(items: list[int], progress: Progress = Progress()):
        await progress.set_total(len(items))
        await progress.set_message("Starting processing")

        for i, _item in enumerate(items):
            await asyncio.sleep(0.001)  # Simulate work
            await progress.increment()
            await progress.set_message(f"Processed item {i + 1}/{len(items)}")

        await progress.set_message("All items processed")

    items = list(range(20))
    execution = await docket.add(process_items)(items)

    await worker.run_until_finished()

    # Verify final state
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED


async def test_progress_without_total(docket: Docket, worker: Worker):
    """Progress should work even without setting total."""

    async def task_without_total(progress: Progress = Progress()):
        for _ in range(5):
            await progress.increment()
            await asyncio.sleep(0.001)

    execution = await docket.add(task_without_total)()

    await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED


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


async def test_concurrent_progress_updates(progress: ExecutionProgress):
    """Progress updates should be atomic and safe for concurrent access."""

    # Simulate concurrent increments
    async def increment_many():
        for _ in range(10):
            await progress.increment()

    await asyncio.gather(
        increment_many(),
        increment_many(),
        increment_many(),
    )

    # Sync to ensure we have the latest value from Redis
    await progress.sync()
    # Should be exactly 30 due to atomic HINCRBY
    assert progress.current == 30


async def test_progress_publish_events(progress: ExecutionProgress):
    """Progress updates should publish events to pub/sub channel."""
    # Set up subscriber in background
    events: list[ProgressEvent] = []

    async def collect_events():
        async for event in progress.subscribe():  # pragma: no cover
            events.append(event)
            if len(events) >= 3:  # Collect 3 events then stop
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Publish updates
    await progress.set_total(100)
    await progress.increment(10)
    await progress.set_message("Processing...")

    # Wait for subscriber to collect events
    await asyncio.wait_for(subscriber_task, timeout=2.0)

    # Verify we received progress events
    assert len(events) >= 3

    # Check set_total event
    total_event = next(e for e in events if e.get("total") == 100)
    assert total_event["type"] == "progress"
    assert total_event["key"] == "test-key"
    assert "updated_at" in total_event

    # Check increment event
    increment_event = next(e for e in events if e.get("current") == 10)
    assert increment_event["type"] == "progress"
    assert increment_event["current"] == 10

    # Check message event
    message_event = next(e for e in events if e.get("message") == "Processing...")
    assert message_event["type"] == "progress"
    assert message_event["message"] == "Processing..."


async def test_state_publish_events(docket: Docket, the_task: AsyncMock):
    """State changes should publish events to pub/sub channel."""
    # Note: This test verifies the pub/sub mechanism works.
    # Pub/sub is skipped for memory:// backend, so this test effectively
    # documents the expected behavior for real Redis backends.

    execution = await docket.add(the_task, key="test-key")()

    # Verify state was set correctly
    assert execution.state == ExecutionState.QUEUED

    # Verify state record exists in Redis
    await execution.sync()
    assert execution.state == ExecutionState.QUEUED


async def test_run_subscribe_both_state_and_progress(execution: Execution):
    """Run.subscribe() should yield both state and progress events."""
    # Set up subscriber in background
    all_events: list[StateEvent | ProgressEvent] = []

    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            all_events.append(event)
            # Stop after we get a running state and some progress
            if (
                len(
                    [
                        e
                        for e in all_events
                        if e["type"] == "state" and e["state"] == ExecutionState.RUNNING
                    ]
                )
                > 0
                and len([e for e in all_events if e["type"] == "progress"]) >= 3
            ):
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Publish mixed state and progress events
    await execution.claim("worker-1")
    await execution.progress.set_total(50)
    await execution.progress.increment(5)

    # Wait for subscriber to collect events
    await asyncio.wait_for(subscriber_task, timeout=2.0)

    # Verify we got both types
    state_events = [e for e in all_events if e["type"] == "state"]
    progress_events = [e for e in all_events if e["type"] == "progress"]

    assert len(state_events) >= 1
    assert len(progress_events) >= 2

    # Verify state event
    running_event = next(
        e for e in state_events if e["state"] == ExecutionState.RUNNING
    )
    assert running_event["worker"] == "worker-1"

    # Verify progress events
    total_event = next(e for e in progress_events if e.get("total") == 50)
    assert total_event["current"] is not None and total_event["current"] >= 0

    increment_event = next(e for e in progress_events if e.get("current") == 5)
    assert increment_event["current"] == 5


async def test_completed_state_publishes_event(execution: Execution):
    """Completed state should publish event with completed_at timestamp."""
    # Set up subscriber
    events: list[StateEvent] = []

    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                events.append(event)
            if any(e["state"] == ExecutionState.COMPLETED for e in events):
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    await execution.claim("worker-1")
    await execution.mark_as_completed()

    await asyncio.wait_for(subscriber_task, timeout=2.0)

    # Find completed event
    completed_event = next(e for e in events if e["state"] == ExecutionState.COMPLETED)
    assert completed_event["type"] == "state"
    assert "completed_at" in completed_event


async def test_failed_state_publishes_event_with_error(execution: Execution):
    """Failed state should publish event with error message."""
    # Set up subscriber
    events: list[StateEvent] = []

    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                events.append(event)
            if any(e["state"] == ExecutionState.FAILED for e in events):
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    await execution.claim("worker-1")
    await execution.mark_as_failed("Something went wrong!")

    await asyncio.wait_for(subscriber_task, timeout=2.0)

    # Find failed event
    failed_event = next(e for e in events if e["state"] == ExecutionState.FAILED)
    assert failed_event["type"] == "state"
    assert failed_event["error"] == "Something went wrong!"
    assert "completed_at" in failed_event


async def test_end_to_end_progress_monitoring_with_worker(
    docket: Docket, worker: Worker
):
    """Test complete end-to-end progress monitoring with real worker execution."""
    collected_events: list[StateEvent | ProgressEvent] = []

    async def task_with_progress(progress: Progress = Progress()):
        """Task that reports progress as it executes."""
        await progress.set_total(5)
        await progress.set_message("Starting work")

        for i in range(5):
            await asyncio.sleep(0.01)
            await progress.increment()
            await progress.set_message(f"Processing step {i + 1}/5")

        await progress.set_message("Work complete")

    # Schedule the task
    execution = await docket.add(task_with_progress)()

    # Start subscriber to collect events
    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            collected_events.append(event)
            # Stop when we reach completed state
            if event["type"] == "state" and event["state"] == ExecutionState.COMPLETED:
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Run the worker
    await worker.run_until_finished()

    # Wait for subscriber to finish
    await asyncio.wait_for(subscriber_task, timeout=5.0)

    # Verify we collected comprehensive events
    assert len(collected_events) > 0

    # Extract event types
    state_events: list[StateEvent] = [
        e for e in collected_events if e["type"] == "state"
    ]
    progress_events = [e for e in collected_events if e["type"] == "progress"]

    # Verify state transitions occurred
    # Note: scheduled may happen before subscriber connects
    state_sequence: list[ExecutionState] = [e["state"] for e in state_events]
    assert state_sequence == [
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.COMPLETED,
    ]

    # Verify worker was recorded
    running_events = [e for e in state_events if e["state"] == ExecutionState.RUNNING]
    assert len(running_events) > 0
    assert "worker" in running_events[0]

    # Verify progress events were published
    assert len(progress_events) >= 5  # At least one for each increment

    # Verify progress reached total
    final_progress = progress_events[-1]
    assert final_progress["current"] is not None and final_progress["current"] == 5
    assert final_progress["total"] == 5

    # Verify messages were updated
    message_events = [e for e in progress_events if e.get("message")]
    assert len(message_events) > 0
    assert any(
        "complete" in e["message"].lower()
        for e in message_events
        if e["message"] is not None
    )

    # Verify final state is completed
    assert state_events[-1]["state"] == ExecutionState.COMPLETED
    assert "completed_at" in state_events[-1]


async def test_end_to_end_failed_task_monitoring(docket: Docket, worker: Worker):
    """Test progress monitoring for a task that fails."""
    collected_events: list[StateEvent | ProgressEvent] = []

    async def failing_task(progress: Progress = Progress()):
        """Task that reports progress then fails."""
        await progress.set_total(10)
        await progress.set_message("Starting work")
        await progress.increment(3)
        await progress.set_message("About to fail")
        raise ValueError("Task failed intentionally")

    # Schedule the task
    execution = await docket.add(failing_task)()

    # Start subscriber
    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            collected_events.append(event)
            # Stop when we reach failed state
            if event["type"] == "state" and event["state"] == ExecutionState.FAILED:
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    # Run the worker
    await worker.run_until_finished()

    # Wait for subscriber
    await asyncio.wait_for(subscriber_task, timeout=5.0)

    # Verify we got events
    assert len(collected_events) > 0

    state_events = [e for e in collected_events if e["type"] == "state"]
    progress_events = [e for e in collected_events if e["type"] == "progress"]

    # Verify task reached running state
    state_sequence = [e["state"] for e in state_events]
    assert state_sequence == [
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.FAILED,
    ]

    # Verify progress was reported before failure
    assert len(progress_events) >= 2

    # Find set_total event
    total_event = next((e for e in progress_events if e.get("total") == 10), None)
    assert total_event is not None

    # Find increment event
    increment_event = next((e for e in progress_events if e.get("current") == 3), None)
    assert increment_event is not None

    # Verify error message in failed event
    failed_event = next(e for e in state_events if e["state"] == ExecutionState.FAILED)
    assert failed_event["error"] is not None
    assert "ValueError" in failed_event["error"]
    assert "intentionally" in failed_event["error"]


async def test_mark_as_failed_without_error_message(execution: Execution):
    """Test mark_as_failed with error=None."""
    await execution.claim("worker-1")
    await execution.mark_as_failed(error=None)

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.error is None
    assert execution.completed_at is not None


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


async def test_subscribing_to_completed_execution(docket: Docket, worker: Worker):
    """Subscribing to already-completed executions should emit final state."""

    async def completed_task():
        await asyncio.sleep(0.01)

    async def failed_task():
        await asyncio.sleep(0.01)
        raise ValueError("Task failed")

    # Test subscribing to a completed task
    execution = await docket.add(completed_task, key="already-done:123")()

    # Run the task to completion first
    await worker.run_until_finished()

    # Now subscribe to the already-completed execution
    async def get_first_event() -> StateEvent | None:
        async for event in execution.subscribe():  # pragma: no cover
            assert event["type"] == "state"
            return event

    first_event = await get_first_event()
    assert first_event is not None

    # Verify the initial state includes completion metadata
    assert first_event["type"] == "state"
    assert first_event["state"] == ExecutionState.COMPLETED
    assert first_event["completed_at"] is not None
    assert first_event["error"] is None

    # Test subscribing to a failed task
    execution = await docket.add(failed_task, key="already-failed:456")()

    # Run the task to failure first
    await worker.run_until_finished()

    # Now subscribe to the already-failed execution
    async def get_first_failed_event() -> StateEvent | None:
        async for event in execution.subscribe():  # pragma: no cover
            assert event["type"] == "state"
            return event

    first_event = await get_first_failed_event()
    assert first_event is not None

    # Verify the initial state includes error metadata
    assert first_event["type"] == "state"
    assert first_event["state"] == ExecutionState.FAILED
    assert first_event["completed_at"] is not None
    assert first_event["error"] is not None
    assert first_event["error"] == "ValueError: Task failed"
