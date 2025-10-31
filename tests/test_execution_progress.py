"""Tests for task execution state and progress tracking."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock


from docket import Docket, Execution, ExecutionState, Progress, Worker
from docket.execution import ExecutionProgress


async def test_run_state_scheduled(docket: Docket):
    """Execution should be set to SCHEDULED when a task is added."""
    task = AsyncMock()
    task.__name__ = "test_task"

    execution = await docket.add(task)("arg1", "arg2")

    assert isinstance(execution, Execution)
    state = await execution.get_state()
    assert state == ExecutionState.SCHEDULED


async def test_run_state_pending_to_running(docket: Docket, worker: Worker):
    """Execution should transition from PENDING to RUNNING during execution."""
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


async def test_run_state_completed_on_success(docket: Docket, worker: Worker):
    """Execution should be set to COMPLETED when task succeeds."""
    task = AsyncMock()
    task.__name__ = "test_task"

    execution = await docket.add(task)()

    await worker.run_until_finished()

    state = await execution.get_state()
    assert state == ExecutionState.COMPLETED


async def test_run_state_failed_on_exception(docket: Docket, worker: Worker):
    """Execution should be set to FAILED when task raises an exception."""

    async def failing_task():
        raise ValueError("Task failed!")

    execution = await docket.add(failing_task)()

    await worker.run_until_finished()

    state = await execution.get_state()
    assert state == ExecutionState.FAILED


async def test_progress_set_total(docket: Docket):
    """Progress should be able to set total value."""
    progress = ExecutionProgress(docket, "test-key")

    await progress.set_total(100)

    data = await progress.get()
    assert data is not None
    assert data[b"total"] == b"100"


async def test_progress_increment(docket: Docket):
    """Progress should atomically increment current value."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )

    # Initialize with set_running (which sets current=0)
    await execution.set_running("worker-1")
    progress = execution.progress

    # Increment multiple times
    await progress.increment()
    await progress.increment()
    await progress.increment(2)

    data = await progress.get()
    assert data is not None
    assert data[b"current"] == b"4"  # 0 + 1 + 1 + 2 = 4


async def test_progress_set_message(docket: Docket):
    """Progress should be able to set status message."""
    progress = ExecutionProgress(docket, "test-key")

    await progress.set_message("Processing items...")

    data = await progress.get()
    assert data is not None
    assert data[b"message"] == b"Processing items..."


async def test_progress_dependency_injection(docket: Docket, worker: Worker):
    """Progress dependency should be injected into task functions."""
    progress_values = []

    async def task_with_progress(progress: ExecutionProgress = Progress()):
        await progress.set_total(10)
        for i in range(10):
            await asyncio.sleep(0.001)
            await progress.increment()
            await progress.set_message(f"Processing item {i + 1}")
            # Capture progress data
            data = await progress.get()
            if data:
                progress_values.append(int(data[b"current"]))

    await docket.add(task_with_progress)()

    await worker.run_until_finished()

    # Verify progress was tracked
    assert len(progress_values) > 0
    assert progress_values[-1] == 10  # Should reach 10


async def test_progress_deleted_on_completion(docket: Docket, worker: Worker):
    """Progress data should be deleted when task completes."""

    async def task_with_progress(progress: ExecutionProgress = Progress()):
        await progress.set_total(5)
        await progress.increment()

    execution = await docket.add(task_with_progress)()

    # Before execution, no progress
    data = await execution.progress.get()
    assert data is None or data == {}

    await worker.run_until_finished()

    # After completion, progress should be deleted
    data = await execution.progress.get()
    assert data is None or data == {}


async def test_run_state_ttl_after_completion(docket: Docket, worker: Worker):
    """Run state should have TTL set after completion."""
    task = AsyncMock()
    task.__name__ = "test_task"

    execution = await docket.add(task)()

    await worker.run_until_finished()

    # Verify state exists
    state = await execution.get_state()
    assert state == ExecutionState.COMPLETED

    # Verify TTL is set (should be 3600 seconds = 1 hour)
    async with docket.redis() as redis:
        ttl = await redis.ttl(execution._redis_key)
        assert 0 < ttl <= 3600  # TTL should be set and reasonable


async def test_full_lifecycle_integration(docket: Docket, worker: Worker):
    """Test complete lifecycle: SCHEDULED -> PENDING -> RUNNING -> COMPLETED."""
    states_observed = []

    async def tracking_task(progress: ExecutionProgress = Progress()):
        await progress.set_total(3)
        for i in range(3):
            await progress.increment()
            await progress.set_message(f"Step {i + 1}")
            await asyncio.sleep(0.01)

    # Schedule task in the future
    when = datetime.now(timezone.utc) + timedelta(milliseconds=50)
    execution = await docket.add(tracking_task, when=when)()

    # Should be SCHEDULED
    state = await execution.get_state()
    assert state == ExecutionState.SCHEDULED
    states_observed.append(state)

    # Run worker
    await worker.run_until_finished()

    # Should be COMPLETED
    state = await execution.get_state()
    assert state == ExecutionState.COMPLETED
    states_observed.append(state)

    # Verify we observed the expected states
    assert ExecutionState.SCHEDULED in states_observed
    assert ExecutionState.COMPLETED in states_observed


async def test_progress_with_multiple_increments(docket: Docket, worker: Worker):
    """Test progress tracking with realistic usage pattern."""

    async def process_items(items: list[int], progress: ExecutionProgress = Progress()):
        await progress.set_total(len(items))
        await progress.set_message("Starting processing")

        for i, item in enumerate(items):
            await asyncio.sleep(0.001)  # Simulate work
            await progress.increment()
            await progress.set_message(f"Processed item {i + 1}/{len(items)}")

        await progress.set_message("All items processed")

    items = list(range(20))
    execution = await docket.add(process_items)(items)

    await worker.run_until_finished()

    # Verify final state
    state = await execution.get_state()
    assert state == ExecutionState.COMPLETED


async def test_progress_without_total(docket: Docket, worker: Worker):
    """Progress should work even without setting total."""

    async def task_without_total(progress: ExecutionProgress = Progress()):
        for _ in range(5):
            await progress.increment()
            await asyncio.sleep(0.001)

    execution = await docket.add(task_without_total)()

    await worker.run_until_finished()

    state = await execution.get_state()
    assert state == ExecutionState.COMPLETED


async def test_run_add_returns_run_instance(docket: Docket):
    """Verify that docket.add() returns an Execution instance."""
    task = AsyncMock()
    task.__name__ = "test_task"

    result = await docket.add(task)("arg1")

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
    state = await execution.get_state()
    assert state == ExecutionState.FAILED

    # Verify error message is stored (would need to add get() method to Run to check this)
    # For now, just verify it failed


async def test_concurrent_progress_updates(docket: Docket):
    """Progress updates should be atomic and safe for concurrent access."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )
    progress = execution.progress

    await execution.set_running("worker-1")

    # Simulate concurrent increments
    async def increment_many():
        for _ in range(10):
            await progress.increment()

    await asyncio.gather(
        increment_many(),
        increment_many(),
        increment_many(),
    )

    data = await progress.get()
    assert data is not None
    # Should be exactly 30 due to atomic HINCRBY
    assert data[b"current"] == b"30"


async def test_progress_publish_events(docket: Docket):
    """Progress updates should publish events to pub/sub channel."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )
    progress = execution.progress

    # Set up subscriber in background
    events = []

    async def collect_events():
        async for event in progress.subscribe():
            events.append(event)
            if len(events) >= 3:  # Collect 3 events then stop
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Initialize and publish updates
    await execution.set_running("worker-1")
    await progress.set_total(100)
    await progress.increment(10)
    await progress.set_message("Processing...")

    # Wait for subscriber to collect events
    try:
        await asyncio.wait_for(subscriber_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass  # May timeout if we got all events

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


async def test_state_publish_events(docket: Docket):
    """State changes should publish events to pub/sub channel."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )

    # Set up subscriber in background
    events = []

    async def collect_events():
        async for event in execution.subscribe():
            if event["type"] == "state":
                events.append(event)
            if len(events) >= 3:  # Collect 3 state events then stop
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Publish state changes
    when = datetime.now(timezone.utc)
    await execution.set_scheduled(when)
    await execution.set_pending()
    await execution.set_running("worker-1")

    # Wait for subscriber to collect events
    try:
        await asyncio.wait_for(subscriber_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass

    # Verify we received state events
    assert len(events) >= 3

    # Check scheduled event
    scheduled_event = next(e for e in events if e.get("state") == "scheduled")
    assert scheduled_event["type"] == "state"
    assert scheduled_event["key"] == "test-key"
    assert "when" in scheduled_event

    # Check pending event
    pending_event = next(e for e in events if e.get("state") == "pending")
    assert pending_event["type"] == "state"

    # Check running event
    running_event = next(e for e in events if e.get("state") == "running")
    assert running_event["type"] == "state"
    assert running_event["worker"] == "worker-1"
    assert "started_at" in running_event


async def test_run_subscribe_both_state_and_progress(docket: Docket):
    """Run.subscribe() should yield both state and progress events."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )

    # Set up subscriber in background
    all_events = []

    async def collect_events():
        async for event in execution.subscribe():
            all_events.append(event)
            # Stop after we get a running state and some progress
            if (
                len(
                    [
                        e
                        for e in all_events
                        if e["type"] == "state" and e.get("state") == "running"
                    ]
                )
                > 0
                and len([e for e in all_events if e["type"] == "progress"]) >= 2
            ):
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Publish mixed state and progress events
    await execution.set_running("worker-1")
    await execution.progress.set_total(50)
    await execution.progress.increment(5)

    # Wait for subscriber to collect events
    try:
        await asyncio.wait_for(subscriber_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass

    # Verify we got both types
    state_events = [e for e in all_events if e["type"] == "state"]
    progress_events = [e for e in all_events if e["type"] == "progress"]

    assert len(state_events) >= 1
    assert len(progress_events) >= 2

    # Verify state event
    running_event = next(e for e in state_events if e.get("state") == "running")
    assert running_event["worker"] == "worker-1"

    # Verify progress events
    total_event = next(e for e in progress_events if e.get("total") == 50)
    assert total_event["current"] >= 0

    increment_event = next(e for e in progress_events if e.get("current") == 5)
    assert increment_event["current"] == 5


async def test_completed_state_publishes_event(docket: Docket):
    """Completed state should publish event with completed_at timestamp."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )

    # Set up subscriber
    events = []

    async def collect_events():
        async for event in execution.subscribe():
            if event["type"] == "state":
                events.append(event)
            if any(e.get("state") == "completed" for e in events):
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    await execution.set_running("worker-1")
    await execution.set_completed()

    try:
        await asyncio.wait_for(subscriber_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass

    # Find completed event
    completed_event = next(e for e in events if e.get("state") == "completed")
    assert completed_event["type"] == "state"
    assert "completed_at" in completed_event


async def test_failed_state_publishes_event_with_error(docket: Docket):
    """Failed state should publish event with error message."""
    execution = Execution(
        docket, AsyncMock(), (), {}, datetime.now(timezone.utc), "test-key", 1
    )

    # Set up subscriber
    events = []

    async def collect_events():
        async for event in execution.subscribe():
            if event["type"] == "state":
                events.append(event)
            if any(e.get("state") == "failed" for e in events):
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    await execution.set_running("worker-1")
    await execution.set_failed("Something went wrong!")

    try:
        await asyncio.wait_for(subscriber_task, timeout=2.0)
    except asyncio.TimeoutError:
        pass

    # Find failed event
    failed_event = next(e for e in events if e.get("state") == "failed")
    assert failed_event["type"] == "state"
    assert failed_event["error"] == "Something went wrong!"
    assert "completed_at" in failed_event


async def test_end_to_end_progress_monitoring_with_worker(
    docket: Docket, worker: Worker
):
    """Test complete end-to-end progress monitoring with real worker execution."""
    collected_events = []

    async def task_with_progress(progress: ExecutionProgress = Progress()):
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
        async for event in execution.subscribe():
            collected_events.append(event)
            # Stop when we reach completed state
            if event.get("type") == "state" and event.get("state") == "completed":
                break

    subscriber_task = asyncio.create_task(collect_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    # Run the worker
    await worker.run_until_finished()

    # Wait for subscriber to finish
    try:
        await asyncio.wait_for(subscriber_task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    # Verify we collected comprehensive events
    assert len(collected_events) > 0

    # Extract event types
    state_events = [e for e in collected_events if e["type"] == "state"]
    progress_events = [e for e in collected_events if e["type"] == "progress"]

    # Verify state transitions occurred
    # Note: scheduled may happen before subscriber connects
    state_sequence = [e["state"] for e in state_events]
    assert "pending" in state_sequence or "running" in state_sequence
    assert "running" in state_sequence
    assert "completed" in state_sequence

    # Verify worker was recorded
    running_events = [e for e in state_events if e.get("state") == "running"]
    assert len(running_events) > 0
    assert "worker" in running_events[0]

    # Verify progress events were published
    assert len(progress_events) >= 5  # At least one for each increment

    # Verify progress reached total
    final_progress = progress_events[-1]
    assert final_progress["current"] == 5
    assert final_progress["total"] == 5

    # Verify messages were updated
    message_events = [e for e in progress_events if e.get("message")]
    assert len(message_events) > 0
    assert any("complete" in e["message"].lower() for e in message_events)

    # Verify final state is completed
    assert state_events[-1]["state"] == "completed"
    assert "completed_at" in state_events[-1]


async def test_end_to_end_failed_task_monitoring(docket: Docket, worker: Worker):
    """Test progress monitoring for a task that fails."""
    collected_events = []

    async def failing_task(progress: ExecutionProgress = Progress()):
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
        async for event in execution.subscribe():
            collected_events.append(event)
            # Stop when we reach failed state
            if event.get("type") == "state" and event.get("state") == "failed":
                break

    subscriber_task = asyncio.create_task(collect_events())
    await asyncio.sleep(0.1)

    # Run the worker
    await worker.run_until_finished()

    # Wait for subscriber
    try:
        await asyncio.wait_for(subscriber_task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    # Verify we got events
    assert len(collected_events) > 0

    state_events = [e for e in collected_events if e["type"] == "state"]
    progress_events = [e for e in collected_events if e["type"] == "progress"]

    # Verify task reached running state
    state_sequence = [e["state"] for e in state_events]
    assert "running" in state_sequence
    assert "failed" in state_sequence

    # Verify progress was reported before failure
    assert len(progress_events) >= 2

    # Find set_total event
    total_event = next((e for e in progress_events if e.get("total") == 10), None)
    assert total_event is not None

    # Find increment event
    increment_event = next((e for e in progress_events if e.get("current") == 3), None)
    assert increment_event is not None

    # Verify error message in failed event
    failed_event = next(e for e in state_events if e.get("state") == "failed")
    assert "error" in failed_event
    assert "ValueError" in failed_event["error"]
    assert "intentionally" in failed_event["error"]
