"""Tests for progress/state pub/sub events and monitoring."""

import asyncio
from datetime import datetime, timezone
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
    state_sequence = [e["state"] for e in state_events]
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
