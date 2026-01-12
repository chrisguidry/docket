"""Tests for basic progress tracking operations."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Execution, ExecutionState, Progress, Worker
from docket.execution import ExecutionProgress


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


async def test_progress_with_multiple_increments(docket: Docket, worker: Worker):
    """Test progress tracking with realistic usage pattern."""

    async def process_items(items: list[int], progress: Progress = Progress()):
        await progress.set_total(len(items))
        await progress.set_message("Starting processing")

        for i in range(len(items)):
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
