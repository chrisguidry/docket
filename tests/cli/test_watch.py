"""Tests for the docket watch CLI command."""

import asyncio
from datetime import datetime, timedelta, timezone
import os
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Progress, Worker
from docket.execution import ExecutionState
from tests._key_leak_checker import KeyCountChecker

from .run import run_cli
from .waiting import (
    wait_for_execution_state,
    wait_for_progress_data,
    wait_for_watch_subscribed,
    wait_for_worker_assignment,
)

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


async def test_watch_completed_task(docket: Docket, the_task: AsyncMock):
    """Watch should display completed task and exit immediately."""
    docket.register(the_task)

    # Create and complete a task
    execution = await docket.add(the_task, key="completed-task")()
    await execution.claim("worker-1")
    await execution.mark_as_completed()

    # Watch should show completion and exit
    result = await run_cli(
        "watch",
        "completed-task",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )

    assert result.exit_code == 0
    assert "completed-task" in result.output
    assert "COMPLETED" in result.output.upper()
    assert docket.name in result.output
    assert "✓" in result.output or "completed successfully" in result.output.lower()


async def test_watch_failed_task(docket: Docket, the_task: AsyncMock):
    """Watch should display failed task with error message."""
    docket.register(the_task)

    execution = await docket.add(the_task, key="failed-task")()
    await execution.claim("worker-1")
    await execution.mark_as_failed("Test error message")

    result = await run_cli(
        "watch",
        "failed-task",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )

    assert result.exit_code == 0
    assert "failed-task" in result.output
    assert "FAILED" in result.output.upper()
    assert docket.name in result.output
    assert "✗" in result.output or "failed" in result.output.lower()
    assert "Test error message" in result.output or "Error" in result.output


async def test_watch_running_task_until_completion(docket: Docket, worker: Worker):
    """Watch should monitor task from running to completion."""

    # Coordination key for synchronization
    ready_key = f"{docket.name}:test:watch:ready"

    async def coordinated_task():
        # Wait for watch to be subscribed before completing
        async with docket.redis() as redis:
            while not await redis.get(ready_key):  # type: ignore[misc]
                await asyncio.sleep(0.01)
        # Now do the work
        await asyncio.sleep(0.5)

    docket.register(coordinated_task)
    await docket.add(coordinated_task, key="slower-task")()

    # Start worker in background
    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for worker to claim the task
    await wait_for_execution_state(docket, "slower-task", ExecutionState.RUNNING)

    # Start watch subprocess in background
    watch_task = asyncio.create_task(
        run_cli(
            "watch",
            "slower-task",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            timeout=10.0,
        )
    )

    # Wait for watch to subscribe (deterministic synchronization)
    await wait_for_watch_subscribed(docket, "slower-task")

    # Signal task it can complete now
    async with docket.redis() as redis:
        await redis.set(ready_key, "1", ex=10)

    # Wait for watch to finish
    result = await watch_task
    await worker_task

    assert result.exit_code == 0
    assert "slower-task" in result.output
    assert docket.name in result.output
    assert "RUNNING" in result.output.upper() or "COMPLETED" in result.output.upper()


async def test_watch_with_progress_updates(docket: Docket, worker: Worker):
    """Watch should display progress bar updates."""

    async def task_with_progress(progress: Progress = Progress()):
        await progress.set_total(10)
        await progress.set_message("Starting")
        for i in range(10):
            # Slower so watch has time to connect and receive events
            await asyncio.sleep(0.1)
            await progress.increment()
            await progress.set_message(f"Step {i + 1}")

    docket.register(task_with_progress)
    await docket.add(task_with_progress, key="progress-task")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    result = await run_cli(
        "watch",
        "progress-task",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )

    await worker_task

    assert result.exit_code == 0
    assert "progress-task" in result.output
    assert docket.name in result.output
    # State should be shown
    assert "COMPLETED" in result.output.upper()


async def test_watch_scheduled_task_transition(docket: Docket, worker: Worker):
    """Watch should show task transition from scheduled to completed."""

    async def scheduled_task():
        await asyncio.sleep(0.01)

    docket.register(scheduled_task)

    # Schedule for near future
    when = datetime.now(timezone.utc) + timedelta(seconds=2)
    await docket.add(scheduled_task, when=when, key="scheduled-task")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    result = await run_cli(
        "watch",
        "scheduled-task",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "scheduled-task" in result.output
    assert docket.name in result.output
    # Should show final completed state
    assert "COMPLETED" in result.output.upper() or "SCHEDULED" in result.output.upper()


async def test_watch_task_with_initial_progress(docket: Docket, worker: Worker):
    """Watch should handle task that already has progress when monitoring starts."""

    async def task_with_initial_progress(progress: Progress = Progress()):
        # Set progress before watch likely connects
        await progress.set_total(20)
        await progress.increment(5)
        # Then continue slowly
        for _ in range(15):
            await asyncio.sleep(0.1)
            await progress.increment()

    docket.register(task_with_initial_progress)
    await docket.add(task_with_initial_progress, key="initial-progress")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for task to report progress data
    await wait_for_progress_data(
        docket, "initial-progress", min_current=1, min_total=20
    )

    result = await run_cli(
        "watch",
        "initial-progress",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0


async def test_watch_task_with_worker_assignment(docket: Docket, worker: Worker):
    """Watch should show worker name when task is claimed."""

    async def long_running_task():
        # Long enough for watch to definitely connect
        await asyncio.sleep(3.0)

    docket.register(long_running_task)
    await docket.add(long_running_task, key="worker-assigned")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for worker to claim task
    await wait_for_worker_assignment(docket, "worker-assigned")

    result = await run_cli(
        "watch",
        "worker-assigned",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "worker-assigned" in result.output
    assert docket.name in result.output
    assert "Worker" in result.output or worker.name in result.output


async def test_watch_task_that_starts_while_watching(docket: Docket, worker: Worker):
    """Watch should receive started_at event and update progress bar timing."""

    async def task_that_waits_then_progresses(progress: Progress = Progress()):
        # Immediately report progress so watch sees it
        await progress.set_total(10)
        await progress.increment(1)
        await progress.set_message("Started")
        # Then continue
        for _ in range(9):
            await asyncio.sleep(0.15)
            await progress.increment()

    docket.register(task_that_waits_then_progresses)

    # Schedule task for slightly in future so watch can connect first
    when = datetime.now(timezone.utc) + timedelta(seconds=2)
    await docket.add(task_that_waits_then_progresses, when=when, key="timing-test")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    # Start watching BEFORE task starts
    result = await run_cli(
        "watch",
        "timing-test",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "timing-test" in result.output
    assert docket.name in result.output


async def test_watch_receives_progress_events_during_execution(
    docket: Docket, worker: Worker
):
    """Watch should receive and process progress events as they occur."""

    async def task_with_many_updates(progress: Progress = Progress()):
        await progress.set_total(20)
        for i in range(20):
            await asyncio.sleep(0.08)  # 1.6 seconds total
            await progress.increment()
            if i % 5 == 0:
                await progress.set_message(f"Checkpoint {i}")

    docket.register(task_with_many_updates)
    await docket.add(task_with_many_updates, key="many-updates")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for worker to start processing
    await wait_for_execution_state(docket, "many-updates", ExecutionState.RUNNING)

    result = await run_cli(
        "watch",
        "many-updates",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "many-updates" in result.output
    assert docket.name in result.output


async def test_watch_already_running_task_with_progress(docket: Docket, worker: Worker):
    """Watch a task that's already running with progress when watch starts."""

    async def task_already_running(progress: Progress = Progress()):
        # Set up some initial state quickly
        await progress.set_total(30)
        await progress.increment(10)
        await progress.set_message("Already started")
        # Then run slowly so watch can observe
        for _ in range(20):
            await asyncio.sleep(0.1)
            await progress.increment()

    docket.register(task_already_running)
    await docket.add(task_already_running, key="already-running")()

    # Start worker
    worker_task = asyncio.create_task(worker.run_until_finished())

    # Wait for task to start and report progress data
    await wait_for_execution_state(docket, "already-running", ExecutionState.RUNNING)
    await wait_for_progress_data(docket, "already-running", min_current=1, min_total=30)

    # Now start watching - task should already be RUNNING with progress
    result = await run_cli(
        "watch",
        "already-running",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "already-running" in result.output
    assert docket.name in result.output


async def test_watch_task_with_worker_in_state_event(docket: Docket, worker: Worker):
    """Watch should handle state events containing worker name."""

    async def task_with_delays(progress: Progress = Progress()):
        # Publish progress to create progress bar
        await progress.set_total(15)
        await progress.increment(1)
        # Long delay so watch receives events
        for _ in range(14):
            await asyncio.sleep(0.15)
            await progress.increment()

    docket.register(task_with_delays)

    # Schedule slightly in future
    when = datetime.now(timezone.utc) + timedelta(milliseconds=150)
    await docket.add(task_with_delays, when=when, key="worker-event")()

    worker_task = asyncio.create_task(worker.run_until_finished())

    # Start watch early so it's listening when task starts
    result = await run_cli(
        "watch",
        "worker-event",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        timeout=10.0,
    )

    await worker_task

    assert result.exit_code == 0
    assert "worker-event" in result.output
    assert docket.name in result.output


async def test_watch_task_with_incomplete_data(
    docket: Docket, key_leak_checker: KeyCountChecker
):
    """Watch should show error when task has incomplete data in Redis."""
    # This test manually creates incomplete test data
    key_leak_checker.add_exemption(f"{docket.name}:runs:incomplete-task")

    # Manually create runs hash with incomplete data (missing function/args/kwargs)
    async with docket.redis() as redis:
        runs_key = f"{docket.name}:runs:incomplete-task"
        await redis.hset(runs_key, mapping={"state": "scheduled"})  # type: ignore[misc]

    result = await run_cli(
        "watch",
        "incomplete-task",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )

    assert result.exit_code == 0
    assert "Error" in result.output
    assert "not found" in result.output or "incomplete-task" in result.output
