"""Tests for progress tracking and execution state."""

import asyncio
from datetime import datetime, timedelta
from typing import Callable

from docket import (
    CurrentExecution,
    Docket,
    Execution,
    ExecutionState,
    Progress,
    Worker,
)
from docket.execution import StateEvent


async def test_tasks_can_report_progress(docket: Docket, worker: Worker):
    """docket should support tasks reporting their progress"""

    called = False

    async def the_task(
        a: str,
        b: str,
        progress: Progress = Progress(),
    ):
        assert a == "a"
        assert b == "c"

        # Set the total expected work
        await progress.set_total(100)

        # Increment progress
        await progress.increment(10)
        await progress.increment(20)

        # Set a status message
        await progress.set_message("Processing items...")

        # Read back current progress
        assert progress.current == 30
        assert progress.total == 100
        assert progress.message == "Processing items..."

        nonlocal called
        called = True

    await docket.add(the_task, key="progress-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_tasks_can_access_execution_state(docket: Docket, worker: Worker):
    """docket should support providing execution state and metadata to a task"""

    called = False

    async def the_task(
        a: str,
        b: str,
        this_execution: Execution = CurrentExecution(),
    ):
        assert a == "a"
        assert b == "c"

        assert isinstance(this_execution, Execution)
        assert this_execution.key == "stateful-task:123"
        assert this_execution.state == ExecutionState.RUNNING
        assert this_execution.worker is not None
        assert this_execution.started_at is not None

        nonlocal called
        called = True

    await docket.add(the_task, key="stateful-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_execution_state_lifecycle(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket executions transition through states: QUEUED → RUNNING → COMPLETED"""

    async def successful_task():
        await asyncio.sleep(0.01)

    async def failing_task():
        await asyncio.sleep(0.01)
        raise ValueError("Task failed")

    # Test successful execution lifecycle
    execution = await docket.add(
        successful_task, key="success:123", when=now() + timedelta(seconds=1)
    )()

    # Collect state events
    state_events: list[StateEvent] = []

    async def collect_states() -> None:
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                state_events.append(event)
                if event["state"] == ExecutionState.COMPLETED:
                    break

    subscriber_task = asyncio.create_task(collect_states())

    await worker.run_until_finished()
    await asyncio.wait_for(subscriber_task, timeout=7.0)

    # Verify we saw the state transitions
    # Note: subscribe() emits the initial state first, then real-time updates
    states = [e["state"] for e in state_events]
    assert states == [
        ExecutionState.SCHEDULED,
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.COMPLETED,
    ]

    # Verify final state has completion metadata
    final_state = state_events[-1]
    assert final_state["state"] == ExecutionState.COMPLETED
    assert final_state["completed_at"] is not None
    assert "error" not in final_state  # No error for successful completion

    # Test failed execution lifecycle
    execution = await docket.add(
        failing_task, key="failure:456", when=now() + timedelta(seconds=1)
    )()

    failed_state_events: list[StateEvent] = []

    async def collect_failed_states() -> None:
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                failed_state_events.append(event)
                if event["state"] == ExecutionState.FAILED:
                    break

    subscriber_task = asyncio.create_task(collect_failed_states())

    await worker.run_until_finished()
    await asyncio.wait_for(subscriber_task, timeout=7.0)

    # Verify we saw the state transitions
    # Note: subscribe() emits the initial state first, then real-time updates
    states = [e["state"] for e in failed_state_events]
    assert states == [
        ExecutionState.SCHEDULED,
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.FAILED,
    ]

    # Verify final state has error information
    final_state = failed_state_events[-1]
    assert final_state["state"] == ExecutionState.FAILED
    assert final_state["completed_at"] is not None
    assert final_state["error"] is not None
    assert final_state["error"] == "ValueError: Task failed"
