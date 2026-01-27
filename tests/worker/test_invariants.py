"""Tests for worker internal state invariants.

These tests verify that internal data structures are properly cleaned up after
task completion and context exit, catching memory leaks early in CI.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest

from docket import CurrentExecution, Docket, Perpetual, TaskLogger, Timeout, Worker
from docket.dependencies import SharedContext
from docket.execution import Execution


async def test_invariant_tasks_by_key_empty_after_completion(docket: Docket):
    """After run_until_finished, _tasks_by_key should be empty (all tasks done)."""

    async def simple_task():
        pass

    docket.register(simple_task)

    async with Worker(docket, concurrency=4) as worker:
        for _ in range(50):
            await docket.add(simple_task)()

        await worker.run_until_finished()

        # After completion, _tasks_by_key should be empty
        assert len(worker._tasks_by_key) == 0  # type: ignore[protected-access]


async def test_invariant_tasks_by_key_no_growth_over_batches(docket: Docket):
    """Running multiple batches should not accumulate entries in _tasks_by_key."""

    async def simple_task():
        pass

    docket.register(simple_task)

    async with Worker(docket, concurrency=4) as worker:
        for batch in range(5):
            for _ in range(20):
                await docket.add(simple_task)()
            await worker.run_until_finished()

            # After each batch, verify cleanup
            assert len(worker._tasks_by_key) == 0, f"Leak after batch {batch}"  # type: ignore[protected-access]


async def test_invariant_execution_counts_empty_after_completion(docket: Docket):
    """_execution_counts should be empty after normal run_until_finished (no run_at_most)."""

    async def simple_task():
        pass

    docket.register(simple_task)

    async with Worker(docket, concurrency=4) as worker:
        for _ in range(10):
            await docket.add(simple_task)()

        await worker.run_until_finished()

        # After normal completion, _execution_counts should be empty
        assert len(worker._execution_counts) == 0  # type: ignore[protected-access]


async def test_invariant_execution_counts_cleared_after_run_at_most(docket: Docket):
    """_execution_counts should be cleared after run_at_most completes."""
    iteration_count = 0

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ):
        nonlocal iteration_count
        iteration_count += 1

    docket.register(perpetual_task)
    await docket.add(perpetual_task, key="test-perpetual")()

    async with Worker(docket, concurrency=1) as worker:
        await worker.run_at_most({"test-perpetual": 3})

        # run_at_most clears _execution_counts in its finally block
        assert len(worker._execution_counts) == 0  # type: ignore[protected-access]

    assert iteration_count == 3


async def test_invariant_worker_attributes_deleted_after_exit(docket: Docket):
    """Worker internal attributes should be deleted after context exit."""
    worker = Worker(docket)
    await worker.__aenter__()

    # Attributes exist during context
    assert hasattr(worker, "_tasks_by_key")
    assert hasattr(worker, "_execution_counts")
    assert hasattr(worker, "_worker_stopping")
    assert hasattr(worker, "_worker_done")
    assert hasattr(worker, "_cancellation_ready")
    assert hasattr(worker, "_heartbeat_task")
    assert hasattr(worker, "_shared_context")

    await worker.__aexit__(None, None, None)

    # Attributes cleaned up after exit
    assert not hasattr(worker, "_tasks_by_key")
    assert not hasattr(worker, "_execution_counts")
    assert not hasattr(worker, "_worker_stopping")
    assert not hasattr(worker, "_worker_done")
    assert not hasattr(worker, "_cancellation_ready")
    assert not hasattr(worker, "_heartbeat_task")
    assert not hasattr(worker, "_shared_context")
    assert not hasattr(worker, "_stack")


async def test_invariant_cleanup_after_task_exceptions(docket: Docket):
    """_tasks_by_key should be cleaned up even when tasks raise exceptions."""

    async def failing_task():
        raise ValueError("intentional failure")

    docket.register(failing_task)

    async with Worker(docket, concurrency=4) as worker:
        for _ in range(10):
            await docket.add(failing_task)()

        await worker.run_until_finished()

        # Even with failures, _tasks_by_key should be empty
        assert len(worker._tasks_by_key) == 0  # type: ignore[protected-access]


async def test_invariant_cleanup_with_varied_tasks(docket: Docket):
    """Cleanup should work with all task types: deps, timeouts, returns, kwargs."""

    async def simple_task():
        pass

    async def task_with_deps(
        execution: Execution = CurrentExecution(),
        logger: logging.LoggerAdapter[logging.Logger] = TaskLogger(),
    ):
        logger.info(f"Running {execution.key}")

    async def task_with_timeout(
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ):
        await asyncio.sleep(0.01)

    async def task_with_return() -> str:
        return "result"

    async def task_with_kwargs(a: int, b: str = "default"):
        pass

    for task in [
        simple_task,
        task_with_deps,
        task_with_timeout,
        task_with_return,
        task_with_kwargs,
    ]:
        docket.register(task)

    async with Worker(docket, concurrency=4) as worker:
        # Add varied tasks
        for _ in range(5):
            await docket.add(simple_task)()
            await docket.add(task_with_deps)()
            await docket.add(task_with_timeout)()
            await docket.add(task_with_return)()
            await docket.add(task_with_kwargs)(a=1, b="test")

        await worker.run_until_finished()

        # Verify cleanup regardless of task type
        assert len(worker._tasks_by_key) == 0  # type: ignore[protected-access]


async def test_invariant_shared_context_reset_after_worker_exit(docket: Docket):
    """SharedContext ContextVars should be reset after worker exits."""
    worker = Worker(docket)
    await worker.__aenter__()

    # During context, resolved should have a value
    resolved = SharedContext.resolved.get()
    assert isinstance(resolved, dict)

    await worker.__aexit__(None, None, None)

    # After exit, resolved should be reset (LookupError since no value was set before)
    with pytest.raises(LookupError):
        SharedContext.resolved.get()
