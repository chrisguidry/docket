"""Tests for sync dependencies, context managers, and contextvar isolation."""

import logging
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

import pytest

from docket import CurrentDocket, Docket, Worker
from docket.dependencies import Depends, Dependency, resolved_dependencies
from docket.dependencies._functional import _Depends  # pyright: ignore[reportPrivateUsage]
from docket.execution import Execution


async def test_sync_function_dependency(docket: Docket, worker: Worker):
    """A task can depend on a synchronous function"""
    called = False

    def sync_dependency() -> str:
        return "sync-value"

    async def dependent_task(value: str = Depends(sync_dependency)):
        assert value == "sync-value"
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called


async def test_sync_context_manager_dependency(docket: Docket, worker: Worker):
    """A task can depend on a synchronous context manager"""
    called = False
    stages: list[str] = []

    @contextmanager
    def sync_cm_dependency():
        stages.append("before")
        yield "sync-cm-value"
        stages.append("after")

    async def dependent_task(value: str = Depends(sync_cm_dependency)):
        assert value == "sync-cm-value"
        stages.append("during")
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called
    assert stages == ["before", "during", "after"]


async def test_mixed_sync_and_async_dependencies(docket: Docket, worker: Worker):
    """A task can depend on both sync and async dependencies"""
    called = False

    def sync_dependency() -> str:
        return "sync"

    async def async_dependency() -> str:
        return "async"

    @contextmanager
    def sync_cm():
        yield "sync-cm"

    async def dependent_task(
        sync_val: str = Depends(sync_dependency),
        async_val: str = Depends(async_dependency),
        sync_cm_val: str = Depends(sync_cm),
    ):
        assert sync_val == "sync"
        assert async_val == "async"
        assert sync_cm_val == "sync-cm"
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called


async def test_nested_sync_dependencies(docket: Docket, worker: Worker):
    """A sync dependency can depend on another sync dependency"""
    called = False

    def base_dependency() -> int:
        return 10

    def derived_dependency(base: int = Depends(base_dependency)) -> int:
        return base * 2

    async def dependent_task(value: int = Depends(derived_dependency)):
        assert value == 20
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called


async def test_sync_dependency_with_docket_context(docket: Docket, worker: Worker):
    """A sync dependency can access docket context"""
    called = False

    def sync_dep_with_context(d: Docket = CurrentDocket()) -> str:
        assert d is docket
        return d.name

    async def dependent_task(name: str = Depends(sync_dep_with_context)):
        assert name == docket.name
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called


async def test_sync_context_manager_cleanup_on_exception(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A sync context manager's cleanup runs even when the task fails"""
    stages: list[str] = []

    @contextmanager
    def sync_cm_with_cleanup():
        stages.append("enter")
        try:
            yield "value"
        finally:
            stages.append("exit")

    async def failing_task(value: str = Depends(sync_cm_with_cleanup)):
        stages.append("task")
        raise ValueError("Task failed")

    await docket.add(failing_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert stages == ["enter", "task", "exit"]
    assert "ValueError: Task failed" in caplog.text


async def test_sync_dependency_caching(docket: Docket, worker: Worker):
    """Sync dependencies are cached and only called once per task"""
    call_count = 0

    def counted_dependency() -> str:
        nonlocal call_count
        call_count += 1
        return f"call-{call_count}"

    async def dependent_task(
        val_a: str = Depends(counted_dependency),
        val_b: str = Depends(counted_dependency),
    ):
        # Both should be the same value since it's cached
        assert val_a == val_b
        assert val_a == "call-1"

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert call_count == 1


async def test_mixed_nested_dependencies(docket: Docket, worker: Worker):
    """Complex nesting with mixed sync and async dependencies"""
    called = False

    def sync_base() -> int:
        return 5

    async def async_multiplier(base: int = Depends(sync_base)) -> int:
        return base * 3

    def sync_adder(multiplied: int = Depends(async_multiplier)) -> int:
        return multiplied + 10

    async def dependent_task(result: int = Depends(sync_adder)):
        # 5 * 3 + 10 = 25
        assert result == 25
        nonlocal called
        called = True

    await docket.add(dependent_task)()
    await worker.run_until_finished()

    assert called


async def test_contextvar_isolation_between_tasks(docket: Docket, worker: Worker):
    """Contextvars should be isolated between sequential task executions"""
    executions_seen: list[tuple[str, Execution]] = []

    async def first_task(a: str):
        # Capture the execution context during first task
        execution = Dependency.execution.get()
        executions_seen.append(("first", execution))
        assert a == "first"

    async def second_task(b: str):
        # Capture the execution context during second task
        execution = Dependency.execution.get()
        executions_seen.append(("second", execution))
        assert b == "second"

    await docket.add(first_task)(a="first")
    await docket.add(second_task)(b="second")
    await worker.run_until_finished()

    # Verify we captured both executions
    assert len(executions_seen) == 2

    # Find first and second executions (order may vary)
    executions_by_name = {name: exec for name, exec in executions_seen}
    assert set(executions_by_name.keys()) == {"first", "second"}

    # Verify the executions are different and have correct kwargs
    first_execution = executions_by_name["first"]
    second_execution = executions_by_name["second"]
    assert first_execution is not second_execution
    assert first_execution.kwargs["a"] == "first"
    assert second_execution.kwargs["b"] == "second"


async def test_contextvar_cleanup_after_task(docket: Docket, worker: Worker):
    """Task-scoped contextvars are reset after task execution completes.

    Worker-scoped contextvars (Dependency.docket, Dependency.worker) remain
    set for the entire worker lifetime to support Shared dependencies.
    """
    captured_stack = None
    captured_cache = None

    async def capture_task():
        nonlocal captured_stack, captured_cache
        # Capture references during task execution
        captured_stack = _Depends.stack.get()
        captured_cache = _Depends.cache.get()

    await docket.add(capture_task)()
    await worker.run_until_finished()

    # Task-scoped contextvars should be reset after the task completes
    with pytest.raises(LookupError):
        _Depends.stack.get()

    with pytest.raises(LookupError):
        _Depends.cache.get()

    with pytest.raises(LookupError):
        Dependency.execution.get()

    # Worker-scoped contextvars (docket, worker) remain set for the worker's
    # lifetime to support Shared dependency initialization
    assert Dependency.docket.get() is docket
    assert Dependency.worker.get() is worker


async def test_dependency_cache_isolated_between_tasks(docket: Docket, worker: Worker):
    """Dependency cache should be fresh for each task, not reused"""
    call_counts = {"task1": 0, "task2": 0}

    def dependency_for_task1() -> str:
        call_counts["task1"] += 1
        return f"task1-call-{call_counts['task1']}"

    def dependency_for_task2() -> str:
        call_counts["task2"] += 1
        return f"task2-call-{call_counts['task2']}"

    async def first_task(val: str = Depends(dependency_for_task1)):
        assert val == "task1-call-1"

    async def second_task(val: str = Depends(dependency_for_task2)):
        assert val == "task2-call-1"

    # Run tasks sequentially
    await docket.add(first_task)()
    await worker.run_until_finished()

    await docket.add(second_task)()
    await worker.run_until_finished()

    # Each dependency should have been called once (no cache leakage between tasks)
    assert call_counts["task1"] == 1
    assert call_counts["task2"] == 1


async def test_async_exit_stack_cleanup(docket: Docket, worker: Worker):
    """AsyncExitStack should be properly cleaned up after task execution"""
    cleanup_called: list[str] = []

    @asynccontextmanager
    async def tracked_resource():
        try:
            yield "resource"
        finally:
            cleanup_called.append("cleaned")

    async def task_with_context(res: str = Depends(tracked_resource)):
        assert res == "resource"
        assert len(cleanup_called) == 0  # Not cleaned up yet

    await docket.add(task_with_context)()
    await worker.run_until_finished()

    # After task completes, cleanup should have been called
    assert cleanup_called == ["cleaned"]


async def test_contextvar_reset_on_reentrant_call(docket: Docket, worker: Worker):
    """Contextvars should be properly reset on reentrant calls to resolved_dependencies"""

    # Create two mock executions
    async def task1(): ...

    async def task2(): ...

    execution1 = Execution(
        docket=docket,
        function=task1,
        args=(),
        kwargs={},
        key="task1-key",
        when=datetime.now(timezone.utc),
        attempt=1,
    )

    execution2 = Execution(
        docket=docket,
        function=task2,
        args=(),
        kwargs={},
        key="task2-key",
        when=datetime.now(timezone.utc),
        attempt=1,
    )

    # Capture contextvars from first call
    captured_exec1 = None
    captured_stack1 = None

    async with resolved_dependencies(worker, execution1):
        captured_exec1 = Dependency.execution.get()
        captured_stack1 = _Depends.stack.get()
        assert captured_exec1 is execution1

    # After exiting, contextvars should be reset (raise LookupError)
    with pytest.raises(LookupError):
        Dependency.execution.get()

    # Now make a second call - should not see values from first call
    async with resolved_dependencies(worker, execution2):
        captured_exec2 = Dependency.execution.get()
        captured_stack2 = _Depends.stack.get()
        assert captured_exec2 is execution2
        assert captured_exec2 is not captured_exec1
        # Stacks should be different objects
        assert captured_stack2 is not captured_stack1


async def test_contextvar_not_leaked_to_caller(docket: Docket):
    """Verify contextvars don't leak outside resolved_dependencies context"""
    # Before calling resolved_dependencies, contextvars should not be set
    with pytest.raises(LookupError):
        Dependency.execution.get()

    async def dummy_task(): ...

    execution = Execution(
        docket=docket,
        function=dummy_task,
        args=(),
        kwargs={},
        key="test-key",
        when=datetime.now(timezone.utc),
        attempt=1,
    )

    async with Docket("test-contextvar-leak", url="memory://leak-test") as test_docket:
        async with Worker(test_docket) as test_worker:
            # Use resolved_dependencies
            async with resolved_dependencies(test_worker, execution):
                # Inside context, we should be able to get values
                assert Dependency.execution.get() is execution

            # After exiting context, contextvars should be cleaned up
            with pytest.raises(LookupError):
                Dependency.execution.get()

            with pytest.raises(LookupError):
                _Depends.stack.get()

            with pytest.raises(LookupError):  # pragma: no branch
                _Depends.cache.get()
