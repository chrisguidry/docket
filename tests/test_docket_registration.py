"""Tests for Docket task registration."""

import asyncio
from docket.docket import Docket
from docket.worker import Worker


def test_standard_tasks_available_after_init():
    """Standard tasks (trace, fail, sleep) should be available after __init__."""
    docket = Docket(name="test-standard-tasks", url="memory://")

    assert "trace" in docket.tasks
    assert "fail" in docket.tasks
    assert "sleep" in docket.tasks


def test_register_task_before_aenter():
    """Tasks can be registered before entering the async context manager."""
    docket = Docket(name="test-pre-register", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


async def test_registered_task_usable_after_aenter():
    """Tasks registered before __aenter__ should be usable inside the context."""
    docket = Docket(name="test-pre-register-usable", url="memory://")

    async def my_task(_value: str) -> None: ...

    docket.register(my_task)

    async with docket:
        assert "my_task" in docket.tasks
        execution = await docket.add(my_task)("test-value")
        assert execution.function is my_task
        assert execution.args == ("test-value",)


async def test_tasks_persist_after_aexit():
    """Task registry should persist after exiting the async context."""
    docket = Docket(name="test-persist-after-exit", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    async with docket:
        ...

    # Tasks should still be there after exit
    assert "my_task" in docket.tasks
    assert "trace" in docket.tasks


async def test_docket_reentry_preserves_tasks():
    """Re-entering the docket should preserve both user and standard tasks."""
    docket = Docket(name="test-reentry", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task)

    # First entry/exit
    async with docket:
        assert "my_task" in docket.tasks
        assert "trace" in docket.tasks

    # Re-entry should still have all tasks
    async with docket:
        assert "my_task" in docket.tasks
        assert "trace" in docket.tasks


def test_register_task_with_custom_name():
    """Tasks can be registered under a custom name instead of __name__."""
    docket = Docket(name="test-custom-name", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=["custom_name"])

    assert "custom_name" in docket.tasks
    assert docket.tasks["custom_name"] is my_task
    # Original name should NOT be registered
    assert "my_task" not in docket.tasks


def test_register_task_with_multiple_names():
    """Tasks can be registered under multiple names."""
    docket = Docket(name="test-multiple-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=["alias_a", "alias_b", "alias_c"])

    assert "alias_a" in docket.tasks
    assert "alias_b" in docket.tasks
    assert "alias_c" in docket.tasks
    assert docket.tasks["alias_a"] is my_task
    assert docket.tasks["alias_b"] is my_task
    assert docket.tasks["alias_c"] is my_task
    # Original name should NOT be registered
    assert "my_task" not in docket.tasks


def test_register_task_with_empty_names_defaults_to_function_name():
    """Empty names list should default to function.__name__."""
    docket = Docket(name="test-empty-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=[])

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


def test_register_task_with_none_names_defaults_to_function_name():
    """None names should default to function.__name__."""
    docket = Docket(name="test-none-names", url="memory://")

    async def my_task() -> None: ...

    docket.register(my_task, names=None)

    assert "my_task" in docket.tasks
    assert docket.tasks["my_task"] is my_task


async def test_schedule_task_by_alias(docket: Docket, worker: Worker):
    """Tasks can be scheduled by their alias name."""
    results: list[str] = []

    async def my_task(value: str) -> None:
        results.append(value)

    docket.register(my_task, names=["task_alias"])

    await docket.add("task_alias")("hello")
    await worker.run_until_finished()

    assert results == ["hello"]


async def test_alias_appears_in_worker_announcements(docket: Docket):
    """Alias names should appear in worker task announcements."""

    async def my_task() -> None: ...

    docket.register(my_task, names=["custom_alias"])

    async with Worker(docket) as w:
        await asyncio.sleep(0.1)  # Let heartbeat fire
        workers = await docket.task_workers("custom_alias")
        assert len(workers) == 1
        assert w.name in {worker.name for worker in workers}
