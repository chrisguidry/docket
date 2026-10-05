"""Tests for the public docket.testing API.

This module tests the user-facing testing utilities that help users write assertions
about scheduled tasks in their own test suites.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Worker
from docket.testing import (
    assert_no_tasks,
    assert_task_count,
    assert_task_not_scheduled,
    assert_task_scheduled,
)


@pytest.fixture
def simple_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "simple_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    return task


@pytest.fixture
def another_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "another_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    return task


async def test_assert_task_scheduled_finds_task_by_function_only(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should find a task by function alone."""
    await docket.add(simple_task)("arg1", kwarg1="value1")

    await assert_task_scheduled(docket, simple_task)


async def test_assert_task_scheduled_finds_task_by_function_and_args(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should find a task by function and args."""
    await docket.add(simple_task)("arg1", "arg2", kwarg1="value1")

    await assert_task_scheduled(docket, simple_task, args=("arg1", "arg2"))


async def test_assert_task_scheduled_finds_task_by_function_and_kwargs(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should find a task by function and kwargs."""
    await docket.add(simple_task)("arg1", kwarg1="value1", kwarg2="value2")

    await assert_task_scheduled(
        docket, simple_task, kwargs={"kwarg1": "value1", "kwarg2": "value2"}
    )


async def test_assert_task_scheduled_finds_task_by_function_args_and_kwargs(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should find a task by function, args, and kwargs."""
    await docket.add(simple_task)("arg1", "arg2", kwarg1="value1")

    await assert_task_scheduled(
        docket, simple_task, args=("arg1", "arg2"), kwargs={"kwarg1": "value1"}
    )


async def test_assert_task_scheduled_finds_task_by_key(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should find a task by key."""
    await docket.add(simple_task, key="my-task-key")("arg1")

    await assert_task_scheduled(docket, simple_task, key="my-task-key")


async def test_assert_task_scheduled_works_with_function_name(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should work with function name as string."""
    docket.register(simple_task)
    await docket.add("simple_task")("arg1")

    await assert_task_scheduled(docket, "simple_task")


async def test_assert_task_scheduled_succeeds_with_multiple_matching_tasks(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should succeed if at least one task matches."""
    await docket.add(simple_task)("arg1", kwarg1="value1")
    await docket.add(simple_task)("arg2", kwarg1="value2")
    await docket.add(simple_task)("arg3", kwarg1="value3")

    await assert_task_scheduled(docket, simple_task, args=("arg2",))


async def test_assert_task_scheduled_fails_when_task_not_found(
    docket: Docket, simple_task: AsyncMock, another_task: AsyncMock
):
    """assert_task_scheduled should fail with clear error when task not found."""
    await docket.add(another_task)("arg1")

    with pytest.raises(AssertionError, match="simple_task.*not found"):
        await assert_task_scheduled(docket, simple_task)


async def test_assert_task_scheduled_fails_when_args_dont_match(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should fail when args don't match."""
    await docket.add(simple_task)("arg1", "arg2")

    with pytest.raises(AssertionError, match="args.*arg3.*arg4"):
        await assert_task_scheduled(docket, simple_task, args=("arg3", "arg4"))


async def test_assert_task_scheduled_fails_when_kwargs_dont_match(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should fail when kwargs don't match."""
    await docket.add(simple_task)(kwarg1="value1")

    with pytest.raises(AssertionError, match="kwargs.*kwarg2"):
        await assert_task_scheduled(docket, simple_task, kwargs={"kwarg2": "value2"})


async def test_assert_task_scheduled_finds_scheduled_future_task(
    docket: Docket, simple_task: AsyncMock, now: Callable[[], datetime]
):
    """assert_task_scheduled should find tasks scheduled in the future."""
    later = now() + timedelta(seconds=10)
    await docket.add(simple_task, when=later)("arg1")

    await assert_task_scheduled(docket, simple_task)


async def test_assert_task_not_scheduled_succeeds_when_no_task(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_not_scheduled should succeed when task not scheduled."""
    await assert_task_not_scheduled(docket, simple_task)


async def test_assert_task_not_scheduled_succeeds_when_different_task(
    docket: Docket, simple_task: AsyncMock, another_task: AsyncMock
):
    """assert_task_not_scheduled should succeed when different task is scheduled."""
    await docket.add(another_task)("arg1")

    await assert_task_not_scheduled(docket, simple_task)


async def test_assert_task_not_scheduled_fails_when_task_exists(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_not_scheduled should fail when task is scheduled."""
    await docket.add(simple_task)("arg1")

    with pytest.raises(AssertionError, match="simple_task.*found.*should not"):
        await assert_task_not_scheduled(docket, simple_task)


async def test_assert_task_not_scheduled_with_specific_args(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_not_scheduled should check specific args."""
    await docket.add(simple_task)("arg1")

    # Should succeed - different args
    await assert_task_not_scheduled(docket, simple_task, args=("arg2",))

    # Should fail - same args
    with pytest.raises(AssertionError):
        await assert_task_not_scheduled(docket, simple_task, args=("arg1",))


async def test_assert_task_count_all_tasks(
    docket: Docket, simple_task: AsyncMock, another_task: AsyncMock
):
    """assert_task_count should count all tasks when no function specified."""
    await docket.add(simple_task)("arg1")
    await docket.add(simple_task)("arg2")
    await docket.add(another_task)("arg3")

    await assert_task_count(docket, count=3)


async def test_assert_task_count_for_specific_function(
    docket: Docket, simple_task: AsyncMock, another_task: AsyncMock
):
    """assert_task_count should count tasks for a specific function."""
    await docket.add(simple_task)("arg1")
    await docket.add(simple_task)("arg2")
    await docket.add(another_task)("arg3")

    await assert_task_count(docket, simple_task, count=2)
    await assert_task_count(docket, another_task, count=1)


async def test_assert_task_count_zero(docket: Docket, simple_task: AsyncMock):
    """assert_task_count should handle zero count."""
    await assert_task_count(docket, simple_task, count=0)
    await assert_task_count(docket, count=0)


async def test_assert_task_count_fails_with_wrong_count(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_count should fail with clear error when count is wrong."""
    await docket.add(simple_task)("arg1")
    await docket.add(simple_task)("arg2")

    with pytest.raises(AssertionError, match="Expected 3.*found 2"):
        await assert_task_count(docket, simple_task, count=3)


async def test_assert_task_count_with_function_name(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_count should work with function name as string."""
    docket.register(simple_task)
    await docket.add("simple_task")("arg1")
    await docket.add("simple_task")("arg2")

    await assert_task_count(docket, "simple_task", count=2)


async def test_assert_no_tasks_succeeds_when_empty(docket: Docket):
    """assert_no_tasks should succeed when docket is empty."""
    await assert_no_tasks(docket)


async def test_assert_no_tasks_fails_when_tasks_present(
    docket: Docket, simple_task: AsyncMock
):
    """assert_no_tasks should fail when tasks are present."""
    await docket.add(simple_task)("arg1")

    with pytest.raises(AssertionError, match="Expected no tasks.*found 1"):
        await assert_no_tasks(docket)


async def test_assert_no_tasks_after_tasks_complete(
    docket: Docket, worker: Worker, simple_task: AsyncMock
):
    """assert_no_tasks should succeed after tasks complete."""
    await docket.add(simple_task)("arg1")

    # Should fail while task is scheduled
    with pytest.raises(AssertionError):
        await assert_no_tasks(docket)

    # Run tasks to completion
    await worker.run_until_finished()

    # Should succeed now
    await assert_no_tasks(docket)


async def test_assert_task_scheduled_partial_kwargs_match(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should match subset of kwargs."""
    await docket.add(simple_task)(kwarg1="value1", kwarg2="value2", kwarg3="value3")

    # Should succeed when checking only some kwargs
    await assert_task_scheduled(docket, simple_task, kwargs={"kwarg1": "value1"})
    await assert_task_scheduled(
        docket, simple_task, kwargs={"kwarg1": "value1", "kwarg2": "value2"}
    )


async def test_assert_task_count_includes_future_and_immediate_tasks(
    docket: Docket, simple_task: AsyncMock, now: Callable[[], datetime]
):
    """assert_task_count should count both immediate and future tasks."""
    await docket.add(simple_task)("immediate")
    later = now() + timedelta(seconds=10)
    await docket.add(simple_task, when=later)("future1")
    await docket.add(simple_task, when=later)("future2")

    await assert_task_count(docket, simple_task, count=3)


async def test_assert_task_scheduled_fails_when_key_doesnt_match(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should fail when key doesn't match."""
    await docket.add(simple_task, key="task-1")("arg1")

    with pytest.raises(AssertionError, match="key='task-2'"):
        await assert_task_scheduled(docket, simple_task, key="task-2")


async def test_assert_task_scheduled_fails_on_empty_docket(
    docket: Docket, simple_task: AsyncMock
):
    """assert_task_scheduled should fail with clear message on empty docket."""
    with pytest.raises(AssertionError, match="no tasks scheduled on docket"):
        await assert_task_scheduled(docket, simple_task)
