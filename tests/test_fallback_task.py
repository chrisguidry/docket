from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from docket import CurrentExecution, Docket, Worker
from docket.dependencies import TaskLogger
from docket.execution import Execution
from tests._key_leak_checker import KeyCountChecker


async def test_default_fallback_task_logs_and_acks(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
    the_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """Default fallback should log a warning and acknowledge the message."""
    await docket.add(the_task)()

    # Unregister the task before worker runs
    docket.tasks.pop("the_task")

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        with caplog.at_level(logging.WARNING):
            await worker.run_until_finished()

    # Should log about unknown task with function name and registration hints
    assert "Unknown task 'the_task' received - dropping" in caplog.text
    assert "Register via CLI (--tasks your.module:tasks)" in caplog.text

    # Message should be acknowledged (no pending messages)
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0


async def test_custom_fallback_receives_original_args_kwargs(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """Custom fallback should receive the original task's args and kwargs."""
    received_args: tuple[Any, ...] = ()
    received_kwargs: dict[str, Any] = {}

    async def custom_fallback(*args: Any, **kwargs: Any) -> None:
        nonlocal received_args, received_kwargs
        received_args = args
        received_kwargs = kwargs

    async def original_task(x: int, y: str, z: bool = True) -> None:
        pass  # pragma: no cover

    docket.register(original_task)
    await docket.add(original_task)(42, "hello", z=False)

    # Unregister before worker runs
    docket.tasks.pop("original_task")

    async with Worker(
        docket,
        fallback_task=custom_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert received_args == (42, "hello")
    assert received_kwargs == {"z": False}


async def test_fallback_can_access_function_name(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """Fallback should be able to access the original function name via execution.function_name."""
    captured_function_name: str | None = None

    async def custom_fallback(
        *args: Any,
        execution: Execution = CurrentExecution(),
        **kwargs: Any,
    ) -> None:
        nonlocal captured_function_name
        captured_function_name = execution.function_name

    async def my_special_task() -> None:
        pass  # pragma: no cover

    docket.register(my_special_task)
    await docket.add(my_special_task)()

    # Unregister before worker runs
    docket.tasks.pop("my_special_task")

    async with Worker(
        docket,
        fallback_task=custom_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert captured_function_name == "my_special_task"


async def test_fallback_dependency_injection(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """Fallback should support full dependency injection like regular tasks."""
    captured_execution: Execution | None = None
    captured_logger: logging.LoggerAdapter[logging.Logger] | None = None

    async def custom_fallback(
        *args: Any,
        execution: Execution = CurrentExecution(),
        logger: logging.LoggerAdapter[logging.Logger] = TaskLogger(),
        **kwargs: Any,
    ) -> None:
        nonlocal captured_execution, captured_logger
        captured_execution = execution
        captured_logger = logger

    async def some_task(value: int) -> None:
        pass  # pragma: no cover

    docket.register(some_task)
    await docket.add(some_task)(123)

    docket.tasks.pop("some_task")

    async with Worker(
        docket,
        fallback_task=custom_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert captured_execution is not None
    assert captured_execution.function_name == "some_task"
    assert captured_execution.args == (123,)
    assert captured_logger is not None


async def test_fallback_custom_user_dependency(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """Fallback should support custom user dependencies via Depends()."""
    from docket.dependencies import Depends

    async def get_request_id() -> str:
        return "req-12345"

    captured_request_id: str | None = None

    async def custom_fallback(
        *args: Any,
        request_id: str = Depends(get_request_id),
        **kwargs: Any,
    ) -> None:
        nonlocal captured_request_id
        captured_request_id = request_id

    async def some_task() -> None:
        pass  # pragma: no cover

    docket.register(some_task)
    await docket.add(some_task)()
    docket.tasks.pop("some_task")

    async with Worker(
        docket,
        fallback_task=custom_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert captured_request_id == "req-12345"


async def test_fallback_return_completes_task(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """A fallback that returns normally should complete and ACK the task."""

    async def custom_fallback(*args: Any, **kwargs: Any) -> str:
        return "handled"

    async def missing_task() -> None:
        pass  # pragma: no cover

    docket.register(missing_task)
    await docket.add(missing_task)()
    docket.tasks.pop("missing_task")

    async with Worker(
        docket,
        fallback_task=custom_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    # No pending messages - task was completed
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0
        assert await redis.xlen(docket.stream_key) == 0


async def test_fallback_exception_triggers_retry(
    docket: Docket,
    key_leak_checker: KeyCountChecker,
):
    """A fallback that raises should trigger retry behavior when using Retry dependency."""
    from docket import Retry

    call_count = 0

    async def failing_fallback(
        *args: Any,
        retry: Retry = Retry(attempts=5, delay=timedelta(milliseconds=10)),
        **kwargs: Any,
    ) -> None:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("Simulated failure")

    async def some_task() -> None:
        pass  # pragma: no cover

    docket.register(some_task)
    await docket.add(some_task)()
    docket.tasks.pop("some_task")

    async with Worker(
        docket,
        fallback_task=failing_fallback,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert call_count == 3


async def test_execution_function_name_matches_for_known_tasks(
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
):
    """For known tasks, execution.function_name should match function.__name__."""
    captured_function_name: str | None = None
    captured_function_dunder_name: str | None = None

    async def known_task(execution: Execution = CurrentExecution()) -> None:
        nonlocal captured_function_name, captured_function_dunder_name
        captured_function_name = execution.function_name
        captured_function_dunder_name = execution.function.__name__

    await docket.add(known_task)()
    await worker.run_until_finished()

    assert captured_function_name == "known_task"
    assert captured_function_dunder_name == "known_task"
    assert captured_function_name == captured_function_dunder_name
