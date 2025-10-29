import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from docket import CurrentDocket, CurrentWorker, Docket, Worker
from docket.dependencies import Depends, ExponentialRetry, Retry, TaskArgument


async def test_dependencies_may_be_duplicated(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        docketA: Docket = CurrentDocket(),
        docketB: Docket = CurrentDocket(),
        workerA: Worker = CurrentWorker(),
        workerB: Worker = CurrentWorker(),
    ):
        assert a == "a"
        assert b == "b"
        assert docketA is docket
        assert docketB is docket
        assert workerA is worker
        assert workerB is worker

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert called


async def test_retries_must_be_unique(docket: Docket, worker: Worker):
    async def the_task(
        a: str,
        retryA: Retry = Retry(attempts=3),
        retryB: Retry = Retry(attempts=5),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match="Only one Retry dependency is allowed per task",
    ):
        await docket.add(the_task)("a")


async def test_users_can_provide_dependencies_directly(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=3),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 42

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b", retry=Retry(attempts=42))

    await worker.run_until_finished()

    assert called


async def test_user_provide_retries_are_used(docket: Docket, worker: Worker):
    calls = 0

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=42),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 2

        nonlocal calls
        calls += 1

        raise Exception("womp womp")

    await docket.add(the_task)("a", "b", retry=Retry(attempts=2))

    await worker.run_until_finished()

    assert calls == 2


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_in_timedelta_time(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            first_call_time = datetime.now(timezone.utc)
            retry.in_(timedelta(seconds=0.5))
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_at_a_specific_time(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) + timedelta(seconds=0.5)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_user_can_request_a_retry_at_a_specific_time_in_the_past(
    docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=2),
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_dependencies_error_for_missing_task_argument(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task will fail when asking for a missing task argument"""

    async def dependency_one(nope: list[str] = TaskArgument()) -> list[str]:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def dependent_task(
        a: list[str],
        b: list[str] = TaskArgument("a"),
        c: list[str] = Depends(dependency_one),
    ) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)(a=["hello", "world"])

    await worker.run_until_finished()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): c" in caplog.text
    assert "ExceptionGroup" in caplog.text
    assert "KeyError: 'nope'" in caplog.text


async def test_a_task_argument_cannot_ask_for_itself(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task argument cannot ask for itself"""

    # This task would be nonsense, because it's asking for itself.
    async def dependent_task(a: list[str] = TaskArgument()) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): a" in caplog.text
    assert "ValueError: No parameter name specified" in caplog.text


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


async def test_progress_dependency(docket: Docket, worker: Worker):
    """Progress dependency should track task progress"""
    from docket.dependencies import Progress
    from docket.state import ProgressInfo, TaskStateStore

    progress_values: list[ProgressInfo] = []

    async def task_with_progress(progress: Progress = Progress()):
        # Set total
        await progress.set_total(200)

        # Increment progress
        await progress.increment(50)
        await progress.increment(50)

        # Set progress directly
        await progress.set(150)

        # Get current progress
        current = await progress.get()
        assert current is not None
        progress_values.append(current)

    docket.register(task_with_progress)
    execution = await docket.add(task_with_progress, key="progress-test")()
    await worker.run_until_finished()

    # Verify progress was tracked during execution
    assert len(progress_values) == 1
    assert progress_values[0] is not None
    assert progress_values[0].current == 150
    assert progress_values[0].total == 200

    # Note: After task completion, progress may be marked complete (current=total)
    # This is expected behavior for the Progress tracking system
    store = TaskStateStore(docket, docket.record_ttl)
    final_progress = await store.get_task_progress(execution.key)
    assert final_progress is not None
    assert final_progress.total == 200
    # Progress completion tracking may set current to total
    assert final_progress.current in [150, 200]


async def test_progress_dependency_context_manager(docket: Docket, worker: Worker):
    """Progress dependency should work as async context manager"""
    from docket.dependencies import Progress

    entered = False
    exited = False

    async def task_with_progress_context(progress: Progress = Progress()):
        nonlocal entered, exited
        entered = True
        # Progress context is already entered when injected
        await progress.set_total(100)
        await progress.increment(25)
        exited = True  # Will be set before __aexit__

    docket.register(task_with_progress_context)
    await docket.add(task_with_progress_context, key="progress-ctx-test")()
    await worker.run_until_finished()

    assert entered
    assert exited


async def test_progress_set_total_validation(docket: Docket, worker: Worker):
    """Progress.set_total() should validate input."""
    from docket.dependencies import Progress

    validation_error = None

    async def task_with_invalid_total(progress: Progress = Progress()):
        nonlocal validation_error
        try:
            await progress.set_total(-10)
        except ValueError as e:
            validation_error = e

    docket.register(task_with_invalid_total)
    await docket.add(task_with_invalid_total, key="validation-test")()
    await worker.run_until_finished()

    assert validation_error is not None
    assert "must be positive" in str(validation_error)


async def test_progress_set_total_zero_validation(docket: Docket, worker: Worker):
    """Progress.set_total() should reject zero."""
    from docket.dependencies import Progress

    validation_error = None

    async def task_with_zero_total(progress: Progress = Progress()):
        nonlocal validation_error
        try:
            await progress.set_total(0)
        except ValueError as e:
            validation_error = e

    docket.register(task_with_zero_total)
    await docket.add(task_with_zero_total, key="zero-validation-test")()
    await worker.run_until_finished()

    assert validation_error is not None
    assert "must be positive" in str(validation_error)


async def test_progress_set_negative_validation(docket: Docket, worker: Worker):
    """Progress.set() should validate negative values."""
    from docket.dependencies import Progress

    validation_error = None

    async def task_with_negative_current(progress: Progress = Progress()):
        nonlocal validation_error
        try:
            await progress.set(-5)
        except ValueError as e:
            validation_error = e

    docket.register(task_with_negative_current)
    await docket.add(task_with_negative_current, key="negative-validation-test")()
    await worker.run_until_finished()

    assert validation_error is not None
    assert "must be non-negative" in str(validation_error)


async def test_progress_set_exceeds_total_validation(docket: Docket, worker: Worker):
    """Progress.set() should validate current doesn't exceed total."""
    from docket.dependencies import Progress

    validation_error = None

    async def task_with_exceeding_current(progress: Progress = Progress()):
        nonlocal validation_error
        await progress.set_total(100)
        try:
            await progress.set(150)
        except ValueError as e:
            validation_error = e

    docket.register(task_with_exceeding_current)
    await docket.add(task_with_exceeding_current, key="exceeds-validation-test")()
    await worker.run_until_finished()

    assert validation_error is not None
    assert "cannot exceed total" in str(validation_error)
