"""Tests for OpenTelemetry metric counters: task lifecycle and execution counters."""

import asyncio
import sys
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup
from opentelemetry.metrics import Counter

from docket import Docket, Worker
from docket.dependencies import Perpetual, Retry


@pytest.fixture
def task_labels(docket: Docket, the_task: AsyncMock) -> dict[str, str]:
    """Create labels dictionary for the task-side metrics."""
    return {"docket.name": docket.name, "docket.task": the_task.__name__}


@pytest.fixture
def TASKS_ADDED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_ADDED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_ADDED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_REPLACED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_REPLACED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_REPLACED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_SCHEDULED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_SCHEDULED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_SCHEDULED.add", mock_obj)
    return mock_obj


async def test_adding_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    task_labels: dict[str, str],
    TASKS_ADDED: Mock,
    TASKS_REPLACED: Mock,
    TASKS_SCHEDULED: Mock,
):
    """Should increment the appropriate counters when adding a task."""
    await docket.add(the_task)()

    TASKS_ADDED.assert_called_once_with(1, task_labels)
    TASKS_REPLACED.assert_not_called()
    TASKS_SCHEDULED.assert_called_once_with(1, task_labels)


async def test_replacing_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    task_labels: dict[str, str],
    TASKS_ADDED: Mock,
    TASKS_REPLACED: Mock,
    TASKS_SCHEDULED: Mock,
):
    """Should increment the appropriate counters when replacing a task."""
    from datetime import datetime, timezone

    when = datetime.now(timezone.utc) + timedelta(minutes=5)
    key = "test-replace-key"

    await docket.replace(the_task, when, key)()

    TASKS_ADDED.assert_not_called()
    TASKS_REPLACED.assert_called_once_with(1, task_labels)
    TASKS_SCHEDULED.assert_called_once_with(1, task_labels)


@pytest.fixture
def TASKS_CANCELLED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_CANCELLED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_CANCELLED.add", mock_obj)
    return mock_obj


async def test_cancelling_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    TASKS_CANCELLED: Mock,
):
    """Should increment the TASKS_CANCELLED counter when cancelling a task."""
    from datetime import datetime, timezone

    when = datetime.now(timezone.utc) + timedelta(minutes=5)
    key = "test-cancel-key"
    await docket.add(the_task, when=when, key=key)()

    await docket.cancel(key)

    TASKS_CANCELLED.assert_called_once_with(1, {"docket.name": docket.name})


@pytest.fixture
def worker_labels(
    docket: Docket, worker: Worker, the_task: AsyncMock
) -> dict[str, str]:
    """Create labels dictionary for worker-side metrics."""
    return {
        "docket.name": docket.name,
        "docket.worker": worker.name,
        "docket.task": the_task.__name__,
    }


@pytest.fixture
def TASKS_STARTED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_STARTED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_STARTED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_COMPLETED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_COMPLETED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_COMPLETED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_SUCCEEDED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_SUCCEEDED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_SUCCEEDED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_FAILED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_FAILED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_FAILED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_RETRIED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_RETRIED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_RETRIED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_PERPETUATED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_PERPETUATED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_PERPETUATED.add", mock_obj)
    return mock_obj


@pytest.fixture
def TASKS_REDELIVERED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_REDELIVERED counter."""
    mock_obj = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_REDELIVERED.add", mock_obj)
    return mock_obj


async def test_worker_execution_increments_task_counters(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    worker_labels: dict[str, str],
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
    TASKS_REDELIVERED: Mock,
):
    """Should increment the appropriate task counters when a worker executes a task."""
    await docket.add(the_task)()

    await worker.run_until_finished()

    TASKS_STARTED.assert_called_once_with(1, worker_labels)
    TASKS_COMPLETED.assert_called_once_with(1, worker_labels)
    TASKS_SUCCEEDED.assert_called_once_with(1, worker_labels)
    TASKS_FAILED.assert_not_called()
    TASKS_RETRIED.assert_not_called()
    TASKS_REDELIVERED.assert_not_called()


async def test_failed_task_increments_failure_counter(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    worker_labels: dict[str, str],
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
    TASKS_REDELIVERED: Mock,
):
    """Should increment the TASKS_FAILED counter when a task fails."""
    the_task.side_effect = ValueError("Womp")

    await docket.add(the_task)()

    await worker.run_until_finished()

    TASKS_STARTED.assert_called_once_with(1, worker_labels)
    TASKS_COMPLETED.assert_called_once_with(1, worker_labels)
    TASKS_FAILED.assert_called_once_with(1, worker_labels)
    TASKS_SUCCEEDED.assert_not_called()
    TASKS_RETRIED.assert_not_called()
    TASKS_REDELIVERED.assert_not_called()


async def test_retried_task_increments_retry_counter(
    docket: Docket,
    worker: Worker,
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
    TASKS_REDELIVERED: Mock,
):
    """Should increment the TASKS_RETRIED counter when a task is retried."""

    async def the_task(retry: Retry = Retry(attempts=2)):  # noqa: ARG001
        raise ValueError("First attempt fails")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert TASKS_STARTED.call_count == 2
    assert TASKS_COMPLETED.call_count == 2
    assert TASKS_FAILED.call_count == 2
    assert TASKS_RETRIED.call_count == 1
    TASKS_SUCCEEDED.assert_not_called()
    TASKS_REDELIVERED.assert_not_called()


async def test_exhausted_retried_task_increments_retry_counter(
    docket: Docket,
    worker: Worker,
    worker_labels: dict[str, str],
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
    TASKS_REDELIVERED: Mock,
):
    """Should increment the appropriate counters when retries are exhausted."""

    async def the_task(retry: Retry = Retry(attempts=1)):  # noqa: ARG001
        raise ValueError("First attempt fails")

    await docket.add(the_task)()

    await worker.run_until_finished()

    TASKS_STARTED.assert_called_once_with(1, worker_labels)
    TASKS_COMPLETED.assert_called_once_with(1, worker_labels)
    TASKS_FAILED.assert_called_once_with(1, worker_labels)
    TASKS_RETRIED.assert_not_called()
    TASKS_SUCCEEDED.assert_not_called()
    TASKS_REDELIVERED.assert_not_called()


async def test_retried_task_metric_uses_bounded_labels(
    docket: Docket,
    worker: Worker,
    TASKS_RETRIED: Mock,
):
    """TASKS_RETRIED should only use bounded-cardinality labels (not task keys)."""

    async def the_task(retry: Retry = Retry(attempts=2)):  # noqa: ARG001
        raise ValueError("Always fails")

    await docket.add(the_task)()
    await worker.run_until_finished()

    assert TASKS_RETRIED.call_count == 1
    call_labels = TASKS_RETRIED.call_args.args[1]

    assert "docket.name" in call_labels
    assert "docket.worker" in call_labels
    assert "docket.task" in call_labels
    assert "docket.key" not in call_labels
    assert "docket.when" not in call_labels
    assert "docket.attempt" not in call_labels


async def test_perpetuated_task_metric_uses_bounded_labels(
    docket: Docket,
    worker: Worker,
    TASKS_PERPETUATED: Mock,
):
    """TASKS_PERPETUATED should only use bounded-cardinality labels (not task keys)."""

    async def the_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),  # noqa: ARG001
    ):
        pass

    execution = await docket.add(the_task)()
    await worker.run_at_most({execution.key: 2})

    assert TASKS_PERPETUATED.call_count >= 1
    call_labels = TASKS_PERPETUATED.call_args.args[1]

    assert "docket.name" in call_labels
    assert "docket.worker" in call_labels
    assert "docket.task" in call_labels
    assert "docket.key" not in call_labels
    assert "docket.when" not in call_labels
    assert "docket.attempt" not in call_labels


async def test_redelivered_tasks_increment_redelivered_counter(
    docket: Docket,
    TASKS_REDELIVERED: Mock,
):
    """Should increment the TASKS_REDELIVERED counter for redelivered tasks."""

    async def test_task():
        await asyncio.sleep(0.01)

    await docket.add(test_task)()

    worker = Worker(docket, redelivery_timeout=timedelta(milliseconds=50))

    async with worker:
        worker._execute = AsyncMock(side_effect=Exception("Simulated worker failure"))  # type: ignore[assignment]

        with pytest.raises(ExceptionGroup) as exc_info:
            await worker.run_until_finished()
        assert any(
            "Simulated worker failure" in str(e) for e in exc_info.value.exceptions
        )

    await asyncio.sleep(0.075)

    worker2 = Worker(docket, redelivery_timeout=timedelta(milliseconds=100))
    async with worker2:
        await worker2.run_until_finished()

    assert TASKS_REDELIVERED.call_count >= 1
