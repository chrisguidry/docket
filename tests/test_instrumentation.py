import asyncio
from datetime import datetime, timedelta, timezone
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import Span, TracerProvider

from docket import Docket, Worker
from docket.dependencies import Retry
from docket.instrumentation import message_getter, message_setter

tracer = trace.get_tracer(__name__)


@pytest.fixture(scope="module", autouse=True)
def tracer_provider() -> TracerProvider:
    """Sets up a "real" TracerProvider so that spans are recorded for the tests"""
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
    return provider


async def test_executing_a_task_is_wrapped_in_a_span(docket: Docket, worker: Worker):
    captured: list[Span] = []

    async def the_task():
        span = trace.get_current_span()
        assert isinstance(span, Span)
        captured.append(span)

    execution = await docket.add(the_task)()

    await worker.run_until_finished()

    assert len(captured) == 1
    (task_span,) = captured
    assert task_span is not None
    assert isinstance(task_span, Span)

    assert task_span.name == "the_task"
    assert task_span.kind == trace.SpanKind.CONSUMER
    assert task_span.attributes

    assert task_span.attributes["docket.name"] == docket.name
    assert task_span.attributes["docket.execution.key"] == execution.key
    assert task_span.attributes["docket.execution.when"] == execution.when.isoformat()
    assert task_span.attributes["docket.execution.attempt"] == 1
    assert task_span.attributes["code.function.name"] == "the_task"


async def test_task_spans_are_linked_to_the_originating_span(
    docket: Docket, worker: Worker
):
    captured: list[Span] = []

    async def the_task():
        span = trace.get_current_span()
        assert isinstance(span, Span)
        captured.append(span)

    with tracer.start_as_current_span("originating_span") as originating_span:
        await docket.add(the_task)()

    assert isinstance(originating_span, Span)
    assert originating_span.context

    await worker.run_until_finished()

    assert len(captured) == 1
    (task_span,) = captured

    assert isinstance(task_span, Span)
    assert task_span.context

    assert task_span.context.trace_id != originating_span.context.trace_id

    assert not originating_span.links

    assert task_span.links
    assert len(task_span.links) == 1
    (link,) = task_span.links

    assert link.context.trace_id == originating_span.context.trace_id
    assert link.context.span_id == originating_span.context.span_id


async def test_message_getter_returns_none_for_missing_key():
    """Should return None when a key is not present in the message."""

    message = {b"existing_key": b"value"}
    result = message_getter.get(message, "missing_key")

    assert result is None


async def test_message_getter_returns_decoded_value():
    """Should return a list with the decoded value when a key is present."""

    message = {b"key": b"value"}
    result = message_getter.get(message, "key")

    assert result == ["value"]


async def test_message_getter_keys_returns_decoded_keys():
    """Should return a list of all keys in the message as decoded strings."""

    message = {b"key1": b"value1", b"key2": b"value2"}
    result = message_getter.keys(message)

    assert sorted(result) == ["key1", "key2"]


async def test_message_setter_encodes_key_and_value():
    """Should encode both key and value when setting a value in the message."""

    message: dict[bytes, bytes] = {}
    message_setter.set(message, "key", "value")

    assert message == {b"key": b"value"}


async def test_message_setter_overwrites_existing_value():
    """Should overwrite an existing value when setting a value for an existing key."""

    message = {b"key": b"old_value"}
    message_setter.set(message, "key", "new_value")

    assert message == {b"key": b"new_value"}


@pytest.fixture
def docket_labels(docket: Docket, the_task: AsyncMock) -> dict[str, str]:
    """Create labels dictionary for the Docket client-side metrics."""
    return {"docket": docket.name, "task": the_task.__name__}


@pytest.fixture
def TASKS_ADDED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_ADDED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_ADDED.add", mock)
    return mock


@pytest.fixture
def TASKS_REPLACED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_REPLACED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_REPLACED.add", mock)
    return mock


@pytest.fixture
def TASKS_SCHEDULED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_SCHEDULED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_SCHEDULED.add", mock)
    return mock


async def test_adding_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    docket_labels: dict[str, str],
    TASKS_ADDED: Mock,
    TASKS_REPLACED: Mock,
    TASKS_SCHEDULED: Mock,
):
    """Should increment the appropriate counters when adding a task."""
    await docket.add(the_task)()

    TASKS_ADDED.assert_called_once_with(1, docket_labels)
    TASKS_REPLACED.assert_not_called()
    TASKS_SCHEDULED.assert_called_once_with(1, docket_labels)


async def test_replacing_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    docket_labels: dict[str, str],
    TASKS_ADDED: Mock,
    TASKS_REPLACED: Mock,
    TASKS_SCHEDULED: Mock,
):
    """Should increment the appropriate counters when replacing a task."""
    when = datetime.now(timezone.utc) + timedelta(minutes=5)
    key = "test-replace-key"

    await docket.replace(the_task, when, key)()

    TASKS_ADDED.assert_not_called()
    TASKS_REPLACED.assert_called_once_with(1, docket_labels)
    TASKS_SCHEDULED.assert_called_once_with(1, docket_labels)


@pytest.fixture
def TASKS_CANCELLED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_CANCELLED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_CANCELLED.add", mock)
    return mock


async def test_cancelling_a_task_increments_counter(
    docket: Docket,
    the_task: AsyncMock,
    docket_labels: dict[str, str],
    TASKS_CANCELLED: Mock,
):
    """Should increment the TASKS_CANCELLED counter when cancelling a task."""
    when = datetime.now(timezone.utc) + timedelta(minutes=5)
    key = "test-cancel-key"
    await docket.add(the_task, when=when, key=key)()

    await docket.cancel(key)

    TASKS_CANCELLED.assert_called_once_with(1, {"docket": docket.name})


@pytest.fixture
def worker_labels(
    docket: Docket, worker: Worker, the_task: AsyncMock
) -> dict[str, str]:
    """Create labels dictionary for worker-side metrics."""
    return {"docket": docket.name, "worker": worker.name, "task": the_task.__name__}


@pytest.fixture
def TASKS_STARTED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_STARTED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_STARTED.add", mock)
    return mock


@pytest.fixture
def TASKS_COMPLETED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_COMPLETED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_COMPLETED.add", mock)
    return mock


@pytest.fixture
def TASKS_SUCCEEDED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_SUCCEEDED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_SUCCEEDED.add", mock)
    return mock


@pytest.fixture
def TASKS_FAILED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_FAILED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_FAILED.add", mock)
    return mock


@pytest.fixture
def TASKS_RETRIED(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_RETRIED counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_RETRIED.add", mock)
    return mock


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
):
    """Should increment the appropriate task counters when a worker executes a task."""
    await docket.add(the_task)()

    await worker.run_until_finished()

    TASKS_STARTED.assert_called_once_with(1, worker_labels)
    TASKS_COMPLETED.assert_called_once_with(1, worker_labels)
    TASKS_SUCCEEDED.assert_called_once_with(1, worker_labels)
    TASKS_FAILED.assert_not_called()
    TASKS_RETRIED.assert_not_called()


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


async def test_retried_task_increments_retry_counter(
    docket: Docket,
    worker: Worker,
    worker_labels: dict[str, str],
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
):
    """Should increment the TASKS_RETRIED counter when a task is retried."""

    async def the_task(retry: Retry = Retry(attempts=2)):
        raise ValueError("First attempt fails")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert TASKS_STARTED.call_count == 2
    assert TASKS_COMPLETED.call_count == 2
    assert TASKS_FAILED.call_count == 2
    assert TASKS_RETRIED.call_count == 1
    TASKS_SUCCEEDED.assert_not_called()


async def test_exhausted_retried_task_increments_retry_counter(
    docket: Docket,
    worker: Worker,
    worker_labels: dict[str, str],
    TASKS_STARTED: Mock,
    TASKS_COMPLETED: Mock,
    TASKS_SUCCEEDED: Mock,
    TASKS_FAILED: Mock,
    TASKS_RETRIED: Mock,
):
    """Should increment the appropriate counters when retries are exhausted."""

    async def the_task(retry: Retry = Retry(attempts=1)):
        raise ValueError("First attempt fails")

    await docket.add(the_task)()

    await worker.run_until_finished()

    TASKS_STARTED.assert_called_once_with(1, worker_labels)
    TASKS_COMPLETED.assert_called_once_with(1, worker_labels)
    TASKS_FAILED.assert_called_once_with(1, worker_labels)
    TASKS_RETRIED.assert_not_called()
    TASKS_SUCCEEDED.assert_not_called()


@pytest.fixture
def TASK_DURATION(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASK_DURATION histogram."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASK_DURATION.record", mock)
    return mock


async def test_task_duration_is_measured(
    docket: Docket, worker: Worker, worker_labels: dict[str, str], TASK_DURATION: Mock
):
    """Should record the duration of task execution in the TASK_DURATION histogram."""

    async def the_task():
        await asyncio.sleep(0.1)

    await docket.add(the_task)()
    await worker.run_until_finished()

    # We can't check the exact value since it depends on actual execution time
    TASK_DURATION.assert_called_once_with(mock.ANY, worker_labels)
    duration: float = TASK_DURATION.call_args.args[0]
    assert isinstance(duration, float)
    assert 0.1 <= duration <= 0.2


@pytest.fixture
def TASK_PUNCTUALITY(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASK_PUNCTUALITY histogram."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASK_PUNCTUALITY.record", mock)
    return mock


async def test_task_punctuality_is_measured(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    worker_labels: dict[str, str],
    TASK_PUNCTUALITY: Mock,
):
    """Should record TASK_PUNCTUALITY values for scheduled tasks."""
    when = datetime.now(timezone.utc) + timedelta(seconds=0.1)
    await docket.add(the_task, when=when)()
    await asyncio.sleep(0.4)
    await worker.run_until_finished()

    # We can't check the exact value since it depends on actual timing
    TASK_PUNCTUALITY.assert_called_once_with(mock.ANY, worker_labels)
    punctuality: float = TASK_PUNCTUALITY.call_args.args[0]
    assert isinstance(punctuality, float)
    assert 0.3 <= punctuality <= 0.5


@pytest.fixture
def TASKS_RUNNING(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_RUNNING up-down counter."""
    mock = Mock()
    monkeypatch.setattr("docket.instrumentation.TASKS_RUNNING.add", mock)
    return mock


async def test_task_running_gauge_is_incremented(
    docket: Docket, worker: Worker, worker_labels: dict[str, str], TASKS_RUNNING: Mock
):
    """Should increment and decrement the TASKS_RUNNING gauge appropriately."""
    inside_task = False

    async def the_task():
        nonlocal inside_task
        inside_task = True

        TASKS_RUNNING.assert_called_once_with(1, worker_labels)

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert inside_task is True

    TASKS_RUNNING.assert_has_calls(
        [
            mock.call(1, worker_labels),
            mock.call(-1, worker_labels),
        ]
    )
