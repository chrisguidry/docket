"""Tests for OpenTelemetry tracing, span creation, and message handling."""

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import Span, TracerProvider
from opentelemetry.trace import StatusCode

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

    run = await docket.add(the_task)()

    await worker.run_until_finished()

    assert len(captured) == 1
    (task_span,) = captured
    assert task_span is not None
    assert isinstance(task_span, Span)

    assert task_span.name == "the_task"
    assert task_span.kind == trace.SpanKind.CONSUMER
    assert task_span.attributes

    print(task_span.attributes)

    assert task_span.attributes["docket.name"] == docket.name
    assert task_span.attributes["docket.task"] == "the_task"
    assert task_span.attributes["docket.key"] == run.key
    assert run.when is not None
    assert task_span.attributes["docket.when"] == run.when.isoformat()
    assert task_span.attributes["docket.attempt"] == 1
    assert task_span.attributes["code.function.name"] == "the_task"


async def test_task_spans_are_linked_to_the_originating_span(
    docket: Docket, worker: Worker
):
    """Task execution spans should link back to the trace that scheduled them.

    The link may point to either the originating span directly or to a child span
    (like docket.add) within the same trace - what matters is traceability back
    to the scheduling context.
    """
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

    # Task execution creates a new trace (not a child of the scheduling trace)
    assert task_span.context.trace_id != originating_span.context.trace_id

    # The originating span should not have links (it's the caller, not the receiver)
    assert not originating_span.links

    # The task span should have a link back to the scheduling trace
    assert task_span.links
    assert len(task_span.links) == 1
    (link,) = task_span.links

    # The link should be to the same trace as the originating span
    # (may be to originating_span or to a child like docket.add)
    assert link.context.trace_id == originating_span.context.trace_id


async def test_failed_task_span_has_error_status(docket: Docket, worker: Worker):
    """When a task fails, its span should have ERROR status."""
    captured: list[Span] = []

    async def the_failing_task():
        span = trace.get_current_span()
        assert isinstance(span, Span)
        captured.append(span)
        raise ValueError("Task failed")

    await docket.add(the_failing_task)()
    await worker.run_until_finished()

    assert len(captured) == 1
    (task_span,) = captured

    assert isinstance(task_span, Span)
    assert task_span.status is not None
    assert task_span.status.status_code == StatusCode.ERROR
    assert task_span.status.description is not None
    assert "Task failed" in task_span.status.description


async def test_retried_task_spans_have_error_status(docket: Docket, worker: Worker):
    """When a task fails and is retried, each failed attempt's span should have ERROR status."""
    captured: list[Span] = []
    attempt_count = 0

    async def the_retrying_task(retry: Retry = Retry(attempts=3)):
        nonlocal attempt_count
        attempt_count += 1
        span = trace.get_current_span()
        assert isinstance(span, Span)
        captured.append(span)

        if attempt_count < 3:
            raise ValueError(f"Attempt {attempt_count} failed")
        # Third attempt succeeds

    await docket.add(the_retrying_task)()
    await worker.run_until_finished()

    assert len(captured) == 3

    # First two attempts should have ERROR status
    for i in range(2):
        span = captured[i]
        assert isinstance(span, Span)
        assert span.status is not None
        assert span.status.status_code == StatusCode.ERROR
        assert span.status.description is not None
        assert f"Attempt {i + 1} failed" in span.status.description

    # Third attempt should have OK status (or no status set, which is treated as OK)
    success_span = captured[2]
    assert isinstance(success_span, Span)
    assert (
        success_span.status is None or success_span.status.status_code == StatusCode.OK
    )


async def test_infinitely_retrying_task_spans_have_error_status(
    docket: Docket, worker: Worker
):
    """When a task with infinite retries fails, each attempt's span should have ERROR status."""
    captured: list[Span] = []
    attempt_count = 0

    async def the_infinite_retry_task(retry: Retry = Retry(attempts=None)):
        nonlocal attempt_count
        attempt_count += 1
        span = trace.get_current_span()
        assert isinstance(span, Span)
        captured.append(span)
        raise ValueError(f"Attempt {attempt_count} failed")

    execution = await docket.add(the_infinite_retry_task)()

    # Run worker for only 3 task executions of this specific task
    await worker.run_at_most({execution.key: 3})

    # All captured spans should have ERROR status
    assert len(captured) == 3
    for i, span in enumerate(captured):
        assert isinstance(span, Span)
        assert span.status is not None
        assert span.status.status_code == StatusCode.ERROR
        assert span.status.description is not None
        assert f"Attempt {i + 1} failed" in span.status.description


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
