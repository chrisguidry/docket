"""Tests for OpenTelemetry tracing, span creation, and message handling."""

import asyncio
from typing import Generator, Sequence
from unittest.mock import Mock

import pytest
from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
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


# --- Tests for Redis instrumentation suppression ---


@pytest.fixture
def span_exporter(tracer_provider: TracerProvider) -> InMemorySpanExporter:
    """Creates an in-memory span exporter that captures all spans."""
    exporter = InMemorySpanExporter()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def redis_instrumentation() -> Generator[None, None, None]:
    """Enables Redis auto-instrumentation for the duration of the test."""
    instrumentor = RedisInstrumentor()
    instrumentor.instrument()  # type: ignore[no-untyped-call]
    try:
        yield
    finally:
        instrumentor.uninstrument()


def _get_polling_spans(spans: Sequence[ReadableSpan]) -> list[ReadableSpan]:
    """Filter spans to only internal polling spans (XREADGROUP, XAUTOCLAIM, XREAD)."""
    polling_commands = {"XREADGROUP", "XAUTOCLAIM", "XREAD"}
    result: list[ReadableSpan] = []
    for span in spans:
        name_upper = span.name.upper()
        if any(cmd in name_upper for cmd in polling_commands):
            result.append(span)
    return result


def _get_xread_spans(spans: Sequence[ReadableSpan]) -> list[ReadableSpan]:
    """Filter spans to only XREAD spans (strike stream monitoring)."""
    result: list[ReadableSpan] = []
    for span in spans:
        name_upper = span.name.upper()
        if "XREAD" in name_upper and "XREADGROUP" not in name_upper:
            result.append(span)
    return result


def test_get_xread_spans_filters_correctly():
    """Unit test for _get_xread_spans helper to cover all branches."""
    # Create mock spans with different names
    xread_span = Mock(spec=ReadableSpan)
    xread_span.name = "XREAD"

    xreadgroup_span = Mock(spec=ReadableSpan)
    xreadgroup_span.name = "XREADGROUP"

    other_span = Mock(spec=ReadableSpan)
    other_span.name = "GET"

    spans = [xread_span, xreadgroup_span, other_span]
    result = _get_xread_spans(spans)

    # Only XREAD should be included (not XREADGROUP or GET)
    assert len(result) == 1
    assert result[0] is xread_span


async def test_internal_redis_polling_spans_suppressed_by_default(
    docket: Docket,
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Internal Redis polling spans (XREADGROUP, XAUTOCLAIM) should be suppressed by default.

    Per-task Redis operations (claim, concurrency checks) are still instrumented since
    they scale with task count, not with polling frequency.
    """
    # Clear any spans from setup fixtures (e.g., key_leak_checker's temp worker)
    span_exporter.clear()

    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    await docket.add(simple_task)()

    # Default: enable_internal_instrumentation=False
    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_executed

    spans = span_exporter.get_finished_spans()
    span_names = [s.name for s in spans]

    # Task execution span SHOULD exist
    assert "simple_task" in span_names, f"Expected task span, got: {span_names}"

    # Internal Redis polling spans (XREADGROUP, XAUTOCLAIM) should NOT exist
    polling_spans = _get_polling_spans(spans)
    assert len(polling_spans) == 0, (
        f"Expected no polling spans with suppression enabled, "
        f"got: {[s.name for s in polling_spans]}"
    )


async def test_internal_redis_polling_spans_present_when_suppression_disabled(
    docket: Docket,
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Internal Redis polling spans should appear when suppression is disabled."""
    # Clear any spans from setup fixtures
    span_exporter.clear()

    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    await docket.add(simple_task)()

    # Explicitly enable internal instrumentation
    async with Worker(docket, enable_internal_instrumentation=True) as worker:
        await worker.run_until_finished()

    assert task_executed

    spans = span_exporter.get_finished_spans()
    span_names = [s.name for s in spans]

    # Task execution span should exist
    assert "simple_task" in span_names, f"Expected task span, got: {span_names}"

    # Redis polling spans SHOULD exist when internal instrumentation is enabled
    polling_spans = _get_polling_spans(spans)
    assert len(polling_spans) > 0, (
        f"Expected polling spans with internal instrumentation enabled, got none. "
        f"All spans: {span_names}"
    )


async def test_docket_strike_xread_spans_suppressed_by_default(
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Docket's strike stream XREAD polling spans should be suppressed by default."""
    span_exporter.clear()

    # Create docket with default enable_internal_instrumentation=False
    async with Docket(url="memory://"):
        # Give the _monitor_strikes task time to do at least one XREAD poll
        await asyncio.sleep(0.1)

    spans = span_exporter.get_finished_spans()
    xread_spans = _get_xread_spans(spans)

    assert len(xread_spans) == 0, (
        f"Expected no XREAD spans with suppression enabled, "
        f"got: {[s.name for s in xread_spans]}"
    )


async def test_docket_strike_xread_spans_present_when_instrumentation_enabled(
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Docket's strike stream XREAD polling spans should appear when instrumentation is enabled."""
    span_exporter.clear()

    # Create docket with internal instrumentation enabled
    async with Docket(url="memory://", enable_internal_instrumentation=True):
        # Give the _monitor_strikes task time to do at least one XREAD poll
        await asyncio.sleep(0.1)

    spans = span_exporter.get_finished_spans()
    xread_spans = _get_xread_spans(spans)

    assert len(xread_spans) > 0, (
        f"Expected XREAD spans with internal instrumentation enabled, got none. "
        f"All spans: {[s.name for s in spans]}"
    )
