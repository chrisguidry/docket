"""Tests for Redis instrumentation suppression."""

import asyncio
from typing import Generator, Sequence
from unittest.mock import Mock

import pytest
from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from docket import Docket, Worker


@pytest.fixture(scope="module")
def tracer_provider() -> TracerProvider:
    """Sets up a TracerProvider for these tests.

    Uses the existing provider if one is set, otherwise creates a new one.
    """
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
    return provider


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


def _get_internal_operation_spans(spans: Sequence[ReadableSpan]) -> list[ReadableSpan]:
    """Filter spans to only internal execution operations (HSET, HGETALL, HDEL, EXPIRE).

    Note: EVAL/EVALSHA is excluded because it's used by both user-facing operations
    (schedule via Lua script) and internal operations (claim via Lua script). We can't
    distinguish between them, so we focus on operations that are uniquely internal.
    """
    internal_commands = {"HSET", "HGETALL", "HDEL", "EXPIRE"}
    result: list[ReadableSpan] = []
    for span in spans:
        name_upper = span.name.upper()
        # Check for exact command matches to avoid false positives
        if any(cmd in name_upper for cmd in internal_commands):
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


async def test_execution_internal_spans_suppressed_by_default(
    docket: Docket,
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Execution's internal Redis operations (claim, terminal state) should be suppressed by default.

    The Execution class uses HSET, HGETALL, EVAL, DELETE, HDEL, and EXPIRE for internal
    state management. These should not generate spans when enable_internal_instrumentation=False.
    """
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

    # Internal execution operations (HSET, HGETALL, EVAL, DELETE, etc.) should NOT exist
    internal_spans = _get_internal_operation_spans(spans)
    assert len(internal_spans) == 0, (
        f"Expected no internal operation spans with suppression enabled, "
        f"got: {[s.name for s in internal_spans]}"
    )


async def test_execution_internal_spans_present_when_enabled(
    span_exporter: InMemorySpanExporter,
    redis_instrumentation: None,
):
    """Execution's internal Redis operations should appear when instrumentation is enabled."""
    span_exporter.clear()

    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    # Create docket with internal instrumentation enabled
    async with Docket(url="memory://", enable_internal_instrumentation=True) as docket:
        await docket.add(simple_task)()

        # Explicitly enable internal instrumentation on worker too
        async with Worker(docket, enable_internal_instrumentation=True) as worker:
            await worker.run_until_finished()

    assert task_executed

    spans = span_exporter.get_finished_spans()
    span_names = [s.name for s in spans]

    # Task execution span should exist
    assert "simple_task" in span_names, f"Expected task span, got: {span_names}"

    # Internal execution operations SHOULD exist when internal instrumentation is enabled
    internal_spans = _get_internal_operation_spans(spans)
    assert len(internal_spans) > 0, (
        f"Expected internal operation spans with internal instrumentation enabled, got none. "
        f"All spans: {span_names}"
    )
