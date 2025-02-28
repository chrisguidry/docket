import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import Span, TracerProvider

from docket import Docket, Worker

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

    await worker.run_until_current()

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

    await worker.run_until_current()

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
    from docket.instrumentation import message_getter

    message = {b"existing_key": b"value"}
    result = message_getter.get(message, "missing_key")

    assert result is None


async def test_message_getter_returns_decoded_value():
    """Should return a list with the decoded value when a key is present."""
    from docket.instrumentation import message_getter

    message = {b"key": b"value"}
    result = message_getter.get(message, "key")

    assert result == ["value"]


async def test_message_getter_keys_returns_decoded_keys():
    """Should return a list of all keys in the message as decoded strings."""
    from docket.instrumentation import message_getter

    message = {b"key1": b"value1", b"key2": b"value2"}
    result = message_getter.keys(message)

    assert sorted(result) == ["key1", "key2"]


async def test_message_setter_encodes_key_and_value():
    """Should encode both key and value when setting a value in the message."""
    from docket.instrumentation import message_setter

    message: dict[bytes, bytes] = {}
    message_setter.set(message, "key", "value")

    assert message == {b"key": b"value"}


async def test_message_setter_overwrites_existing_value():
    """Should overwrite an existing value when setting a value for an existing key."""
    from docket.instrumentation import message_setter

    message = {b"key": b"old_value"}
    message_setter.set(message, "key", "new_value")

    assert message == {b"key": b"new_value"}
