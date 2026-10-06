"""Normalize the spans that one implementation's processes exported.

Each span becomes a dictionary of what must match across implementations.
A link or a parent becomes a description of the span it points to, because
span IDs differ on every run.
"""

from collections.abc import Iterable
from datetime import datetime

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.trace.v1.trace_pb2 import Span, Status

from .metrics import attributes, process
from .names import Names

MISSING = "missing"

Spans = list[dict[str, object]]


def when(value: object) -> object:
    """``<iso8601>`` when the value parses as ISO 8601, else the value itself."""
    if not isinstance(value, str):
        return value
    try:
        # Python 3.10 does not read a trailing Z.
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return "<iso8601>"


def status(span: Span) -> str:
    if span.status.code == Status.STATUS_CODE_ERROR:
        return f"error: {span.status.message}"
    if span.status.code == Status.STATUS_CODE_OK:
        return "ok"
    return "unset"


def event(span_event: Span.Event, names: Names) -> str:
    message = attributes(span_event.attributes, names).get("exception.message")
    return span_event.name if message is None else f"{span_event.name}: {message}"


def otlp_spans(requests: Iterable[ExportTraceServiceRequest], names: Names) -> Spans:
    exported: list[tuple[str, str, Span]] = [
        (process(resource_spans.resource), scope_spans.scope.name, span)
        for request in requests
        for resource_spans in request.resource_spans
        for scope_spans in resource_spans.scope_spans
        for span in scope_spans.spans
    ]

    described: dict[bytes, str] = {}
    for source, _, span in exported:
        labels = attributes(span.attributes, names)
        description = f"{source} {span.name} key={labels.get('docket.key')}"
        if "docket.attempt" in labels:
            description += f" attempt={labels['docket.attempt']}"
        described[span.span_id] = description

    normalized: Spans = []
    for source, scope, span in exported:
        labels = attributes(span.attributes, names)
        if "docket.when" in labels:
            labels["docket.when"] = when(labels["docket.when"])
        normalized.append(
            {
                "process": source,
                "name": span.name,
                "tracer": scope,
                "kind": Span.SpanKind.Name(span.kind)
                .removeprefix("SPAN_KIND_")
                .lower(),
                "status": status(span),
                "attributes": dict(sorted(labels.items())),
                "events": [event(span_event, names) for span_event in span.events],
                "parent": (
                    described.get(span.parent_span_id, MISSING)
                    if span.parent_span_id
                    else None
                ),
                "links": [described.get(link.span_id, MISSING) for link in span.links],
            }
        )
    return normalized
