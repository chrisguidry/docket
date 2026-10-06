"""Normalize the OTLP metrics that one implementation's processes exported.

The result maps each process, ``producer`` or ``worker``, to its metrics.
Each metric has its kind, unit, description, and meter, and maps each data
point's attributes to a value that does not depend on timing.
"""

from collections.abc import Iterable

from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from .names import Names, labels

# pydocket measures caches of Python function signatures, which other
# languages do not have.
IGNORED = {"docket_cache_size"}

# A producer's strike monitor counts its own strike and restore only when it
# reads them before the producer exits, so that count depends on timing.
IGNORED_IN_PROCESS = {("producer", "docket_strikes_in_effect")}

# A gauge's value depends on when the heartbeat ran, so only its presence
# counts.
PRESENT = "present"

Metrics = dict[str, dict[str, object]]


def any_value(value: AnyValue) -> object:
    kind = value.WhichOneof("value")
    return getattr(value, kind) if kind else None


def attributes(pairs: Iterable[KeyValue], names: Names) -> dict[str, object]:
    return {pair.key: names.scrub(any_value(pair.value)) for pair in pairs}


def process(resource: Resource) -> str:
    """``producer`` or ``worker``, from the ``service.name`` the driver set."""
    service = attributes(resource.attributes, Names("", frozenset(), "")).get(
        "service.name", ""
    )
    return str(service).removeprefix("conformance-")


def number(value: float) -> int | float:
    return int(value) if float(value).is_integer() else value


def kind(metric: Metric) -> str:
    data = metric.WhichOneof("data")
    if data == "sum":
        return "counter" if metric.sum.is_monotonic else "up-down counter"
    return str(data)


def points(metric: Metric, names: Names) -> list[tuple[int, str, object]]:
    """Each data point as its time, its attributes, and its value."""
    data = metric.WhichOneof("data")
    if data == "sum":
        return [
            (
                point.time_unix_nano,
                labels(attributes(point.attributes, names)),
                number(point.as_int if point.HasField("as_int") else point.as_double),
            )
            for point in metric.sum.data_points
        ]
    if data == "histogram":
        return [
            (
                point.time_unix_nano,
                labels(attributes(point.attributes, names)),
                point.count,
            )
            for point in metric.histogram.data_points
        ]
    if data == "gauge":
        return [
            (point.time_unix_nano, labels(attributes(point.attributes, names)), PRESENT)
            for point in metric.gauge.data_points
        ]
    return []


def otlp_metrics(
    requests: Iterable[ExportMetricsServiceRequest], names: Names
) -> dict[str, Metrics]:
    """The newest value of every data point, by process and metric.

    The exporters send cumulative values on a timer and once more when they
    shut down, so the newest point holds the totals.
    """
    described: dict[tuple[str, str], dict[str, object]] = {}
    newest: dict[tuple[str, str, str], tuple[int, object]] = {}

    for request in requests:
        for resource_metrics in request.resource_metrics:
            source = process(resource_metrics.resource)
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if (
                        metric.name in IGNORED
                        or (source, metric.name) in IGNORED_IN_PROCESS
                    ):
                        continue
                    described[(source, metric.name)] = {
                        "kind": kind(metric),
                        "unit": metric.unit,
                        "description": metric.description,
                        "meter": scope_metrics.scope.name,
                    }
                    for time, point, value in points(metric, names):
                        seen = newest.get((source, metric.name, point))
                        if seen is None or time >= seen[0]:
                            newest[(source, metric.name, point)] = (time, value)

    values: dict[tuple[str, str], dict[str, object]] = {}
    for (source, name, point), (_, value) in sorted(newest.items()):
        values.setdefault((source, name), {})[point] = value

    result: dict[str, Metrics] = {}
    for (source, name), description in sorted(described.items()):
        measured = values.get((source, name), {})
        result.setdefault(source, {})[name] = {**description, "points": measured}
    return result
