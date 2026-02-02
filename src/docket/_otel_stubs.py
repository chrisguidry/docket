"""Stub implementations for OpenTelemetry interfaces.

These noop implementations allow docket to function without OpenTelemetry installed.
All operations are silent and do nothing.
"""

from contextlib import contextmanager
from enum import Enum
from typing import Any, Generator, Mapping, Sequence


class StatusCode(Enum):
    """Noop StatusCode enum matching OpenTelemetry's StatusCode."""

    UNSET = 0
    OK = 1
    ERROR = 2


class Status:
    """Noop Status class matching OpenTelemetry's Status."""

    def __init__(
        self, status_code: StatusCode = StatusCode.UNSET, description: str | None = None
    ) -> None:
        self.status_code = status_code
        self.description = description


class SpanKind(Enum):
    """Noop SpanKind enum matching OpenTelemetry's SpanKind."""

    INTERNAL = 0
    SERVER = 1
    CLIENT = 2
    PRODUCER = 3
    CONSUMER = 4


class SpanContext:
    """Noop SpanContext class."""

    trace_id: int = 0
    span_id: int = 0
    is_remote: bool = False
    trace_flags: int = 0
    trace_state: None = None

    @property
    def is_valid(self) -> bool:
        return False


class Link:
    """Noop Link class matching OpenTelemetry's Link."""

    def __init__(
        self,
        context: SpanContext,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        self.context = context
        self.attributes = attributes or {}


class NoopSpan:
    """Noop Span that does nothing."""

    def __init__(self, name: str = "", context: SpanContext | None = None) -> None:
        self._name = name
        self._context = context or SpanContext()

    def get_span_context(self) -> SpanContext:
        return self._context

    def set_status(self, status: Status | StatusCode, description: str | None = None) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_attributes(self, attributes: Mapping[str, Any]) -> None:
        pass

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, Any] | None = None,
        timestamp: int | None = None,
    ) -> None:
        pass

    def record_exception(
        self,
        exception: BaseException,
        attributes: Mapping[str, Any] | None = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        pass

    def update_name(self, name: str) -> None:
        pass

    def is_recording(self) -> bool:
        return False

    def end(self, end_time: int | None = None) -> None:
        pass

    def __enter__(self) -> "NoopSpan":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class NoopTracer:
    """Noop Tracer that returns noop spans."""

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Any = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, Any] | None = None,
        links: Sequence[Link] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> Generator[NoopSpan, None, None]:
        yield NoopSpan(name)

    def start_span(
        self,
        name: str,
        context: Any = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, Any] | None = None,
        links: Sequence[Link] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> NoopSpan:
        return NoopSpan(name)


class NoopCounter:
    """Noop Counter that does nothing."""

    def add(
        self,
        amount: int | float,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        pass


class NoopHistogram:
    """Noop Histogram that does nothing."""

    def record(
        self,
        amount: int | float,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        pass


class NoopGauge:
    """Noop Gauge that does nothing."""

    def set(
        self,
        amount: int | float,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        pass


class NoopUpDownCounter:
    """Noop UpDownCounter that does nothing."""

    def add(
        self,
        amount: int | float,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        pass


class NoopMeter:
    """Noop Meter that returns noop instruments."""

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> NoopCounter:
        return NoopCounter()

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> NoopHistogram:
        return NoopHistogram()

    def create_gauge(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> NoopGauge:
        return NoopGauge()

    def create_up_down_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> NoopUpDownCounter:
        return NoopUpDownCounter()


class NoopMeterProvider:
    """Noop MeterProvider."""

    def get_meter(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
    ) -> NoopMeter:
        return NoopMeter()


class NoopTracerProvider:
    """Noop TracerProvider."""

    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: str | None = None,
        schema_url: str | None = None,
    ) -> NoopTracer:
        return NoopTracer()


# Global noop providers
_tracer_provider = NoopTracerProvider()
_meter_provider = NoopMeterProvider()


def get_tracer(
    instrumenting_module_name: str,
    instrumenting_library_version: str | None = None,
    tracer_provider: NoopTracerProvider | None = None,
    schema_url: str | None = None,
) -> NoopTracer:
    """Get a noop tracer."""
    provider = tracer_provider or _tracer_provider
    return provider.get_tracer(instrumenting_module_name, instrumenting_library_version)


def get_meter(
    name: str,
    version: str = "",
    meter_provider: NoopMeterProvider | None = None,
) -> NoopMeter:
    """Get a noop meter."""
    provider = meter_provider or _meter_provider
    return provider.get_meter(name, version)


def get_current_span(context: Any = None) -> NoopSpan:
    """Get the current span (always returns noop)."""
    return NoopSpan()


def set_meter_provider(meter_provider: Any) -> None:
    """Noop set_meter_provider."""
    pass


# Noop context module
class NoopContext:
    """Noop context that stores nothing."""

    pass


_SUPPRESS_INSTRUMENTATION_KEY = "suppress_instrumentation"


def context_get_current() -> NoopContext:
    """Get current context (always returns new noop context)."""
    return NoopContext()


def context_set_value(key: str, value: Any, context: NoopContext | None = None) -> NoopContext:
    """Set a value in context (returns same noop context)."""
    return context or NoopContext()


def context_attach(context: NoopContext) -> object:
    """Attach context (returns noop token)."""
    return object()


def context_detach(token: object) -> None:
    """Detach context (does nothing)."""
    pass


# Noop propagation
class NoopGetter:
    """Noop Getter for propagation."""

    def get(self, carrier: Any, key: str) -> list[str] | None:
        return None

    def keys(self, carrier: Any) -> list[str]:
        return []


class NoopSetter:
    """Noop Setter for propagation."""

    def set(self, carrier: Any, key: str, value: str) -> None:
        pass


def propagate_inject(
    carrier: Any,
    context: Any = None,
    setter: NoopSetter | None = None,
) -> None:
    """Noop inject (does nothing)."""
    pass


def propagate_extract(
    carrier: Any,
    context: Any = None,
    getter: NoopGetter | None = None,
) -> NoopContext:
    """Noop extract (returns noop context)."""
    return NoopContext()

