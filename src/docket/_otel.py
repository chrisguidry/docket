"""Unified OpenTelemetry interface with fallback to stubs.

This module provides a single import point for OpenTelemetry functionality.
When OpenTelemetry is installed, it uses the real implementation.
When not installed, it falls back to noop stubs.

Usage:
    from docket._otel import trace, metrics, get_tracer, get_meter, OTEL_AVAILABLE
"""

from typing import Any

# Try importing OpenTelemetry, fall back to stubs if not available
try:
    from opentelemetry import context as _otel_context
    from opentelemetry import metrics as _otel_metrics
    from opentelemetry import propagate as _otel_propagate
    from opentelemetry import trace as _otel_trace
    from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
    from opentelemetry.metrics import set_meter_provider
    from opentelemetry.propagators.textmap import Getter, Setter
    from opentelemetry.trace import Link, SpanKind, Status, StatusCode

    OTEL_AVAILABLE = True

    # Re-export trace functions
    def get_tracer(
        instrumenting_module_name: str,
        instrumenting_library_version: str | None = None,
        tracer_provider: Any = None,
        schema_url: str | None = None,
    ) -> Any:
        """Get a tracer from OpenTelemetry."""
        return _otel_trace.get_tracer(
            instrumenting_module_name,
            instrumenting_library_version,
            tracer_provider,
            schema_url,
        )

    def get_current_span(context: Any = None) -> Any:
        """Get the current span."""
        return _otel_trace.get_current_span(context)

    # Re-export metrics functions
    def get_meter(
        name: str,
        version: str = "",
        meter_provider: Any = None,
    ) -> Any:
        """Get a meter from OpenTelemetry."""
        return _otel_metrics.get_meter(name, version, meter_provider)

    # Re-export context functions
    def context_get_current() -> Any:
        """Get the current context."""
        return _otel_context.get_current()

    def context_set_value(key: str, value: Any, context: Any = None) -> Any:
        """Set a value in context."""
        return _otel_context.set_value(key, value, context)

    def context_attach(context: Any) -> object:
        """Attach a context."""
        return _otel_context.attach(context)

    def context_detach(token: object) -> None:
        """Detach a context."""
        _otel_context.detach(token)

    # Re-export propagation functions
    def propagate_inject(
        carrier: Any,
        context: Any = None,
        setter: Any = None,
    ) -> None:
        """Inject trace context into a carrier."""
        _otel_propagate.inject(carrier, context, setter)

    def propagate_extract(
        carrier: Any,
        context: Any = None,
        getter: Any = None,
    ) -> Any:
        """Extract trace context from a carrier."""
        return _otel_propagate.extract(carrier, context, getter)

    # Context type for type hints
    Context = _otel_context.Context

except ImportError:
    from ._otel_stubs import (
        Link,
        NoopContext,
        NoopGetter as Getter,
        NoopSetter as Setter,
        SpanKind,
        Status,
        StatusCode,
        _SUPPRESS_INSTRUMENTATION_KEY,
        context_attach,
        context_detach,
        context_get_current,
        context_set_value,
        get_current_span,
        get_meter,
        get_tracer,
        propagate_extract,
        propagate_inject,
        set_meter_provider,
    )

    OTEL_AVAILABLE = False

    # Context type for type hints when OTel not available
    Context = NoopContext  # type: ignore[misc,assignment]


__all__ = [
    "OTEL_AVAILABLE",
    "Context",
    "Getter",
    "Link",
    "Setter",
    "SpanKind",
    "Status",
    "StatusCode",
    "_SUPPRESS_INSTRUMENTATION_KEY",
    "context_attach",
    "context_detach",
    "context_get_current",
    "context_set_value",
    "get_current_span",
    "get_meter",
    "get_tracer",
    "propagate_extract",
    "propagate_inject",
    "set_meter_provider",
]

