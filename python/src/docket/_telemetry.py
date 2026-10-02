"""Vendored OpenTelemetry instrumentation utilities.

This module provides a minimal implementation of suppress_instrumentation
from opentelemetry-instrumentation, using only the opentelemetry-api package.
This avoids pulling in the full instrumentation package with its exact SDK pins.

Original source: opentelemetry-instrumentation (Apache 2.0)
https://github.com/open-telemetry/opentelemetry-python-contrib
"""

from contextlib import contextmanager
from typing import Generator

from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY

_SUPPRESS_INSTRUMENTATION_KEY_PLAIN = "suppress_instrumentation"


@contextmanager
def suppress_instrumentation() -> Generator[None, None, None]:
    """Suppress OpenTelemetry instrumentation within the context.

    When this context manager is active, instrumented libraries will skip
    creating spans. This is useful for internal operations (like Redis polling)
    that would generate excessive noise in traces.
    """
    ctx = context.get_current()
    ctx = context.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True, ctx)
    ctx = context.set_value(_SUPPRESS_INSTRUMENTATION_KEY_PLAIN, True, ctx)
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)
