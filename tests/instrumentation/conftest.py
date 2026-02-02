"""Pytest configuration for instrumentation tests.

These tests require both OpenTelemetry API and SDK to be installed.
The API provides base types, while the SDK provides implementations
needed for metrics_server and TracerProvider.

If either is missing, all tests in this directory are skipped.
"""

from importlib import import_module

import pytest

from docket._otel import OTEL_AVAILABLE


def _check_otel_sdk_available() -> bool:
    """Check if OpenTelemetry SDK is available (needed for metrics_server and TracerProvider)."""
    try:
        import_module("opentelemetry.sdk.metrics")
        import_module("opentelemetry.sdk.trace")
        return True
    except ImportError:
        return False


OTEL_SDK_AVAILABLE = _check_otel_sdk_available()

# Skip all tests in this directory if OpenTelemetry API or SDK is not available
pytestmark = pytest.mark.skipif(
    not OTEL_AVAILABLE or not OTEL_SDK_AVAILABLE,
    reason="OpenTelemetry SDK not installed (install with: pip install pydocket[metrics])",
)

