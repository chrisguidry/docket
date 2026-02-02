"""Pytest configuration for instrumentation tests.

These tests require OpenTelemetry to be installed. If not available,
all tests in this directory are skipped.
"""

import pytest

from docket._otel import OTEL_AVAILABLE

# Skip all tests in this directory if OpenTelemetry is not available
pytestmark = pytest.mark.skipif(
    not OTEL_AVAILABLE,
    reason="OpenTelemetry not installed (install with: pip install pydocket[telemetry])",
)

