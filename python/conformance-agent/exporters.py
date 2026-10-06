"""The OpenTelemetry SDK that the agent installs when the driver asks for it.

pydocket records metrics and spans through ``opentelemetry-api`` only, and
records nothing until an application installs providers.  The driver sets
``OTEL_EXPORTER_OTLP_ENDPOINT`` when it wants the agent's telemetry, and the
SDK reads that variable and the other standard ``OTEL_*`` variables itself.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def exporting() -> Generator[None, None, None]:
    """Export traces and metrics over OTLP/HTTP until the block ends."""
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        yield
        return

    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    meter_provider = MeterProvider(
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())]
    )
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(meter_provider)
    try:
        yield
    finally:
        # The driver reads the telemetry only after the agent exits, and the
        # batches still in memory go out only when the providers shut down.
        tracer_provider.shutdown()
        meter_provider.shutdown()
