//! The OpenTelemetry SDK that the agent installs when the driver asks for
//! it, as pydocket's agent does.
//!
//! docket-rs records metrics and spans through the `opentelemetry` API only,
//! and records nothing until an application installs providers.  The driver
//! sets `OTEL_EXPORTER_OTLP_ENDPOINT` when it wants the agent's telemetry
//! over OTLP, and `DOCKET_WORKER_METRICS_PORT` when it wants a worker to
//! serve Prometheus metrics.  The SDK reads the other `OTEL_*` variables
//! itself.

use opentelemetry::global;
use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::SdkTracerProvider;

/// The providers the agent installed, which it shuts down before it exits.
pub struct Exporting {
    otlp: Option<(SdkTracerProvider, SdkMeterProvider)>,
    /// The Prometheus exporter and the port to serve it on.
    pub prometheus: Option<(docket::prometheus::Exporter, u16)>,
}

/// Installs the providers the environment asks for.  A docket binds to the
/// global providers when it connects, so this comes first.
pub fn start() -> Exporting {
    // pydocket's default propagators, which OTEL_PROPAGATORS defaults to.
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
    let otlp = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").map(|_| {
        let spans = SpanExporter::builder()
            .with_http()
            .build()
            .expect("the OTLP span exporter builds");
        let tracers = SdkTracerProvider::builder()
            .with_batch_exporter(spans)
            .build();
        let metrics = MetricExporter::builder()
            .with_http()
            .build()
            .expect("the OTLP metric exporter builds");
        let meters = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metrics).build())
            .build();
        global::set_tracer_provider(tracers.clone());
        global::set_meter_provider(meters.clone());
        (tracers, meters)
    });
    let prometheus = std::env::var("DOCKET_WORKER_METRICS_PORT")
        .ok()
        .map(|port| {
            let port = port.parse().expect("DOCKET_WORKER_METRICS_PORT is a port");
            (docket::prometheus::Exporter::install(), port)
        });
    Exporting { otlp, prometheus }
}

impl Exporting {
    /// Sends what is still in memory.  The driver reads the telemetry only
    /// after the agent exits.
    pub fn shutdown(self) {
        if let Some((tracers, meters)) = self.otlp {
            let _ = tracers.shutdown();
            let _ = meters.shutdown();
        }
    }
}
