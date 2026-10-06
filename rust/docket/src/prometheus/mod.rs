//! Serves docket's metrics to Prometheus, with the same names, labels, and
//! text as pydocket's `--metrics-port`.
//!
//! A docket binds its instruments to the global meter provider when it
//! connects, so install the exporter first:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let exporter = docket::prometheus::Exporter::install();
//! let docket = docket::Docket::connect("orders", "redis://localhost:6379/0").await?;
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:9090").await?;
//! tokio::spawn(exporter.serve(listener));
//! # Ok(())
//! # }
//! ```
//!
//! An application with a meter provider of its own can take
//! [`Exporter::meter_provider`] for its instruments, or render the page
//! with [`Exporter::render`] from its own HTTP server.

mod names;
mod text;
mod values;

use std::io;
use std::sync::{Arc, Weak};
use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, MeterProviderBuilder, Pipeline, SdkMeterProvider, Temporality,
};
use tokio::net::TcpListener;

/// The content type of the Prometheus text format that [`Exporter::render`]
/// writes.
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Collects docket's metrics and renders them in the Prometheus text format.
#[derive(Clone, Debug)]
pub struct Exporter {
    provider: SdkMeterProvider,
    reader: SharedReader,
}

impl Exporter {
    /// An exporter with a meter provider of its own, which has the SDK's
    /// default resource.  Its instruments record only into this exporter.
    #[must_use]
    pub fn new() -> Self {
        Self::with_provider(SdkMeterProvider::builder())
    }

    /// An exporter whose meter provider is the global one, so that every
    /// docket that connects after this records into it.
    #[must_use]
    pub fn install() -> Self {
        let exporter = Self::new();
        opentelemetry::global::set_meter_provider(exporter.provider.clone());
        exporter
    }

    /// Builds the provider from `builder`, with this exporter's reader.
    fn with_provider(builder: MeterProviderBuilder) -> Self {
        let reader = SharedReader(Arc::new(ManualReader::builder().build()));
        let provider = builder.with_reader(reader.clone()).build();
        Self { provider, reader }
    }

    /// The meter provider whose metrics this exporter renders.
    #[must_use]
    pub fn meter_provider(&self) -> &SdkMeterProvider {
        &self.provider
    }

    /// Collects the metrics now and renders them in the Prometheus text
    /// format, version 0.0.4.  The page is empty before anything records.
    #[must_use]
    pub fn render(&self) -> String {
        let mut collected = ResourceMetrics::default();
        // A provider that has shut down collects nothing, and nothing
        // renders as an empty page, which is the right answer for it.
        let _ = self.reader.collect(&mut collected);
        text::render(&collected)
    }

    /// Answers every request on `listener` with [`render`](Self::render),
    /// on any path, until the future is dropped.  It skips a connection
    /// that fails before it is accepted, so it never returns on its own.
    ///
    /// The future holds its own clone of the exporter, so it can be spawned.
    pub fn serve(
        &self,
        listener: TcpListener,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let exporter = self.clone();
        crate::serving::serve(listener, CONTENT_TYPE, move || exporter.render())
    }
}

impl Default for Exporter {
    fn default() -> Self {
        Self::new()
    }
}

/// A reader that the exporter keeps a handle to after the provider takes
/// it.  The provider owns the readers it is given, and without this handle
/// the exporter could not ask for a collection.
#[derive(Clone, Debug)]
struct SharedReader(Arc<ManualReader>);

impl MetricReader for SharedReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }

    fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(metrics)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    /// Cumulative for every instrument, so that counters count from the
    /// start, as Prometheus expects.
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

#[cfg(test)]
mod tests;
