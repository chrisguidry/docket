//! The spans and metrics every test docket records, read back from the
//! SDK's in-memory exporters.
//!
//! A docket binds its tracers and instruments to the global providers when
//! it connects, so `support::docket` installs these first.  The providers
//! are global, so the tests share them, and each test reads only what its
//! own docket recorded.

use std::sync::LazyLock;
use std::time::Duration;

use docket::Docket;
use opentelemetry::{KeyValue, Value, global};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};

pub struct Recorded {
    spans: InMemorySpanExporter,
    metrics: InMemoryMetricExporter,
    meters: SdkMeterProvider,
}

static RECORDED: LazyLock<Recorded> = LazyLock::new(|| {
    let spans = InMemorySpanExporter::default();
    let tracers = SdkTracerProvider::builder()
        .with_simple_exporter(spans.clone())
        .build();
    let metrics = InMemoryMetricExporter::default();
    // Metrics export only when a test flushes them.
    let reader = PeriodicReader::builder(metrics.clone())
        .with_interval(Duration::from_secs(3600))
        .build();
    let meters = SdkMeterProvider::builder().with_reader(reader).build();
    global::set_tracer_provider(tracers);
    global::set_meter_provider(meters.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    Recorded {
        spans,
        metrics,
        meters,
    }
});

/// Installs the providers, once for the whole run.
pub fn install() {
    LazyLock::force(&RECORDED);
}

/// Whether a span or a data point belongs to `docket`.
fn ours(attributes: &[KeyValue], docket: &Docket) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key.as_str() == "docket.name"
            && attribute.value == Value::from(docket.name().to_owned())
    })
}

/// The spans `docket` ended so far, oldest first.
pub fn spans(docket: &Docket) -> Vec<SpanData> {
    let mut spans: Vec<SpanData> = RECORDED
        .spans
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|span| ours(&span.attributes, docket))
        .collect();
    spans.sort_by_key(|span| span.start_time);
    spans
}

/// The spans named `name` that `docket` ended.
pub fn spans_named(docket: &Docket, name: &str) -> Vec<SpanData> {
    spans(docket)
        .into_iter()
        .filter(|span| span.name == name)
        .collect()
}

/// A span attribute's value, as text.
pub fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.to_string())
}

/// One data point of a metric: its attributes other than `docket.name`, as
/// `key=value` text sorted by key, and its value.  A histogram's value is
/// its count.
pub type Point = (Vec<String>, f64);

/// The data points `docket` recorded for the metric `name`, now.
pub fn points(docket: &Docket, name: &str) -> Vec<Point> {
    RECORDED.meters.force_flush().unwrap();
    let exported = RECORDED.metrics.get_finished_metrics().unwrap();
    let Some(latest) = exported.last() else {
        return Vec::new();
    };
    let mut points: Vec<Point> = latest
        .scope_metrics()
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(metric_points)
        .filter(|(attributes, _)| ours(attributes, docket))
        .map(|(attributes, value)| (described(&attributes), value))
        .collect();
    points.sort_by(|a, b| a.0.cmp(&b.0));
    points
}

/// The value of the one point of `name` whose attributes include all of
/// `labels`, or `None` when there is no such point.
pub fn value(docket: &Docket, name: &str, labels: &[&str]) -> Option<f64> {
    points(docket, name)
        .into_iter()
        .find(|(attributes, _)| {
            labels
                .iter()
                .all(|label| attributes.iter().any(|a| a == label))
        })
        .map(|(_, value)| value)
}

fn described(attributes: &[KeyValue]) -> Vec<String> {
    let mut described: Vec<String> = attributes
        .iter()
        .filter(|attribute| attribute.key.as_str() != "docket.name")
        .map(|attribute| format!("{}={}", attribute.key, attribute.value))
        .collect();
    described.sort();
    described
}

#[expect(clippy::cast_precision_loss, reason = "the tests' counts are small")]
fn metric_points(metric: &Metric) -> Vec<(Vec<KeyValue>, f64)> {
    let attributes = |points: &mut dyn Iterator<Item = &KeyValue>| points.cloned().collect();
    match metric.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
            .data_points()
            .map(|point| (attributes(&mut point.attributes()), point.value() as f64))
            .collect(),
        AggregatedMetrics::I64(MetricData::Sum(sum)) => sum
            .data_points()
            .map(|point| (attributes(&mut point.attributes()), point.value() as f64))
            .collect(),
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .map(|point| (attributes(&mut point.attributes()), point.value() as f64))
            .collect(),
        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
            .data_points()
            .map(|point| (attributes(&mut point.attributes()), point.count() as f64))
            .collect(),
        other => panic!("docket records no metric like {other:?}"),
    }
}
