//! Renders collected metrics in the Prometheus text format, version 0.0.4,
//! the way pydocket's vendored `PrometheusMetricReader` and
//! `prometheus_client`'s `generate_latest` render them.

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, HistogramDataPoint, Metric, MetricData, ResourceMetrics, ScopeMetrics,
};

use super::names::{label_name, metric_name, unit_suffix};
use super::values::{go_float, label_value, python_repr};

/// How Prometheus types a family.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
}

/// One line of a family: the suffix after the family's name, the labels,
/// and the value.
struct Sample {
    suffix: &'static str,
    labels: Vec<(String, String)>,
    value: f64,
}

/// What tells one family apart from another, as pydocket's exporter keys
/// them.  Two series of one metric with different label names are two
/// families.
#[derive(PartialEq, Eq)]
struct FamilyId {
    name: String,
    description: String,
    label_names: Vec<String>,
    unit: String,
    kind: Kind,
}

/// The samples that share one `# HELP` and `# TYPE`.
struct Family {
    /// `None` for `target_info`, which no metric can join.
    id: Option<FamilyId>,
    name: String,
    help: String,
    kind: Kind,
    samples: Vec<Sample>,
}

/// One data point, with its labels as Prometheus names them.
struct Point {
    labels: Vec<(String, String)>,
    value: Reading,
}

enum Reading {
    Number(f64),
    Histogram {
        /// Each upper bound, as Python writes it, with the cumulative count.
        buckets: Vec<(String, f64)>,
        count: f64,
        sum: f64,
    },
}

/// The numbers that the SDK aggregates, as the floats Prometheus samples
/// are.  Python's exporter converts them the same way.
trait Number: Copy {
    fn to_f64(self) -> f64;
}

impl Number for f64 {
    fn to_f64(self) -> f64 {
        self
    }
}

impl Number for u64 {
    #[expect(clippy::cast_precision_loss, reason = "a sample is a float")]
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl Number for i64 {
    #[expect(clippy::cast_precision_loss, reason = "a sample is a float")]
    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// The page for `collected`, with the `target_info` series first when
/// `target_info` is set.  It is empty when nothing has been recorded, as
/// pydocket's is.
pub(super) fn render(collected: &ResourceMetrics, target_info: bool) -> String {
    let metrics: Vec<&Metric> = collected
        .scope_metrics()
        .flat_map(ScopeMetrics::metrics)
        .collect();
    if metrics.is_empty() {
        return String::new();
    }
    let mut families = Vec::new();
    if target_info {
        families.push(resource_family(collected.resource()));
    }
    for metric in metrics {
        add_metric(&mut families, metric);
    }
    families.iter().map(Family::render).collect()
}

/// The `target_info` family, whose labels are the resource's attributes.
fn resource_family(resource: &Resource) -> Family {
    let labels = resource
        .iter()
        .map(|(key, value)| (label_name(key.as_str()), label_value(value)))
        .collect();
    Family {
        id: None,
        name: "target_info".to_owned(),
        help: "Target metadata".to_owned(),
        kind: Kind::Gauge,
        samples: vec![Sample {
            suffix: "",
            labels,
            value: 1.0,
        }],
    }
}

fn add_metric(families: &mut Vec<Family>, metric: &Metric) {
    let (kind, mut points) = match metric.data() {
        AggregatedMetrics::F64(data) => points(data),
        AggregatedMetrics::U64(data) => points(data),
        AggregatedMetrics::I64(data) => points(data),
    };
    // The SDK keeps series in a hash map, so they come out in any order.
    // Sorting them keeps the page the same from one scrape to the next.
    points.sort_by(|a, b| a.labels.cmp(&b.labels));
    let name = metric_name(metric.name());
    let unit = unit_suffix(metric.unit());
    for point in points {
        let id = Some(FamilyId {
            name: name.clone(),
            description: metric.description().to_owned(),
            label_names: point.labels.iter().map(|(name, _)| name.clone()).collect(),
            unit: unit.clone(),
            kind,
        });
        let found = families.iter().position(|family| family.id == id);
        let index = found.unwrap_or_else(|| {
            families.push(Family {
                id,
                name: family_name(&name, &unit, kind),
                help: metric.description().to_owned(),
                kind,
                samples: Vec::new(),
            });
            families.len() - 1
        });
        families[index].samples.extend(point.samples(kind));
    }
}

/// The family's name as `prometheus_client` builds it.  A counter loses a
/// `_total` it already has, since the page adds `_total` back.  The unit
/// goes on the end unless the name already ends with it.
fn family_name(name: &str, unit: &str, kind: Kind) -> String {
    let name = match kind {
        Kind::Counter => name.strip_suffix("_total").unwrap_or(name),
        _ => name,
    };
    if unit.is_empty() || name.ends_with(&format!("_{unit}")) {
        name.to_owned()
    } else {
        format!("{name}_{unit}")
    }
}

fn points<T: Number>(data: &MetricData<T>) -> (Kind, Vec<Point>) {
    match data {
        MetricData::Gauge(gauge) => (
            Kind::Gauge,
            gauge
                .data_points()
                .map(|p| Point::new(p.attributes(), Reading::Number(p.value().to_f64())))
                .collect(),
        ),
        MetricData::Sum(sum) => (
            // docket's reader asks for cumulative sums, and a cumulative
            // sum that can go down, such as an up-down counter, is a gauge.
            if sum.is_monotonic() {
                Kind::Counter
            } else {
                Kind::Gauge
            },
            sum.data_points()
                .map(|p| Point::new(p.attributes(), Reading::Number(p.value().to_f64())))
                .collect(),
        ),
        MetricData::Histogram(histogram) => (
            Kind::Histogram,
            histogram
                .data_points()
                .map(|p| Point::new(p.attributes(), Reading::histogram(p)))
                .collect(),
        ),
        // pydocket's exporter cannot render an exponential histogram, so
        // the page leaves it out.
        MetricData::ExponentialHistogram(_) => (Kind::Histogram, Vec::new()),
    }
}

impl Point {
    /// Labels in the order of their attribute keys, as Python's exporter
    /// sorts them, which also orders the family's label names.
    fn new<'a>(attributes: impl Iterator<Item = &'a KeyValue>, value: Reading) -> Self {
        let mut attributes: Vec<&KeyValue> = attributes.collect();
        attributes.sort_by(|a, b| a.key.cmp(&b.key));
        let labels = attributes
            .iter()
            .map(|attribute| {
                (
                    label_name(attribute.key.as_str()),
                    label_value(&attribute.value),
                )
            })
            .collect();
        Self { labels, value }
    }

    fn samples(self, kind: Kind) -> Vec<Sample> {
        let labels = self.labels;
        match self.value {
            Reading::Number(value) => vec![Sample {
                suffix: if kind == Kind::Counter { "_total" } else { "" },
                labels,
                value,
            }],
            Reading::Histogram {
                buckets,
                count,
                sum,
            } => {
                let mut samples: Vec<Sample> = buckets
                    .into_iter()
                    .map(|(bound, cumulative)| {
                        let mut labels = labels.clone();
                        labels.push(("le".to_owned(), bound));
                        Sample {
                            suffix: "_bucket",
                            labels,
                            value: cumulative,
                        }
                    })
                    .collect();
                samples.push(Sample {
                    suffix: "_count",
                    labels: labels.clone(),
                    value: count,
                });
                samples.push(Sample {
                    suffix: "_sum",
                    labels,
                    value: sum,
                });
                samples
            }
        }
    }
}

impl Reading {
    fn histogram<T: Number>(point: &HistogramDataPoint<T>) -> Self {
        let bounds = point
            .bounds()
            .map(python_repr)
            .chain(std::iter::once("+Inf".to_owned()));
        let mut cumulative = 0;
        let buckets = bounds
            .zip(point.bucket_counts())
            .map(|(bound, count)| {
                cumulative += count;
                (bound, cumulative.to_f64())
            })
            .collect();
        Self::Histogram {
            buckets,
            count: point.count().to_f64(),
            sum: point.sum().to_f64(),
        }
    }
}

impl Family {
    fn render(&self) -> String {
        let (exposed, kind) = match self.kind {
            Kind::Counter => (format!("{}_total", self.name), "counter"),
            Kind::Gauge => (self.name.clone(), "gauge"),
            Kind::Histogram => (self.name.clone(), "histogram"),
        };
        let mut text = format!(
            "# HELP {exposed} {}\n# TYPE {exposed} {kind}\n",
            escape_help(&self.help)
        );
        for sample in &self.samples {
            text.push_str(&self.name);
            text.push_str(sample.suffix);
            text.push_str(&render_labels(&sample.labels));
            text.push(' ');
            text.push_str(&go_float(sample.value));
            text.push('\n');
        }
        text
    }
}

/// `{name="value",...}`, sorted by name as `generate_latest` sorts them, or
/// nothing for no labels.
fn render_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&(String, String)> = labels.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let pairs: Vec<String> = sorted
        .iter()
        .map(|(name, value)| format!("{name}=\"{}\"", escape_label_value(value)))
        .collect();
    format!("{{{}}}", pairs.join(","))
}

fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_label_value(value: &str) -> String {
    escape_help(value).replace('"', "\\\"")
}
