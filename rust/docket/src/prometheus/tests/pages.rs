//! Each page here is what pydocket's vendored exporter and
//! `prometheus_client` 0.26 render for the same measurements, so a match
//! means docket-rs serves the same text as pydocket.

use opentelemetry::metrics::Meter;
use opentelemetry::{Array, KeyValue, StringValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};

use super::super::Exporter;

/// An exporter whose resource has only `attributes`, so that the page does
/// not change with the SDK's version.
fn exporter_with(attributes: Vec<KeyValue>) -> Exporter {
    let resource = Resource::builder_empty()
        .with_attributes(attributes)
        .build();
    Exporter::with_provider(SdkMeterProvider::builder().with_resource(resource))
}

fn exporter() -> Exporter {
    exporter_with(vec![KeyValue::new("service.name", "docket-test")])
}

fn meter(exporter: &Exporter) -> Meter {
    opentelemetry::metrics::MeterProvider::meter(exporter.meter_provider(), "docket")
}

#[test]
fn renders_each_kind_of_docket_metric() {
    let exporter = exporter();
    let meter = meter(&exporter);
    let added = meter
        .u64_counter("docket_tasks_added")
        .with_unit("1")
        .with_description("How many tasks added to the docket")
        .build();
    added.add(1, &[KeyValue::new("docket.name", "d")]);
    added.add(
        2,
        &[
            KeyValue::new("docket.name", "d"),
            KeyValue::new("docket.task", "t"),
        ],
    );
    let running = meter
        .i64_up_down_counter("docket_tasks_running")
        .with_unit("1")
        .with_description("How many tasks running")
        .build();
    let worker = [
        KeyValue::new("docket.name", "d"),
        KeyValue::new("docket.worker", "w"),
    ];
    running.add(2, &worker);
    running.add(-1, &worker);
    let duration = meter
        .f64_histogram("docket_task_duration")
        .with_unit("s")
        .with_description("How long tasks run")
        .build();
    duration.record(0.5, &[KeyValue::new("docket.name", "d")]);
    duration.record(7.0, &[KeyValue::new("docket.name", "d")]);
    meter
        .u64_gauge("docket_queue_depth")
        .with_unit("1")
        .with_description("How deep the queue is")
        .build()
        .record(3, &[KeyValue::new("docket.name", "d")]);

    assert_eq!(exporter.render(), KINDS);
}

const KINDS: &str = r#"# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="docket-test"} 1.0
# HELP docket_tasks_added_total How many tasks added to the docket
# TYPE docket_tasks_added_total counter
docket_tasks_added_total{docket_name="d"} 1.0
# HELP docket_tasks_added_total How many tasks added to the docket
# TYPE docket_tasks_added_total counter
docket_tasks_added_total{docket_name="d",docket_task="t"} 2.0
# HELP docket_tasks_running How many tasks running
# TYPE docket_tasks_running gauge
docket_tasks_running{docket_name="d",docket_worker="w"} 1.0
# HELP docket_task_duration_seconds How long tasks run
# TYPE docket_task_duration_seconds histogram
docket_task_duration_seconds_bucket{docket_name="d",le="0.0"} 0.0
docket_task_duration_seconds_bucket{docket_name="d",le="5.0"} 1.0
docket_task_duration_seconds_bucket{docket_name="d",le="10.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="25.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="50.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="75.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="100.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="250.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="500.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="750.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="1000.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="2500.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="5000.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="7500.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="10000.0"} 2.0
docket_task_duration_seconds_bucket{docket_name="d",le="+Inf"} 2.0
docket_task_duration_seconds_count{docket_name="d"} 2.0
docket_task_duration_seconds_sum{docket_name="d"} 7.5
# HELP docket_queue_depth How deep the queue is
# TYPE docket_queue_depth gauge
docket_queue_depth{docket_name="d"} 3.0
"#;

#[test]
fn builds_names_from_units_and_numbers_from_every_type() {
    let exporter = exporter();
    let meter = meter(&exporter);
    meter
        .f64_counter("requests_total")
        .with_unit("s")
        .with_description("Requests")
        .build()
        .add(1.5, &[]);
    meter
        .f64_up_down_counter("http.server.active")
        .with_unit("{request}")
        .with_description("Active")
        .build()
        .add(-2.5, &[]);
    meter
        .f64_histogram("latency_seconds")
        .with_unit("s")
        .with_description("Latency")
        .build()
        .record(0.3, &[KeyValue::new("zone", "a")]);
    meter
        .u64_histogram("payload")
        .with_unit("By")
        .with_description("Payload")
        .build()
        .record(1_234_567, &[]);
    meter
        .i64_gauge("temperature")
        .with_unit("Cel")
        .with_description("Temperature")
        .build()
        .record(-40, &[]);
    meter
        .f64_gauge("ratio")
        .with_unit("1")
        .with_description("Ratio")
        .build()
        .record(12_345_678.9, &[]);

    assert_eq!(exporter.render(), NAMES);
}

const NAMES: &str = r#"# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="docket-test"} 1.0
# HELP requests_seconds_total Requests
# TYPE requests_seconds_total counter
requests_seconds_total 1.5
# HELP http_server_active Active
# TYPE http_server_active gauge
http_server_active -2.5
# HELP latency_seconds Latency
# TYPE latency_seconds histogram
latency_seconds_bucket{le="0.0",zone="a"} 0.0
latency_seconds_bucket{le="5.0",zone="a"} 1.0
latency_seconds_bucket{le="10.0",zone="a"} 1.0
latency_seconds_bucket{le="25.0",zone="a"} 1.0
latency_seconds_bucket{le="50.0",zone="a"} 1.0
latency_seconds_bucket{le="75.0",zone="a"} 1.0
latency_seconds_bucket{le="100.0",zone="a"} 1.0
latency_seconds_bucket{le="250.0",zone="a"} 1.0
latency_seconds_bucket{le="500.0",zone="a"} 1.0
latency_seconds_bucket{le="750.0",zone="a"} 1.0
latency_seconds_bucket{le="1000.0",zone="a"} 1.0
latency_seconds_bucket{le="2500.0",zone="a"} 1.0
latency_seconds_bucket{le="5000.0",zone="a"} 1.0
latency_seconds_bucket{le="7500.0",zone="a"} 1.0
latency_seconds_bucket{le="10000.0",zone="a"} 1.0
latency_seconds_bucket{le="+Inf",zone="a"} 1.0
latency_seconds_count{zone="a"} 1.0
latency_seconds_sum{zone="a"} 0.3
# HELP payload_bytes Payload
# TYPE payload_bytes histogram
payload_bytes_bucket{le="0.0"} 0.0
payload_bytes_bucket{le="5.0"} 0.0
payload_bytes_bucket{le="10.0"} 0.0
payload_bytes_bucket{le="25.0"} 0.0
payload_bytes_bucket{le="50.0"} 0.0
payload_bytes_bucket{le="75.0"} 0.0
payload_bytes_bucket{le="100.0"} 0.0
payload_bytes_bucket{le="250.0"} 0.0
payload_bytes_bucket{le="500.0"} 0.0
payload_bytes_bucket{le="750.0"} 0.0
payload_bytes_bucket{le="1000.0"} 0.0
payload_bytes_bucket{le="2500.0"} 0.0
payload_bytes_bucket{le="5000.0"} 0.0
payload_bytes_bucket{le="7500.0"} 0.0
payload_bytes_bucket{le="10000.0"} 0.0
payload_bytes_bucket{le="+Inf"} 1.0
payload_bytes_count 1.0
payload_bytes_sum 1.234567e+06
# HELP temperature_celsius Temperature
# TYPE temperature_celsius gauge
temperature_celsius -40.0
# HELP ratio Ratio
# TYPE ratio gauge
ratio 1.23456789e+07
"#;

#[test]
fn escapes_help_text_label_names_and_label_values() {
    let exporter = exporter();
    meter(&exporter)
        .u64_counter("weird")
        .with_unit("1")
        .with_description("A \"help\" with \\ and\nnewline")
        .build()
        .add(
            1,
            &[
                KeyValue::new("9lives", "a\"b\\c\nd"),
                KeyValue::new("a.b-c", "x"),
            ],
        );

    assert_eq!(exporter.render(), ESCAPES);
}

const ESCAPES: &str = r#"# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="docket-test"} 1.0
# HELP weird_total A "help" with \\ and\nnewline
# TYPE weird_total counter
weird_total{_lives="a\"b\\c\nd",a_b_c="x"} 1.0
"#;

fn strings(texts: &[&'static str]) -> Value {
    Value::Array(Array::String(
        texts.iter().map(|text| StringValue::from(*text)).collect(),
    ))
}

#[test]
fn writes_label_values_that_are_not_strings_as_json() {
    let exporter = exporter();
    let meter = meter(&exporter);
    meter
        .u64_counter("typed")
        .with_unit("1")
        .with_description("Typed")
        .build()
        .add(
            1,
            &[
                KeyValue::new("bool", true),
                KeyValue::new("int", 3_i64),
                KeyValue::new("float", 1.5),
                KeyValue::new("big", 1e20),
                KeyValue::new("inf", f64::INFINITY),
                KeyValue::new("nan", f64::NAN),
                KeyValue::new("ninf", f64::NEG_INFINITY),
            ],
        );
    meter
        .u64_counter("lists")
        .with_unit("1")
        .with_description("Lists")
        .build()
        .add(
            1,
            &[
                KeyValue::new("bools", Value::Array(Array::Bool(vec![true, false]))),
                KeyValue::new("ints", Value::Array(Array::I64(vec![1, -2]))),
                KeyValue::new(
                    "floats",
                    Value::Array(Array::F64(vec![0.5, 1e-7, f64::INFINITY])),
                ),
                KeyValue::new(
                    "strings",
                    strings(&["plain", "q\"b\\n\n\r\t\u{8}\u{c}\u{1}\u{7f}é😀"]),
                ),
            ],
        );

    assert_eq!(exporter.render(), TYPED);
}

const TYPED: &str = r#"# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="docket-test"} 1.0
# HELP typed_total Typed
# TYPE typed_total counter
typed_total{big="1e+20",bool="true",float="1.5",inf="Infinity",int="3",nan="NaN",ninf="-Infinity"} 1.0
# HELP lists_total Lists
# TYPE lists_total counter
lists_total{bools="[true, false]",floats="[0.5, 1e-07, Infinity]",ints="[1, -2]",strings="[\"plain\", \"q\\\"b\\\\n\\n\\r\\t\\b\\f\\u0001\\u007f\\u00e9\\ud83d\\ude00\"]"} 1.0
"#;

#[test]
fn writes_each_bucket_bound_as_python_does() {
    let exporter = exporter();
    meter(&exporter)
        .f64_histogram("sizes")
        .with_unit("By")
        .with_description("Sizes")
        .with_boundaries(vec![0.005, 0.25, 1_234_567.0, 1e16])
        .build()
        .record(2.0, &[]);

    assert_eq!(exporter.render(), BOUNDS);
}

const BOUNDS: &str = r#"# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="docket-test"} 1.0
# HELP sizes_bytes Sizes
# TYPE sizes_bytes histogram
sizes_bytes_bucket{le="0.005"} 0.0
sizes_bytes_bucket{le="0.25"} 0.0
sizes_bytes_bucket{le="1234567.0"} 1.0
sizes_bytes_bucket{le="1e+16"} 1.0
sizes_bytes_bucket{le="+Inf"} 1.0
sizes_bytes_count 1.0
sizes_bytes_sum 2.0
"#;

#[test]
fn labels_target_info_with_the_resource() {
    let exporter = exporter_with(vec![
        KeyValue::new("service.name", "s"),
        KeyValue::new("9.x", 2_i64),
        KeyValue::new("deploy.env", strings(&["a", "b"])),
    ]);
    meter(&exporter).u64_counter("c").build().add(1, &[]);

    // An instrument without a description has an empty help text, and the
    // `# HELP` line still has its space.
    assert_eq!(
        exporter.render(),
        "# HELP target_info Target metadata\n\
         # TYPE target_info gauge\n\
         target_info{_x=\"2\",deploy_env=\"[\\\"a\\\", \\\"b\\\"]\",service_name=\"s\"} 1.0\n\
         # HELP c_total \n\
         # TYPE c_total counter\n\
         c_total 1.0\n"
    );
}

#[test]
fn leaves_out_exponential_histograms() {
    let exponential = |instrument: &Instrument| {
        Stream::builder()
            .with_aggregation(Aggregation::Base2ExponentialHistogram {
                max_size: 160,
                max_scale: 20,
                record_min_max: true,
            })
            .build()
            .ok()
            .filter(|_| instrument.name() == "sizes")
    };
    let resource = Resource::builder_empty()
        .with_attributes([KeyValue::new("service.name", "docket-test")])
        .build();
    let exporter = Exporter::with_provider(
        SdkMeterProvider::builder()
            .with_resource(resource)
            .with_view(exponential),
    );
    meter(&exporter)
        .f64_histogram("sizes")
        .build()
        .record(2.0, &[]);

    assert_eq!(
        exporter.render(),
        "# HELP target_info Target metadata\n\
         # TYPE target_info gauge\n\
         target_info{service_name=\"docket-test\"} 1.0\n"
    );
}
