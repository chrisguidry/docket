use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::super::Exporter;

#[test]
fn renders_nothing_before_anything_records() {
    assert_eq!(Exporter::new().render(), "");
}

#[test]
fn renders_the_sdk_default_resource() {
    let exporter = Exporter::default();
    let meter = exporter.meter_provider().meter("docket");
    meter.u64_counter("c").build().add(1, &[]);

    let page = exporter.render();

    assert!(
        page.starts_with("# HELP target_info Target metadata\n"),
        "{page}"
    );
    assert!(page.contains("service_name=\"unknown_service"), "{page}");
    assert!(page.contains("telemetry_sdk_language=\"rust\""), "{page}");
}

#[test]
fn leaves_off_target_info_when_asked() {
    let exporter = Exporter::new().without_target_info();
    let meter = exporter.meter_provider().meter("docket");
    meter.u64_counter("c").build().add(1, &[]);

    let page = exporter.render();

    assert_eq!(
        page,
        "# HELP c_total \n# TYPE c_total counter\nc_total 1.0\n"
    );
}

#[test]
fn counts_from_the_start_on_every_render() {
    let exporter = Exporter::new();
    let counter = exporter
        .meter_provider()
        .meter("docket")
        .u64_counter("c")
        .build();
    counter.add(1, &[]);
    assert!(exporter.render().ends_with("\nc_total 1.0\n"));

    counter.add(2, &[]);
    assert!(exporter.render().ends_with("\nc_total 3.0\n"));
}

#[test]
fn flushes_without_error() {
    let exporter = Exporter::new();
    exporter.meter_provider().force_flush().unwrap();
}

#[test]
fn renders_nothing_once_its_provider_shuts_down() {
    let exporter = Exporter::new();
    let meter = exporter.meter_provider().meter("docket");
    meter.u64_counter("c").build().add(1, &[]);

    exporter.meter_provider().shutdown().unwrap();

    assert_eq!(exporter.render(), "");
}

#[test]
fn renders_with_the_views_of_the_applications_builder() {
    let punctuality = |instrument: &Instrument| {
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: vec![0.5, 5.0],
                record_min_max: false,
            })
            .build()
            .ok()
            .filter(|_| instrument.name() == "docket_task_punctuality")
    };
    let exporter = Exporter::with_provider(SdkMeterProvider::builder().with_view(punctuality));
    let meter = exporter.meter_provider().meter("docket");
    meter
        .f64_histogram("docket_task_punctuality")
        .with_unit("s")
        .build()
        .record(1.0, &[]);

    let page = exporter.render();

    assert!(
        page.contains(
            "docket_task_punctuality_seconds_bucket{le=\"0.5\"} 0.0\n\
             docket_task_punctuality_seconds_bucket{le=\"5.0\"} 1.0\n\
             docket_task_punctuality_seconds_bucket{le=\"+Inf\"} 1.0\n"
        ),
        "{page}"
    );
}

#[tokio::test]
async fn serves_the_page_to_a_scrape() {
    let exporter = Exporter::new();
    let meter = exporter.meter_provider().meter("docket");
    let counter = meter.u64_counter("c").build();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serving = exporter.clone();
    tokio::spawn(async move { serving.serve(listener).await });
    // The page is rendered when the request arrives, not when serving starts.
    counter.add(1, &[]);

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();

    let page = exporter.render();
    assert_eq!(
        answer,
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n\
             {page}",
            page.len()
        )
    );
}
