use opentelemetry::metrics::MeterProvider;
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
