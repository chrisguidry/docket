//! The spans docket records, with pydocket's names, kinds, and attributes,
//! and the links from each run to the span that scheduled it.

use std::sync::Arc;
use std::time::Duration;

use docket::{Cooldown, Perpetual, Retry, Strike};
use opentelemetry::trace::{SpanId, SpanKind, Status, TraceId};
use opentelemetry_sdk::trace::SpanData;
use tokio::sync::Notify;

use crate::support::telemetry::{attribute, spans, spans_named};
use crate::support::{Echo, Noop, docket, within, worker};

fn failing(
    succeed_on: u32,
) -> impl Fn(docket::Context, Noop) -> std::future::Ready<Result<(), std::io::Error>> {
    move |ctx, _| {
        std::future::ready(if ctx.attempt() >= succeed_on {
            Ok(())
        } else {
            Err(std::io::Error::other("boom"))
        })
    }
}

/// The spans that `span` links to, by trace and span ID.  A linked context
/// came through Redis, so it is remote, unlike the span it names.
fn links(span: &SpanData) -> Vec<(TraceId, SpanId)> {
    span.links
        .iter()
        .map(|link| (link.span_context.trace_id(), link.span_context.span_id()))
        .collect()
}

fn ids(span: &SpanData) -> (TraceId, SpanId) {
    (span.span_context.trace_id(), span.span_context.span_id())
}

fn events(span: &SpanData) -> Vec<(String, Option<String>)> {
    span.events
        .iter()
        .map(|event| {
            let message = event
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == "exception.message")
                .map(|attribute| attribute.value.to_string());
            (event.name.to_string(), message)
        })
        .collect()
}

#[tokio::test]
async fn an_add_records_a_producer_span() {
    let docket = docket().await;
    docket.add(Echo::new("hi")).key("greeting").await.unwrap();

    let [add] = &spans_named(&docket, "docket.add")[..] else {
        panic!("one add span")
    };
    assert_eq!(add.instrumentation_scope.name(), "docket.docket");
    assert_eq!(add.span_kind, SpanKind::Internal);
    assert_eq!(add.status, Status::Unset);
    for (key, expected) in [
        ("docket.task", "echo"),
        ("docket.key", "greeting"),
        ("docket.attempt", "1"),
        ("code.function.name", "echo"),
        ("docket.disposition", "scheduled"),
    ] {
        assert_eq!(attribute(add, key).as_deref(), Some(expected), "{key}");
    }
    assert!(attribute(add, "docket.when").is_some());
}

#[tokio::test]
async fn a_run_records_a_consumer_span_linked_to_its_add() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket.add(Echo::new("hi")).key("greeting").await.unwrap();

    within(10, worker(&docket).name("tracer").run_until_finished())
        .await
        .unwrap();

    let [add] = &spans_named(&docket, "docket.add")[..] else {
        panic!("one add span")
    };
    let [run] = &spans_named(&docket, "echo")[..] else {
        panic!("one run span")
    };
    assert_eq!(run.instrumentation_scope.name(), "docket.worker");
    assert_eq!(run.span_kind, SpanKind::Consumer);
    assert_eq!(run.status, Status::Ok);
    assert_eq!(run.parent_span_id, SpanId::INVALID);
    assert_eq!(attribute(run, "docket.worker").as_deref(), Some("tracer"));
    assert_eq!(attribute(run, "docket.key").as_deref(), Some("greeting"));
    assert_eq!(links(run), [ids(add)]);
}

#[tokio::test]
async fn a_failed_run_records_its_error() {
    let docket = docket().await;
    docket.register(failing(2));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let [run] = &spans_named(&docket, "noop")[..] else {
        panic!("one run span")
    };
    assert_eq!(run.status, Status::error("boom"));
    assert_eq!(
        events(run),
        [("exception".to_owned(), Some("boom".to_owned()))]
    );
}

#[tokio::test]
async fn a_retry_links_to_the_run_that_failed() {
    let docket = docket().await;
    docket.register(failing(2)).with(Retry::attempts(2));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let [first, second] = &spans_named(&docket, "noop")[..] else {
        panic!("two run spans")
    };
    assert_eq!(attribute(second, "docket.attempt").as_deref(), Some("2"));
    assert_eq!(links(second), [ids(first)]);
}

#[tokio::test]
async fn a_perpetual_task_replaces_itself_inside_its_run() {
    let docket = docket().await;
    docket
        .register(|ctx: docket::Context, _: Noop| {
            ctx.perpetual().unwrap().cancel();
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    docket
        .replace(Noop, "forever", chrono::Utc::now())
        .await
        .unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let [run] = &spans_named(&docket, "noop")[..] else {
        panic!("one run span")
    };
    assert_eq!(run.status, Status::Ok);
    // The run cancelled itself, so it scheduled nothing.
    assert_eq!(spans_named(&docket, "docket.replace").len(), 1);
}

#[tokio::test]
async fn a_perpetual_run_traces_its_next_run_under_itself() {
    let docket = docket().await;
    let ran = Arc::new(Notify::new());
    let notified = Arc::clone(&ran);
    docket
        .register(move |_ctx, _: Noop| {
            notified.notify_one();
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    docket.add(Noop).key("forever").await.unwrap();

    within(
        10,
        worker(&docket).run_until(async {
            ran.notified().await;
            while spans_named(&docket, "docket.replace").is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }),
    )
    .await
    .unwrap();

    let [run] = &spans_named(&docket, "noop")[..] else {
        panic!("one run span")
    };
    let [next] = &spans_named(&docket, "docket.replace")[..] else {
        panic!("one replace span")
    };
    assert_eq!(next.parent_span_id, run.span_context.span_id());
    assert_eq!(next.span_context.trace_id(), run.span_context.trace_id());
}

#[tokio::test]
async fn a_blocked_run_ends_its_span_ok() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Cooldown::new(Duration::from_secs(60)).scope(docket.name()));
    docket.add(Noop).await.unwrap();
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let runs = spans_named(&docket, "noop");
    assert_eq!(runs.len(), 2);
    assert!(runs.iter().all(|run| run.status == Status::Ok));
    let blocked: Vec<_> = runs.iter().filter(|run| !run.events.is_empty()).collect();
    assert_eq!(blocked.len(), 1);
    assert_eq!(events(blocked[0])[0].0, "exception");
}

#[tokio::test]
async fn the_other_producer_calls_record_spans() {
    let docket = docket().await;
    docket.cancel("gone").await.unwrap();
    docket.strike(Strike::task::<Noop>()).await.unwrap();
    docket.restore(Strike::task::<Noop>()).await.unwrap();
    docket
        .add_many([docket.call(Noop), docket.call(Echo::new("x"))])
        .await
        .unwrap();
    docket.clear().await.unwrap();

    let names: Vec<String> = spans(&docket)
        .iter()
        .map(|span| span.name.to_string())
        .collect();
    assert_eq!(
        names,
        [
            "docket.cancel",
            "docket.strike",
            "docket.restore",
            "docket.add_many",
            "docket.clear"
        ]
    );
    let spans = spans(&docket);
    assert_eq!(attribute(&spans[0], "docket.key").as_deref(), Some("gone"));
    assert_eq!(attribute(&spans[1], "docket.task").as_deref(), Some("noop"));
    assert_eq!(
        attribute(&spans[3], "docket.batch.count").as_deref(),
        Some("2")
    );
    assert_eq!(
        attribute(&spans[3], "docket.batch.stricken").as_deref(),
        Some("0")
    );
}
