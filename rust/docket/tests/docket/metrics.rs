//! The metrics docket records, with pydocket's names and labels.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use docket::{Docket, Perpetual, Retry, Strike};
use redis::AsyncCommands;

use crate::snapshots::raw;
use crate::support::telemetry::{points, value};
use crate::support::{Echo, Noop, docket, within, worker};

const NOOP: &str = "docket.task=noop";

/// Waits until the metric `name` has a point with `labels` and `expected`.
async fn settles(docket: &Docket, name: &str, labels: &[&str], expected: f64) {
    within(10, async {
        while value(docket, name, labels) != Some(expected) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn an_add_counts_as_added_and_scheduled() {
    let docket = docket().await;
    docket.add(Noop).key("once").await.unwrap();
    docket.add(Noop).key("once").await.unwrap();

    assert_eq!(value(&docket, "docket_tasks_added", &[NOOP]), Some(2.0));
    assert_eq!(value(&docket, "docket_tasks_scheduled", &[NOOP]), Some(1.0));
}

#[tokio::test]
async fn a_replace_counts_as_replaced_cancelled_and_scheduled() {
    let docket = docket().await;
    docket.replace(Noop, "again", Utc::now()).await.unwrap();

    assert_eq!(value(&docket, "docket_tasks_replaced", &[NOOP]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_cancelled", &[NOOP]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_scheduled", &[NOOP]), Some(1.0));
}

#[tokio::test]
async fn a_cancel_counts_without_a_task_label() {
    let docket = docket().await;
    docket.cancel("anything").await.unwrap();

    assert_eq!(
        points(&docket, "docket_tasks_cancelled"),
        [(Vec::new(), 1.0)]
    );
}

#[tokio::test]
async fn a_struck_add_counts_where_the_strike_stopped_it() {
    let docket = docket().await;
    docket.strike(Strike::task::<Noop>()).await.unwrap();
    docket.add(Noop).await.unwrap();

    let labels = [NOOP, "docket.where=docket"];
    assert_eq!(value(&docket, "docket_tasks_stricken", &labels), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_added", &[NOOP]), None);
}

#[tokio::test]
async fn a_batch_counts_each_task_it_places() {
    let docket = docket().await;
    docket.strike(Strike::task::<Echo>()).await.unwrap();
    let calls = [
        docket.call(Noop),
        docket.call(Noop),
        docket.call(Echo::new("no")),
    ];
    docket.add_many(calls).await.unwrap();

    assert_eq!(value(&docket, "docket_tasks_added", &[NOOP]), Some(2.0));
    let echo = ["docket.task=echo", "docket.where=docket"];
    assert_eq!(value(&docket, "docket_tasks_stricken", &echo), Some(1.0));
}

#[tokio::test]
async fn a_run_counts_its_start_and_its_end() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket.add(Echo::new("hi")).await.unwrap();

    within(10, worker(&docket).name("counter").run_until_finished())
        .await
        .unwrap();

    let labels = ["docket.task=echo", "docket.worker=counter"];
    for name in [
        "docket_tasks_started",
        "docket_tasks_succeeded",
        "docket_tasks_completed",
        "docket_task_duration",
        "docket_task_punctuality",
    ] {
        assert_eq!(value(&docket, name, &labels), Some(1.0), "{name}");
    }
    assert_eq!(value(&docket, "docket_tasks_running", &labels), Some(0.0));
    assert_eq!(value(&docket, "docket_tasks_failed", &labels), None);
}

fn failing_until(
    attempt: u32,
) -> impl Fn(docket::Context, Noop) -> std::future::Ready<Result<(), std::io::Error>> {
    move |ctx, _| {
        std::future::ready(if ctx.attempt() >= attempt {
            Ok(())
        } else {
            Err(std::io::Error::other("boom"))
        })
    }
}

#[tokio::test]
async fn a_failed_run_counts_as_failed_and_completed() {
    let docket = docket().await;
    docket.register(failing_until(2));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(value(&docket, "docket_tasks_failed", &[NOOP]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_completed", &[NOOP]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_succeeded", &[NOOP]), None);
}

#[tokio::test]
async fn a_retry_counts_as_retried() {
    let docket = docket().await;
    docket.register(failing_until(2)).with(Retry::attempts(2));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(value(&docket, "docket_tasks_started", &[NOOP]), Some(2.0));
    assert_eq!(value(&docket, "docket_tasks_retried", &[NOOP]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_succeeded", &[NOOP]), Some(1.0));
}

#[tokio::test]
async fn a_perpetual_task_counts_each_next_run_as_a_replace() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |ctx: docket::Context, _: Noop| {
            if counted.fetch_add(1, Ordering::SeqCst) == 2 {
                ctx.perpetual().unwrap().cancel();
            }
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    docket.add(Noop).key("forever").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 3);
    assert_eq!(
        value(&docket, "docket_tasks_perpetuated", &[NOOP]),
        Some(2.0)
    );
    // The replaces come from the docket, so they carry no worker label.
    assert_eq!(
        points(&docket, "docket_tasks_replaced"),
        [(vec![NOOP.to_owned()], 2.0)]
    );
    assert_eq!(value(&docket, "docket_tasks_cancelled", &[NOOP]), Some(2.0));
}

#[tokio::test]
async fn a_perpetual_task_replaced_mid_run_counts_as_superseded() {
    let docket = docket().await;
    docket
        .register(|ctx: docket::Context, _: Noop| async move {
            let later = Utc::now() + chrono::Duration::hours(1);
            ctx.docket().replace(Noop, ctx.key(), later).await?;
            Ok::<_, docket::Error>(())
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    docket.add(Noop).key("taken").await.unwrap();

    within(
        10,
        worker(&docket).run_until(async {
            settles(&docket, "docket_tasks_superseded", &[NOOP], 1.0).await;
        }),
    )
    .await
    .unwrap();

    let labels = [NOOP, "docket.where=on_complete"];
    assert_eq!(
        value(&docket, "docket_tasks_superseded", &labels),
        Some(1.0)
    );
    assert_eq!(value(&docket, "docket_tasks_perpetuated", &[NOOP]), None);
}

#[tokio::test]
async fn a_struck_run_counts_where_the_strike_stopped_it() {
    let docket = docket().await;
    docket.add(Noop).await.unwrap();
    docket.strike(Strike::task::<Noop>()).await.unwrap();

    within(10, worker(&docket).name("striker").run_until_finished())
        .await
        .unwrap();

    let labels = [NOOP, "docket.where=worker", "docket.worker=striker"];
    assert_eq!(value(&docket, "docket_tasks_stricken", &labels), Some(1.0));
}

#[tokio::test]
async fn a_stale_delivery_counts_as_superseded() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.add(Noop).key("stale").await.unwrap();
    let runs = format!("{}:runs:stale", docket.name());
    let _: i64 = raw.hset(runs, "generation", 99).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let labels = [NOOP, "docket.where=worker"];
    assert_eq!(
        value(&docket, "docket_tasks_superseded", &labels),
        Some(1.0)
    );
}

#[tokio::test]
async fn a_delivery_cancelled_before_its_claim_is_not_counted_again() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.add(Noop).key("late").await.unwrap();
    let runs = format!("{}:runs:late", docket.name());
    let _: i64 = raw.hset(runs, "state", "cancelled").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(value(&docket, "docket_tasks_started", &[NOOP]), None);
    assert_eq!(value(&docket, "docket_tasks_cancelled", &[]), None);
}

#[tokio::test]
async fn the_monitor_counts_the_strikes_in_effect() {
    let docket = docket().await;
    docket.strike(Strike::task::<Noop>()).await.unwrap();
    settles(&docket, "docket_strikes_in_effect", &[NOOP], 1.0).await;

    docket.restore(Strike::task::<Noop>()).await.unwrap();
    settles(&docket, "docket_strikes_in_effect", &[NOOP], 0.0).await;
}

#[tokio::test]
async fn a_heartbeat_reports_the_queue_and_schedule_depths() {
    let docket = docket().await;
    docket
        .add(Noop)
        .after(Duration::from_secs(3600))
        .await
        .unwrap();

    within(
        10,
        worker(&docket).run_until(async {
            settles(&docket, "docket_schedule_depth", &[], 1.0).await;
        }),
    )
    .await
    .unwrap();

    assert_eq!(value(&docket, "docket_queue_depth", &[]), Some(0.0));
}
