//! The `docket_parked_depth` gauge: how many tasks wait behind concurrency
//! limits, which the queue depth leaves out.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, State};
use tokio::sync::Notify;

use redis::AsyncCommands;

use crate::logs::Logs;
use crate::snapshots::raw;
use crate::support::telemetry::value;
use crate::support::{Echo, Noop, docket_with, within, worker};

const PARKED: &str = "docket_parked_depth";

/// A docket whose workers report the depths often.
async fn docket() -> Docket {
    docket_with(|builder| builder.heartbeat_interval(Duration::from_millis(50))).await
}

/// Waits until a heartbeat reports `expected` parked tasks.
async fn settles(docket: &Docket, expected: f64) {
    within(10, async {
        while value(docket, PARKED, &[]) != Some(expected) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// Holds each run, one at a time, until the test lets it go.
fn register_held(docket: &Docket) -> Arc<Notify> {
    let release = Arc::new(Notify::new());
    let released = Arc::clone(&release);
    docket
        .register(move |_ctx, args: Echo| {
            let released = Arc::clone(&released);
            async move {
                released.notified().await;
                Ok::<_, std::io::Error>(args.text)
            }
        })
        .with(ConcurrencyLimit::new(1));
    release
}

#[tokio::test]
async fn the_parked_depth_follows_the_backlog_until_it_drains() {
    let docket = docket().await;
    let release = register_held(&docket);
    for text in ["first", "second", "third"] {
        docket.add(Echo::new(text)).await.unwrap();
    }

    let run = tokio::spawn(worker(&docket).concurrency(3).run_forever());
    settles(&docket, 2.0).await;

    release.notify_one();
    settles(&docket, 1.0).await;
    release.notify_one();
    settles(&docket, 0.0).await;
    release.notify_one();
    run.abort();
}

#[tokio::test]
async fn the_parked_depth_falls_when_a_parked_task_is_cancelled() {
    let docket = docket().await;
    let release = register_held(&docket);
    docket.add(Echo::new("first")).await.unwrap();
    let run = tokio::spawn(worker(&docket).concurrency(2).run_forever());
    // The holder has its slot once the docket has no task left to start.
    within(10, async {
        while docket.snapshot().await.unwrap().running.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let parked = docket.add(Echo::new("second")).await.unwrap();
    settles(&docket, 1.0).await;

    // The holder still has its slot, so only the cancel can lower the depth.
    docket.cancel(parked.key()).await.unwrap();
    settles(&docket, 0.0).await;

    release.notify_one();
    run.abort();
    assert_eq!(
        parked.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[tokio::test]
async fn the_parked_depth_falls_when_a_dead_holders_slot_is_scavenged() {
    let docket = docket().await;
    let hang = Arc::new(AtomicBool::new(true));
    let hanging = Arc::clone(&hang);
    docket
        .register(move |_ctx, _: Noop| {
            let hang = hanging.swap(false, Ordering::SeqCst);
            async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                Ok::<_, std::io::Error>(())
            }
        })
        .with(ConcurrencyLimit::new(1));
    docket.add(Noop).await.unwrap();
    let waiter = docket.add(Noop).await.unwrap();

    let dying = tokio::spawn(
        worker(&docket)
            .concurrency(2)
            .redelivery_timeout(Duration::from_millis(300))
            .run_forever(),
    );
    settles(&docket, 1.0).await;
    dying.abort();

    within(
        30,
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(300))
            .run_until(settles(&docket, 0.0)),
    )
    .await
    .unwrap();

    assert_ne!(
        waiter.status().await.unwrap().unwrap().state,
        State::Scheduled
    );
}

#[tokio::test]
async fn a_heartbeat_fails_when_it_cannot_read_the_parked_total() {
    let Some(mut raw) = raw().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let total = format!("{}:indexes:concurrency-waiters:total", docket.name());
    let _: i64 = raw.hset(&total, "not", "a number").await.unwrap();

    let logged = logs.clone();
    within(
        20,
        worker(&docket).run_until(async move {
            logged.wait_for("Error sending worker heartbeat").await;
        }),
    )
    .await
    .unwrap();

    let _: i64 = raw.del(&total).await.unwrap();
    assert_eq!(value(&docket, "docket_queue_depth", &[]), Some(0.0));
}
