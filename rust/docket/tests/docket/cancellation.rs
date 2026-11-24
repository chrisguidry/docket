//! Cancelling tasks that a worker holds: parked on a concurrency limit, or
//! running while Redis comes and goes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, Execution, State};
use redis::AsyncCommands;
use redis::streams::StreamRangeReply;
use tokio::sync::Notify;

use crate::logs::Logs;
use crate::support::proxy::Proxy;
use crate::support::{Echo, Noop, docket, docket_through, proxy, within, worker};
use crate::undecodable::{PICKLED, docket_key};

/// Whether the docket has a safeguard scheduled for a parked task.
async fn safeguard_scheduled(docket: &Docket) -> bool {
    let snapshot = docket.snapshot().await.unwrap();
    snapshot
        .future
        .iter()
        .any(|task| task.key.starts_with("__safeguard__:"))
}

/// Records each run, and holds it until the test lets it go.
fn register_held(docket: &Docket) -> (Arc<Mutex<Vec<String>>>, Arc<Notify>) {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let (recorded, released) = (Arc::clone(&ran), Arc::clone(&release));
    docket
        .register(move |_ctx, args: Echo| {
            recorded.lock().unwrap().push(args.text.clone());
            let released = Arc::clone(&released);
            async move {
                released.notified().await;
                Ok::<_, std::io::Error>(args.text)
            }
        })
        .with(ConcurrencyLimit::new(1));
    (ran, release)
}

/// Adds a task that runs and one that parks behind it, and waits until the
/// second is parked.
async fn park_one(docket: &Docket) -> Execution<String> {
    docket.add(Echo::new("first")).await.unwrap();
    let parked = docket.add(Echo::new("second")).await.unwrap();
    within(10, async {
        while !safeguard_scheduled(docket).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    parked
}

#[tokio::test]
async fn cancelling_a_parked_task_takes_it_out_of_line() {
    let docket = docket().await;
    let (ran, release) = register_held(&docket);
    let run = tokio::spawn(worker(&docket).concurrency(2).run_until_finished());
    let parked = park_one(&docket).await;

    docket.cancel(parked.key()).await.unwrap();
    within(10, async {
        while safeguard_scheduled(&docket).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    release.notify_one();
    within(10, run).await.unwrap().unwrap();

    assert_eq!(*ran.lock().unwrap(), ["first"]);
    assert_eq!(
        parked.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

/// A task that pydocket added has pickled arguments in its runs hash, which
/// the worker reads to find the task's place in line.
#[tokio::test]
async fn cancelling_a_parked_task_with_pickled_arguments_takes_it_out_of_line() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let (_, release) = register_held(&docket);
    let run = tokio::spawn(worker(&docket).concurrency(2).run_until_finished());
    let parked = park_one(&docket).await;
    let runs = docket_key(&docket, &format!("runs:{}", parked.key()));
    let mut redis = docket.redis().await.unwrap();
    let () = redis.hset(&runs, "kwargs", PICKLED).await.unwrap();
    let waiters: String = redis.hget(&runs, "waiter_stream").await.unwrap();
    let entry: String = redis.hget(&runs, "waiter_entry_id").await.unwrap();

    docket.cancel(parked.key()).await.unwrap();
    within(10, async {
        while safeguard_scheduled(&docket).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    release.notify_one();
    within(10, run).await.unwrap().unwrap();

    let line: StreamRangeReply = redis.xrange(&waiters, &entry, &entry).await.unwrap();
    assert_eq!(line.ids.len(), 0);
    assert!(!logs.contains("cleaning up a cancelled waiter failed"));
}

/// A parked task's runs hash with a value outside its arguments that is not
/// UTF-8 text cannot be read, so the worker logs the cleanup it skipped.
#[tokio::test]
async fn a_worker_logs_a_parked_task_whose_runs_hash_cannot_be_read() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let (_, release) = register_held(&docket);
    let run = tokio::spawn(worker(&docket).concurrency(2).run_until_finished());
    let parked = park_one(&docket).await;
    let runs = docket_key(&docket, &format!("runs:{}", parked.key()));
    let mut redis = docket.redis().await.unwrap();
    let () = redis.hset(&runs, "worker", PICKLED).await.unwrap();

    docket.cancel(parked.key()).await.unwrap();
    within(10, logs.wait_for("cleaning up a cancelled waiter failed")).await;
    release.notify_one();
    within(10, run).await.unwrap().unwrap();

    assert!(logs.contains("field is not UTF-8 text"));
}

#[rstest::rstest]
#[case::reading_the_run(|proxy: &Proxy| proxy.fail("HGETALL", 1))]
#[case::leaving_the_line(
    |proxy: &Proxy| proxy.fail_script(include_str!("../../lua/cancel_cleanup.lua"), 1)
)]
#[tokio::test]
async fn a_worker_logs_a_parked_task_it_could_not_take_out_of_line(#[case] arm: fn(&Proxy)) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let (_, release) = register_held(&docket);
    let logged = logs.clone();
    let run = tokio::spawn(worker(&docket).concurrency(2).run_until(async move {
        logged
            .wait_for("cleaning up a cancelled waiter failed")
            .await;
    }));
    let parked = park_one(&docket).await;

    arm(&proxy);
    docket.cancel(parked.key()).await.unwrap();
    logs.wait_for("cleaning up a cancelled waiter failed").await;
    release.notify_one();

    within(20, run).await.unwrap().unwrap();
    assert!(logs.contains("injected failure"));
}

#[tokio::test]
async fn the_cancel_listener_listens_again_after_redis_drops_it() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        signal.notify_one();
        std::future::pending::<Result<(), std::io::Error>>()
    });
    let execution = docket.add(Noop).await.unwrap();
    let run = tokio::spawn(
        worker(&docket)
            .reconnection_delay(Duration::from_millis(50))
            .run_until_finished(),
    );
    within(10, started.notified()).await;

    // The listener waits a second after Redis drops it, and another after
    // its next attempt fails while Redis is still away.
    proxy.cut();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    proxy.heal();
    within(20, async {
        while !run.is_finished() {
            let _ = docket.cancel(execution.key()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    within(10, run).await.unwrap().unwrap();
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}
