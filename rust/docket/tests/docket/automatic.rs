//! How workers schedule automatic perpetual tasks: when they start, and
//! again every so often in case one was lost.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::{Docket, Perpetual, State, Strike};
use redis::AsyncCommands;

use crate::logs::Logs;
use crate::snapshots::raw;
use crate::support::proxy::Proxy;
use crate::support::{Noop, docket, docket_through, proxy, within, worker};

/// Registers `Noop` as an automatic task that runs once a minute, and
/// counts its runs.
fn register_automatic(docket: &Docket) -> Arc<AtomicU32> {
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            counted.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, std::io::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_mins(1)).automatic());
    runs
}

/// Waits until the automatic task has run once and is scheduled again.
async fn ran_once(docket: &Docket, runs: &AtomicU32) {
    within(10, async {
        loop {
            let state = match docket.execution("noop").await.unwrap() {
                Some(execution) => execution.status().await.unwrap().map(|status| status.state),
                None => None,
            };
            if state == Some(State::Scheduled) && runs.load(Ordering::SeqCst) == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn scheduling_again_leaves_a_running_or_scheduled_task_alone() {
    let docket = docket().await;
    let runs = register_automatic(&docket);
    let run = tokio::spawn(
        worker(&docket)
            .automatic_tasks_interval(Duration::from_millis(10))
            .run_forever(),
    );
    ran_once(&docket, &runs).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    run.abort();

    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_struck_automatic_task_is_not_scheduled() {
    let docket = docket().await;
    let runs = register_automatic(&docket);
    docket.strike(Strike::task::<Noop>()).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn only_the_worker_holding_the_lock_schedules_automatic_tasks() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let runs = register_automatic(&docket);
    let _: () = raw
        .set_ex(format!("{}:perpetual:lock", docket.name()), "someone", 10)
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[rstest::rstest]
#[case::taking_the_lock(|proxy: &Proxy| proxy.fail("SET", 1), "injected failure for SET")]
#[case::scheduling(
    |proxy: &Proxy| proxy.fail_script(include_str!("../../lua/schedule.lua"), 1),
    "injected failure for EVALSHA"
)]
#[tokio::test]
async fn a_worker_reconnects_when_redis_refuses_its_automatic_tasks(
    #[case] arm: fn(&Proxy),
    #[case] logged: &str,
) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let runs = register_automatic(&docket);
    arm(&proxy);
    let run = tokio::spawn(
        worker(&docket)
            .reconnection_delay(Duration::from_millis(10))
            .run_forever(),
    );

    ran_once(&docket, &runs).await;
    run.abort();

    assert!(logs.contains(logged));
}

#[tokio::test]
async fn a_worker_logs_when_scheduling_again_fails() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let runs = register_automatic(&docket);
    let run = tokio::spawn(
        worker(&docket)
            .automatic_tasks_interval(Duration::from_millis(10))
            .run_forever(),
    );
    ran_once(&docket, &runs).await;

    proxy.fail("HGETALL", 1);

    logs.wait_for("Error re-seeding automatic perpetual tasks")
        .await;
    run.abort();
}
