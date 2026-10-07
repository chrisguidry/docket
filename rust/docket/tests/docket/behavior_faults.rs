//! What the behaviors do when Redis refuses one of their commands.  These
//! tests need a plain Redis behind the fault proxy, so they do nothing on the
//! in-process engine or a cluster.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, Execution, Retry, State};
use rstest::rstest;

use crate::logs::Logs;
use crate::support::proxy::Proxy;
use crate::support::{Noop, docket_through, proxy, within, worker};

/// Registers a task that runs for `duration` under a limit of one at a time.
fn limited(docket: &Docket, duration: Duration) {
    docket
        .register(move |_ctx, _: Noop| async move {
            tokio::time::sleep(duration).await;
            Ok::<_, std::io::Error>(())
        })
        .with(ConcurrencyLimit::new(1));
}

async fn state(execution: &Execution<()>) -> State {
    execution.status().await.unwrap().unwrap().state
}

/// A refused slot fails that attempt, as any failed admission check does, so
/// the task's retry runs it again as its next attempt.
#[tokio::test]
async fn a_refused_slot_fails_the_attempt_and_its_retry_runs_it_again() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&attempts);
    docket
        .register(move |ctx, _: Noop| {
            recorded.lock().unwrap().push(ctx.attempt());
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(ConcurrencyLimit::new(1))
        .with(Retry::attempts(2));
    let execution = docket.add(Noop).await.unwrap();
    proxy.fail_script(include_str!("../../lua/acquire_or_park.lua"), 1);

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(state(&execution).await, State::Completed);
    assert_eq!(*attempts.lock().unwrap(), [2]);
}

#[tokio::test]
async fn a_refused_release_still_completes_the_task() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    limited(&docket, Duration::ZERO);
    let execution = docket.add(Noop).await.unwrap();
    proxy.fail_script(include_str!("../../lua/release_and_wake.lua"), 1);

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(state(&execution).await, State::Completed);
    assert!(logs.contains("releasing a concurrency slot failed"));
}

#[tokio::test]
async fn a_refused_renewal_still_completes_the_task() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    limited(&docket, Duration::from_millis(300));
    let execution = docket.add(Noop).await.unwrap();
    // The renewal is the only plain ZADD besides the worker's heartbeat,
    // which shrugs off failures too.
    proxy.fail("ZADD", 1000);

    within(
        10,
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(400))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(state(&execution).await, State::Completed);
    assert!(logs.contains("Concurrency lease renewal failed for"));
}

/// Runs two tasks under a limit of one, so that one of them parks, with
/// `fault` set on the proxy just before the worker starts.
#[rstest]
#[case::safeguard_refused(|proxy: &Proxy| proxy.fail_script(include_str!("../../lua/schedule.lua"), 1))]
#[case::parked_state_refused(|proxy: &Proxy| proxy.fail("HGET", 1))]
#[tokio::test]
async fn a_parked_task_runs_when_its_safeguard_fails(#[case] fault: fn(&Proxy)) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    limited(&docket, Duration::from_millis(200));
    let first = docket.add(Noop).await.unwrap();
    let second = docket.add(Noop).await.unwrap();
    fault(&proxy);

    within(10, worker(&docket).concurrency(2).run_until_finished())
        .await
        .unwrap();

    assert_eq!(state(&first).await, State::Completed);
    assert_eq!(state(&second).await, State::Completed);
    assert!(logs.contains("scheduling a concurrency safeguard failed"));
}
