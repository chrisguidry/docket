//! What a worker does with a claim reply it cannot interpret, and with a
//! connection that closes partway through a reply.  These tests need a plain
//! Redis behind the fault proxy, so they do nothing on the in-process engine
//! or a cluster.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::{Docket, Event, State, Worker};
use futures::StreamExt;

use crate::logs::Logs;
use crate::support::proxy::Proxy;
use crate::support::telemetry::value;
use crate::support::{Echo, docket_through, proxy, within, worker};
use crate::undecodable::assert_no_disruption;

/// A worker that takes over a lost delivery and reconnects quickly.
fn hasty(docket: &Docket) -> Worker {
    worker(docket)
        .redelivery_timeout(Duration::from_millis(200))
        .reconnection_delay(Duration::from_millis(50))
}

const CLAIM: &str = include_str!("../../lua/claim.lua");

/// Registers `Echo` and adds one task, after the proxy is armed so that the
/// add itself goes through.
async fn echo_once(proxy: &Proxy, arm: impl FnOnce(&Proxy)) -> (Docket, docket::Execution<String>) {
    let docket = docket_through(proxy).await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("again")).await.unwrap();
    load_claim(&docket).await;
    arm(proxy);
    (docket, execution)
}

/// Loads the claim script into Redis, so that a forwarded `EVALSHA` of it
/// runs instead of failing with `NOSCRIPT`.
async fn load_claim(docket: &Docket) {
    let mut redis = docket.redis().await.unwrap();
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(CLAIM)
        .query_async(&mut redis)
        .await
        .unwrap();
}

/// A reply that is not a list, and longer than a log line shows.
fn a_long_text() -> Vec<u8> {
    let mut reply = b"$300\r\n".to_vec();
    reply.extend([b'x'; 300]);
    reply.extend(b"\r\n");
    reply
}

#[rstest::rstest]
#[case::not_a_list(a_long_text(), "…")]
#[case::a_status_that_is_not_text(b"*3\r\n$1\r\n\xff\r\n*0\r\n*0\r\n".to_vec(), "status")]
#[tokio::test]
async fn a_claim_whose_status_cannot_be_read_is_redelivered(
    #[case] reply: Vec<u8>,
    #[case] logged: &str,
) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let (docket, execution) = echo_once(&proxy, |proxy| {
        proxy.answer_script(CLAIM, 1, Some(reply));
    })
    .await;

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "again");
    assert!(logs.contains("Could not read the claim of task"));
    assert!(logs.contains(logged));
    assert!(!logs.contains(&"x".repeat(150)));
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), None);
    assert_no_disruption(&docket, &logs);
}

/// The claim runs in Redis, but its reply cannot be read.  The redelivery
/// sweep hands the delivery back, and the task runs once.
#[tokio::test]
async fn a_claim_that_ran_but_whose_reply_cannot_be_read_runs_the_task_once() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket.register(move |_ctx, args: Echo| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { Ok::<_, std::io::Error>(args.text) }
    });
    let execution = docket.add(Echo::new("once")).await.unwrap();
    load_claim(&docket).await;
    let mut events = execution.subscribe().await.unwrap();
    proxy.replace_script_reply(CLAIM, b"$1\r\n\xff\r\n", Duration::ZERO);

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "once");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(value(&docket, "docket_tasks_redelivered", &[]), Some(1.0));
    // Each claim that ran in Redis published a running state.
    let mut running = 0;
    within(10, async {
        while let Some(event) = events.next().await {
            match event.unwrap() {
                Event::State(event) if event.state == State::Running => running += 1,
                Event::State(event) if event.state == State::Completed => break,
                _ => {}
            }
        }
    })
    .await;
    assert_eq!(running, 2);
}

/// A producer's command whose reply is cut short fails, and the next command
/// opens a new connection rather than failing on the closed one.
#[tokio::test]
async fn the_command_after_a_reply_is_cut_short_succeeds() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let execution = docket.add(Echo::new("kept")).await.unwrap();
    proxy.cut_reply("HGETALL");

    let error = docket.execution(execution.key()).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");

    let found = docket.execution(execution.key()).await.unwrap().unwrap();
    assert_eq!(found.key(), execution.key());
}

/// An OK claim whose runs hash is a number.
const UNREADABLE_CLAIM: &[u8] = b"*3\r\n$2\r\nOK\r\n:5\r\n*0\r\n";

#[tokio::test]
async fn a_claim_whose_runs_hash_cannot_be_read_fails_the_run() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let (docket, execution) = echo_once(&proxy, |proxy| {
        proxy.answer_script(CLAIM, 1, Some(UNREADABLE_CLAIM.to_vec()));
    })
    .await;

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    let error = status.error.unwrap();
    assert!(
        error.starts_with("could not read the runs hash of a claim: "),
        "{error}"
    );
    assert_eq!(value(&docket, "docket_tasks_started", &[]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_completed", &[]), Some(1.0));
    assert_eq!(value(&docket, "docket_tasks_running", &[]), Some(0.0));
    assert!(logs.contains("could not read the runs hash of a claim"));
    assert_no_disruption(&docket, &logs);
}

/// A cancel that reaches the worker while it waits for its claim's reply
/// leaves the task cancelled, whether or not the claim ran in Redis first.
#[rstest::rstest]
#[case::before_the_claim_runs(
    |proxy: &Proxy| proxy.answer_script_after(CLAIM, UNREADABLE_CLAIM, Duration::from_secs(1)),
    State::Queued
)]
#[case::after_the_claim_runs(
    |proxy: &Proxy| proxy.replace_script_reply(CLAIM, UNREADABLE_CLAIM, Duration::from_secs(1)),
    State::Running
)]
#[tokio::test]
async fn a_cancel_during_a_claim_whose_runs_hash_cannot_be_read_wins(
    #[case] arm: fn(&Proxy),
    #[case] claimed: State,
) {
    let Some(proxy) = proxy().await else { return };
    let (docket, execution) = echo_once(&proxy, |_| {}).await;
    let sent = proxy.script_count(CLAIM);
    arm(&proxy);
    // A second connection, because the delayed reply holds up the worker's.
    let canceller = Docket::connect(docket.name(), proxy.url()).await.unwrap();
    let seen = canceller.execution(execution.key()).await.unwrap().unwrap();

    // The default redelivery timeout keeps the sweep away from the delivery
    // while its claim's reply waits.
    let run = tokio::spawn(worker(&docket).run_until_finished());
    within(10, async {
        while proxy.script_count(CLAIM) == sent
            || seen.status().await.unwrap().unwrap().state != claimed
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    canceller.cancel(execution.key()).await.unwrap();
    within(20, run).await.unwrap().unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Cancelled);
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), None);
}

/// redis-rs reports a connection that closes partway through a reply as a
/// parse error.  The worker reconnects, and the task runs.
#[rstest::rstest]
#[case::reading(|proxy: &Proxy| proxy.cut_reply("XREADGROUP"))]
#[case::claiming(|proxy: &Proxy| proxy.cut_script_reply(CLAIM))]
#[tokio::test]
async fn a_worker_reconnects_after_a_reply_is_cut_short(#[case] arm: fn(&Proxy)) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let (docket, execution) = echo_once(&proxy, arm).await;

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "again");
    assert!(logs.contains("Redis is unavailable"));
    let disruptions = value(&docket, "docket_redis_disruptions", &[]).unwrap();
    assert!(disruptions >= 1.0, "{disruptions}");
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), None);
}
