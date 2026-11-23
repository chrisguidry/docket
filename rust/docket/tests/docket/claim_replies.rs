//! What a worker does with a claim reply it cannot interpret, and with a
//! connection that closes partway through a reply.  These tests need a plain
//! Redis behind the fault proxy, so they do nothing on the in-process engine
//! or a cluster.

use std::time::Duration;

use docket::{Docket, State, Worker};

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
    arm(proxy);
    (docket, execution)
}

#[rstest::rstest]
#[case::not_a_list(b"$1\r\n\xff\r\n")]
#[case::a_status_that_is_not_text(b"*3\r\n$1\r\n\xff\r\n*0\r\n*0\r\n")]
#[tokio::test]
async fn a_claim_whose_status_cannot_be_read_is_redelivered(#[case] reply: &[u8]) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let (docket, execution) = echo_once(&proxy, |proxy| {
        proxy.answer_script(CLAIM, 1, Some(reply.to_vec()));
    })
    .await;

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "again");
    assert!(logs.contains("Could not interpret the claim of task"));
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), None);
    assert_no_disruption(&docket, &logs);
}

#[tokio::test]
async fn a_claim_whose_runs_hash_cannot_be_read_fails_the_run() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let (docket, execution) = echo_once(&proxy, |proxy| {
        proxy.answer_script(CLAIM, 1, Some(b"*3\r\n$2\r\nOK\r\n:5\r\n*0\r\n".to_vec()));
    })
    .await;

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    let error = status.error.unwrap();
    assert!(
        error.starts_with("could not interpret a reply from Redis: the runs hash"),
        "{error}"
    );
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), Some(1.0));
    assert!(logs.contains("could not interpret a reply from Redis"));
    assert_no_disruption(&docket, &logs);
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
