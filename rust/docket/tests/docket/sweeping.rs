//! Taking over the deliveries of a worker that died, a few at a time.

use std::sync::Arc;
use std::time::Duration;

use docket::{Docket, Execution, State, Worker};
use tokio::sync::Semaphore;

use crate::logs::Logs;
use crate::support::{Noop, docket, docket_through, proxy, shared_url, within, worker};

const TIMEOUT: Duration = Duration::from_millis(200);

/// Leaves three tasks with deliveries that a dead worker took and no longer
/// renews.
async fn abandon_three(docket: &Docket, url: &str) -> Vec<Execution<()>> {
    let doomed = Docket::connect(docket.name(), url).await.unwrap();
    let started = Arc::new(Semaphore::new(0));
    let signal = Arc::clone(&started);
    doomed.register(move |_ctx, _: Noop| {
        signal.add_permits(1);
        std::future::pending::<Result<(), std::io::Error>>()
    });
    let mut executions = Vec::new();
    for _ in 0..3 {
        executions.push(docket.add(Noop).await.unwrap());
    }
    let run = tokio::spawn(
        worker(&doomed)
            .name("doomed")
            .redelivery_timeout(TIMEOUT)
            .run_forever(),
    );
    within(10, started.acquire_many(3)).await.unwrap().forget();
    run.abort();
    // Past the redelivery timeout, every delivery is free to take over.
    tokio::time::sleep(TIMEOUT * 2).await;
    executions
}

/// A worker that takes over one delivery at a time.
fn survivor(docket: &Docket) -> Worker {
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    worker(docket)
        .name("survivor")
        .concurrency(1)
        .redelivery_timeout(TIMEOUT)
        .reconnection_delay(Duration::from_millis(50))
}

#[tokio::test]
async fn a_sweep_takes_over_lost_deliveries_one_batch_at_a_time() {
    let docket = docket().await;
    let executions = abandon_three(&docket, &shared_url(&docket)).await;

    within(20, survivor(&docket).run_until_finished())
        .await
        .unwrap();

    for execution in executions {
        let status = execution.status().await.unwrap().unwrap();
        assert_eq!(
            (status.state, status.worker.as_deref()),
            (State::Completed, Some("survivor"))
        );
    }
}

#[tokio::test]
async fn a_sweep_starts_over_when_redis_refuses_to_extend_its_lease_mid_walk() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let upstream = std::env::var("DOCKET_TEST_URL").unwrap();
    let executions = abandon_three(&docket, &upstream).await;
    proxy.fail_script(include_str!("../../lua/refresh_lease.lua"), 1);

    within(20, survivor(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains("injected failure for EVALSHA"));
    for execution in executions {
        let status = execution.status().await.unwrap().unwrap();
        assert_eq!(status.state, State::Completed);
    }
}

#[tokio::test]
async fn a_worker_reconnects_when_redis_refuses_to_extend_its_sweep_lease() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    proxy.fail_script(include_str!("../../lua/refresh_lease.lua"), 1);
    let logged = logs.clone();

    within(
        20,
        // The sweep's lease outlives a read, so the worker often finds its
        // own lease still held and extends it.
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(400))
            .reconnection_delay(Duration::from_millis(10))
            .run_until(async move { logged.wait_for("injected failure for EVALSHA").await }),
    )
    .await
    .unwrap();
}
