//! What docket does when Redis refuses a command or goes away.  These tests
//! need a plain Redis behind the fault proxy, so they do nothing on the
//! in-process engine or a cluster.

use std::time::Duration;

use docket::{State, Strike};

use crate::support::{Echo, Noop, docket_through, proxy, within, worker};

#[tokio::test]
async fn producers_report_a_refused_command() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    proxy.fail("EVALSHA", 1);
    let error = docket.add(Noop).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
    assert!(error.to_string().contains("injected failure"), "{error}");
}

#[tokio::test]
async fn producers_reconnect_after_redis_comes_back() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    docket.add(Noop).await.unwrap();

    proxy.cut();
    assert!(docket.add(Noop).await.is_err());
    assert!(docket.snapshot().await.is_err());
    assert!(docket.cancel("anything").await.is_err());
    assert!(docket.strike(Strike::task::<Noop>()).await.is_err());
    proxy.heal();

    within(10, async {
        while docket.add(Noop).await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn a_worker_reconnects_and_finishes_the_tasks() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let first = docket.add(Echo::new("before")).await.unwrap();

    let run = tokio::spawn(
        worker(&docket)
            .reconnection_delay(Duration::from_millis(50))
            .run_until_finished(),
    );
    within(10, first.result()).await.unwrap();
    proxy.cut();
    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.heal();
    let second = within(10, async {
        loop {
            if let Ok(execution) = docket.add(Echo::new("after")).await {
                return execution;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(within(10, second.result()).await.unwrap(), "after");
    within(10, run).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_task_whose_ending_fails_runs_again_after_its_lease() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let execution = docket.add(Noop).await.unwrap();
    proxy.fail_script(include_str!("../../lua/terminal.lua"), 1);

    within(
        20,
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(200))
            .reconnection_delay(Duration::from_millis(50))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
