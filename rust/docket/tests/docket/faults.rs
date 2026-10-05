//! What docket does when Redis refuses a command or goes away.  These tests
//! need a plain Redis behind the fault proxy, so they do nothing on the
//! in-process engine or a cluster.

use std::sync::Arc;
use std::time::Duration;

use docket::{Disposition, Docket, State, Strike};
use futures::FutureExt;
use futures::future::BoxFuture;
use rstest::rstest;
use tokio::sync::Notify;

use crate::support::proxy::Proxy;
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

    // The worker runs until the test has its second result, because an empty
    // docket after the reconnect must not end the run early.
    let done = Arc::new(Notify::new());
    let finished = Arc::clone(&done);
    let run = tokio::spawn(
        worker(&docket)
            .reconnection_delay(Duration::from_millis(50))
            .run_until(async move { finished.notified().await }),
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
    done.notify_one();
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

type Faulty = fn(Docket, Arc<Proxy>) -> BoxFuture<'static, docket::Result<()>>;

#[rstest]
#[case::cancel_script(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail_script(include_str!("../../lua/cancel_task.lua"), 1);
    docket.cancel("k").await
}.boxed())]
#[case::cancel_expiry(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("EXPIRE", 1);
    docket.cancel("k").await
}.boxed())]
#[case::cancel_publish(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("PUBLISH", 1);
    docket.cancel("k").await
}.boxed())]
#[case::strike(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("XADD", 1);
    docket.strike(Strike::task::<Noop>()).await
}.boxed())]
#[case::add_many_load(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("SCRIPT", 1);
    docket.add_many([docket.call(Noop)]).await.map(drop)
}.boxed())]
#[case::snapshot_group(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("XGROUP", 1);
    docket.snapshot().await.map(drop)
}.boxed())]
#[case::snapshot_read(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("XPENDING", 1);
    docket.snapshot().await.map(drop)
}.boxed())]
#[case::snapshot_parked(|docket: Docket, proxy: Arc<Proxy>| async move {
    docket.add(Noop).after(Duration::from_secs(60)).await?;
    proxy.fail("HGETALL", 1);
    docket.snapshot().await.map(drop)
}.boxed())]
#[case::snapshot_workers(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("ZREMRANGEBYSCORE", 1);
    docket.snapshot().await.map(drop)
}.boxed())]
#[case::workers(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("ZRANGE", 1);
    docket.workers().await.map(drop)
}.boxed())]
#[case::clear_read(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("XLEN", 1);
    docket.clear().await.map(drop)
}.boxed())]
#[case::clear_write(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("XTRIM", 1);
    docket.clear().await.map(drop)
}.boxed())]
#[case::execution(|docket: Docket, proxy: Arc<Proxy>| async move {
    proxy.fail("HGETALL", 1);
    docket.execution("k").await.map(drop)
}.boxed())]
#[case::status(|docket: Docket, proxy: Arc<Proxy>| async move {
    let execution = docket.add(Noop).await?;
    proxy.fail("HGETALL", 1);
    execution.status().await.map(drop)
}.boxed())]
#[case::progress(|docket: Docket, proxy: Arc<Proxy>| async move {
    let execution = docket.add(Noop).await?;
    proxy.fail("HGETALL", 1);
    execution.progress().await.map(drop)
}.boxed())]
#[case::subscribe(|docket: Docket, proxy: Arc<Proxy>| async move {
    let execution = docket.add(Noop).await?;
    proxy.fail("HGETALL", 1);
    execution.subscribe().await.map(drop)
}.boxed())]
#[case::subscribe_after_redis_goes_away(|docket: Docket, proxy: Arc<Proxy>| async move {
    let execution = docket.add(Noop).await?;
    proxy.cut();
    execution.result().await.map(drop)
}.boxed())]
#[case::result(|docket: Docket, proxy: Arc<Proxy>| async move {
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("stored")).await?;
    worker(&docket).run_until_finished().await?;
    proxy.fail("GET", 1);
    execution.result().await.map(drop)
}.boxed())]
#[tokio::test]
async fn a_producer_reports_each_command_redis_refuses(#[case] call: Faulty) {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let error = within(10, call(docket, Arc::new(proxy))).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
}

#[tokio::test]
async fn listing_workers_reports_a_refused_task_list() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let run = tokio::spawn(worker(&docket).run_forever());
    within(10, async {
        while docket.workers().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    proxy.fail("SMEMBERS", 1);
    let error = docket.workers().await.unwrap_err();
    run.abort();
    assert!(error.to_string().contains("injected failure"), "{error}");
}

#[tokio::test]
async fn a_batch_reports_the_add_that_redis_refused() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    proxy.fail_script(include_str!("../../lua/schedule.lua"), 1);
    let executions = docket
        .add_many([docket.call(Noop), docket.call(Noop)])
        .await
        .unwrap();
    assert!(
        matches!(executions[0].disposition(), Disposition::Failed(error) if error.contains("injected failure"))
    );
    assert_eq!(executions[1].disposition(), &Disposition::Scheduled);
}

#[tokio::test]
async fn a_docket_that_keeps_nothing_reports_a_refused_delete() {
    let Some(proxy) = proxy().await else { return };
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), proxy.url())
        .execution_ttl(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    docket.cancel("k").await.unwrap();
    proxy.fail("DEL", 1);
    let error = docket.cancel("k").await.unwrap_err();
    assert!(error.to_string().contains("injected failure"), "{error}");
}

#[tokio::test]
async fn a_waiting_result_subscribes_again_when_its_subscription_drops() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket
        .add(Echo::new("later"))
        .after(Duration::from_millis(500))
        .await
        .unwrap();
    let waiting = tokio::spawn(async move { execution.result().await });
    within(10, async {
        while proxy.count("SUBSCRIBE") == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let reads_before = proxy.count("HGETALL");
    proxy.drop_subscribers();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, waiting).await.unwrap().unwrap(), "later");
    // A wait that spun on the ended subscription would read the run state
    // over and over until the task finished.
    assert!(
        proxy.count("HGETALL") - reads_before < 20,
        "{}",
        proxy.count("HGETALL") - reads_before
    );
}
