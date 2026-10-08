//! What docket does with a Redis it cannot reach or a URL it cannot use.
//! Nothing listens on port 1, so every connection to it is refused at once.

use std::future::IntoFuture;
use std::time::Duration;

use docket::{Docket, Strike, Worker};
use futures::FutureExt;
use futures::future::BoxFuture;
use rstest::rstest;

use crate::logs::Logs;
use crate::support::{Noop, within};

const UNREACHABLE: &str = "redis://127.0.0.1:1/0";

async fn unreachable() -> Docket {
    Docket::connect("unreachable", UNREACHABLE).await.unwrap()
}

type Call = fn(Docket) -> BoxFuture<'static, docket::Result<()>>;

#[rstest]
#[case::add(|docket: Docket| async move { docket.add(Noop).await.map(drop) }.boxed())]
#[case::replace(|docket: Docket| async move {
    docket.replace(Noop, "k", chrono::Utc::now()).await.map(drop)
}.boxed())]
#[case::add_many(|docket: Docket| async move {
    docket.add_many([docket.call(Noop)]).await.map(drop)
}.boxed())]
#[case::cancel(|docket: Docket| async move { docket.cancel("k").await }.boxed())]
#[case::strike(|docket: Docket| async move { docket.strike(Strike::task::<Noop>()).await }.boxed())]
#[case::snapshot(|docket: Docket| async move { docket.snapshot().await.map(drop) }.boxed())]
#[case::execution(|docket: Docket| async move { docket.execution("k").await.map(drop) }.boxed())]
#[case::workers(|docket: Docket| async move { docket.workers().await.map(drop) }.boxed())]
#[case::task_workers(|docket: Docket| async move {
    docket.task_workers("noop").await.map(drop)
}.boxed())]
#[case::clear(|docket: Docket| async move { docket.clear().await.map(drop) }.boxed())]
#[case::redis(|docket: Docket| async move { docket.redis().await.map(drop) }.boxed())]
#[tokio::test]
async fn every_call_reports_that_redis_is_unreachable(#[case] call: Call) {
    let error = call(unreachable().await).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
}

#[tokio::test]
async fn a_worker_keeps_trying_to_reach_redis_until_it_is_shut_down() {
    let (logs, _guard) = Logs::capture();
    let docket = unreachable().await;
    let shutdown = logs.clone();
    within(
        10,
        Worker::new(docket)
            .reconnection_delay(Duration::from_millis(10))
            .run_until(async move {
                shutdown.wait_for("Redis is unavailable, retrying in").await;
            }),
    )
    .await
    .unwrap();
    assert!(logs.contains("Connection refused"));
}

#[rstest]
#[case::no_scheme("localhost:6379")]
#[case::bad_port("redis://localhost:port/0")]
#[case::bad_cluster_port("redis+cluster://localhost:port")]
#[tokio::test]
async fn a_url_docket_cannot_use_fails_to_connect(#[case] url: &str) {
    assert!(Docket::connect("bad", url).await.is_err());
}

/// A Redis that is not a cluster, from a test run against a plain Redis.
fn standalone_address() -> Option<String> {
    let url = std::env::var("DOCKET_TEST_URL").ok()?;
    Some(url.strip_prefix("redis://")?.split('/').next()?.to_owned())
}

#[tokio::test]
async fn a_cluster_url_for_a_plain_redis_reports_the_mismatch() {
    let Some(address) = standalone_address() else {
        return;
    };
    let docket = Docket::connect("mismatch", format!("redis+cluster://{address}"))
        .await
        .unwrap();
    let error = docket.add(Noop).await.unwrap_err();
    assert!(
        error.to_string().contains("cluster support disabled"),
        "{error}"
    );
}

/// The sentinels of a test run against a Sentinel.
fn sentinel_hosts() -> Option<String> {
    let url = std::env::var("DOCKET_TEST_URL").ok()?;
    Some(
        url.strip_prefix("redis+sentinel://")?
            .split('/')
            .next()?
            .to_owned(),
    )
}

#[rstest]
#[case::unknown_service("{hosts}/nosuchmaster", "Master with given name not found")]
#[case::wrong_password(":wrong@{hosts}/mymaster", "AuthenticationFailed")]
#[case::missing_database("{hosts}/mymaster/99", "DB index is out of range")]
#[tokio::test]
async fn a_sentinel_docket_reports_why_it_cannot_reach_the_master(
    #[case] path: &str,
    #[case] expected: &str,
) {
    let Some(hosts) = sentinel_hosts() else {
        return;
    };
    let url = format!("redis+sentinel://{}", path.replace("{hosts}", &hosts));
    let docket = Docket::connect("sentinel", url).await.unwrap();
    let error = docket.add(Noop).await.unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[tokio::test]
async fn a_worker_behind_an_unreachable_sentinel_keeps_trying() {
    let docket = Docket::connect("sentinel", "redis+sentinel://127.0.0.1:1/mymaster")
        .await
        .unwrap();
    let error = docket.add(Noop).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
    within(
        10,
        Worker::new(docket)
            .reconnection_delay(Duration::from_millis(10))
            .run_until(tokio::time::sleep(Duration::from_millis(100))),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_worker_with_automatic_tasks_shuts_down_while_redis_is_unreachable() {
    let docket = unreachable().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(docket::Perpetual::every(Duration::from_secs(1)).automatic());
    within(
        10,
        Worker::new(docket)
            .reconnection_delay(Duration::from_millis(10))
            .run_until(tokio::time::sleep(Duration::from_millis(100))),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_batch_reports_an_unreachable_cluster() {
    let docket = Docket::connect("unreachable", "redis+cluster://127.0.0.1:1")
        .await
        .unwrap();
    let error = within(10, docket.add_many([docket.call(Noop)]).into_future())
        .await
        .unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
}
