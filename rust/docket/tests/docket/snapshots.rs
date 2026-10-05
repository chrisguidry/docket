//! Looking at what a docket holds: snapshots, single executions, and
//! clearing it out.

use std::time::Duration;

use chrono::Utc;
use docket::Docket;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;

use crate::support::{Echo, Noop, docket, url};

/// A plain connection to the Redis under the tests, when they run against a
/// plain Redis, for writing what docket itself never writes.  A docket's keys
/// there start with its name.
pub async fn raw() -> Option<MultiplexedConnection> {
    let url = std::env::var("DOCKET_TEST_URL").ok()?;
    if !url.starts_with("redis://") {
        return None;
    }
    let client = redis::Client::open(url).unwrap();
    Some(client.get_multiplexed_async_connection().await.unwrap())
}

#[tokio::test]
async fn a_snapshot_skips_what_is_not_a_task() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let name = docket.name();
    docket.add(Echo::new("real")).key("real").await.unwrap();
    let _: String = raw
        .xadd(format!("{name}:stream"), "*", &[("junk", "1")])
        .await
        .unwrap();
    let _: i64 = raw.zadd(format!("{name}:queue"), "ghost", 1).await.unwrap();

    let snapshot = docket.snapshot().await.unwrap();

    assert_eq!(snapshot.total_tasks, 3);
    let keys: Vec<&str> = snapshot
        .future
        .iter()
        .map(|task| task.key.as_str())
        .collect();
    assert_eq!(keys, ["real"]);
}

#[tokio::test]
async fn clear_counts_stream_entries_that_are_not_tasks() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let _: String = raw
        .xadd(format!("{}:stream", docket.name()), "*", &[("junk", "1")])
        .await
        .unwrap();
    assert_eq!(docket.clear().await.unwrap(), 1);
    assert_eq!(docket.snapshot().await.unwrap().total_tasks, 0);
}

#[tokio::test]
async fn execution_finds_a_task_added_earlier() {
    let docket = docket().await;
    let when = Utc::now() + chrono::Duration::seconds(60);
    docket
        .add(Echo::new("later"))
        .key("k")
        .at(when)
        .await
        .unwrap();

    let execution = docket.execution("k").await.unwrap().unwrap();

    assert_eq!((execution.key(), execution.function()), ("k", "echo"));
    assert_eq!(execution.when().timestamp_millis(), when.timestamp_millis());
    assert!(format!("{execution:?}").contains(r#"key: "k""#));
    assert!(format!("{docket:?}").contains(docket.name()));
}

#[tokio::test]
async fn execution_knows_nothing_of_a_key_never_added() {
    let docket = docket().await;
    assert!(docket.execution("nobody").await.unwrap().is_none());
}

#[tokio::test]
async fn execution_is_due_now_when_its_run_has_no_time() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let _: i64 = raw
        .hset(format!("{}:runs:bare", docket.name()), "function", "noop")
        .await
        .unwrap();
    let before = Utc::now();

    let execution = docket.execution("bare").await.unwrap().unwrap();

    assert!(execution.when() >= before);
}

#[tokio::test]
async fn a_docket_that_keeps_nothing_forgets_cleared_tasks() {
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url())
        .execution_ttl(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(docket.clear().await.unwrap(), 1);
    assert_eq!(execution.status().await.unwrap(), None);
}
