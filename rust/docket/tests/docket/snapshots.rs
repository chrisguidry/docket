//! Looking at what a docket holds: snapshots, single executions, and
//! clearing it out.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use docket::{Docket, Event, State};
use futures::StreamExt;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use rstest::rstest;
use tokio::sync::Notify;

use crate::support::{Echo, Noop, docket, url, within, worker};

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

#[rstest]
#[case::queued(Duration::ZERO)]
#[case::scheduled(Duration::from_secs(60))]
#[tokio::test]
async fn clear_cancels_each_task_it_removes(#[case] delay: Duration) {
    let docket = docket().await;
    let execution = docket.add(Noop).key("cleared").after(delay).await.unwrap();

    docket.clear().await.unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[rstest]
#[case::queued(Duration::ZERO)]
#[case::scheduled(Duration::from_secs(60))]
#[tokio::test]
async fn a_cleared_key_can_be_added_again(#[case] delay: Duration) {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket
        .add(Echo::new("first"))
        .key("cleared")
        .after(delay)
        .await
        .unwrap();
    docket.clear().await.unwrap();

    let second = docket
        .add(Echo::new("second"))
        .key("cleared")
        .await
        .unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, second.result()).await.unwrap(), "second");
}

/// No worker holds a task that has not started, so the clear itself has to
/// publish the cancelled state, the way a cancel does, as JSON that any key
/// leaves valid.
#[rstest]
#[case::queued(Duration::ZERO, "plain")]
#[case::scheduled(Duration::from_secs(60), "plain")]
#[case::a_key_with_a_control_character(Duration::ZERO, "back\u{8}space")]
#[tokio::test]
async fn clear_tells_a_follower_that_the_task_is_cancelled(
    #[case] delay: Duration,
    #[case] key: &str,
) {
    let docket = docket().await;
    let execution = docket.add(Noop).key(key).after(delay).await.unwrap();
    let mut events = execution.subscribe().await.unwrap();

    docket.clear().await.unwrap();

    within(10, async {
        while let Some(event) = events.next().await {
            if let Event::State(event) = event.unwrap()
                && event.state == State::Cancelled
            {
                return;
            }
        }
    })
    .await;
}

/// An immediate task has no parked data, so a clear deletes nothing at the
/// key that parked data would have, even when that key is the stream's.
#[tokio::test]
async fn clear_keeps_the_stream_when_a_task_key_names_it() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.add(Noop).key("stream").await.unwrap();

    docket.clear().await.unwrap();

    let exists: bool = raw
        .exists(format!("{}:stream", docket.name()))
        .await
        .unwrap();
    assert!(exists);
}

/// A docket that keeps no records still keeps a running task's record until
/// the task ends, so the task shows as running through a clear.
#[tokio::test]
async fn clear_leaves_a_running_task_alone() {
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url())
        .execution_ttl(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (starting, released) = (Arc::clone(&started), Arc::clone(&release));
    docket.register(move |_ctx, _: Noop| {
        let (starting, released) = (Arc::clone(&starting), Arc::clone(&released));
        async move {
            starting.notify_one();
            released.notified().await;
            Ok::<_, std::io::Error>(())
        }
    });
    docket.add(Noop).key("held").await.unwrap();
    let run = tokio::spawn(worker(&docket).run_until_finished());
    within(10, started.notified()).await;

    docket.clear().await.unwrap();
    let during = match docket.execution("held").await.unwrap() {
        Some(execution) => execution.status().await.unwrap().map(|status| status.state),
        None => None,
    };
    release.notify_one();
    within(10, run).await.unwrap().unwrap();

    assert_eq!(during, Some(State::Running));
}
