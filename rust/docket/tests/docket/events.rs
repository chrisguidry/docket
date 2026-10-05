//! Following a run's events, and waiting on its result through them.

use std::time::Duration;

use docket::{Docket, Event, Task};
use futures::StreamExt;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use rstest::rstest;
use serde::{Deserialize, Serialize};

use crate::snapshots::raw;
use crate::support::{Echo, Noop, docket, docket_through, proxy, url, within, worker};

#[rstest]
#[case::not_text(b"\xff")]
#[case::not_json(b"not json")]
#[case::unknown_type(br#"{"type": "gossip"}"#)]
#[tokio::test]
async fn an_event_that_is_not_one_arrives_as_an_error(#[case] payload: &[u8]) {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let mut events = execution.subscribe().await.unwrap().skip(2);

    let _: i64 = raw
        .publish(
            format!("{}:state:{}", docket.name(), execution.key()),
            payload,
        )
        .await
        .unwrap();

    assert!(within(10, events.next()).await.unwrap().is_err());
}

/// Waits until someone follows the run of `key`.
async fn followed(raw: &mut MultiplexedConnection, docket: &Docket, key: &str) {
    let channel = format!("{}:state:{key}", docket.name());
    within(10, async {
        loop {
            let counts: Vec<(String, i64)> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(&channel)
                .query_async(raw)
                .await
                .unwrap();
            if counts[0].1 > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn result_reports_an_event_it_cannot_read() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let key = execution.key().to_owned();
    let waiting = tokio::spawn(async move { execution.result().await });
    followed(&mut raw, &docket, &key).await;

    let _: i64 = raw
        .publish(format!("{}:state:{key}", docket.name()), "not json")
        .await
        .unwrap();

    let error = within(10, waiting).await.unwrap().unwrap_err();
    assert!(matches!(error, docket::Error::Json(_)), "{error}");
}

#[tokio::test]
async fn result_reports_that_redis_went_away_while_it_waited() {
    let Some(proxy) = proxy().await else { return };
    let Some(mut raw) = raw().await else { return };
    let docket = docket_through(&proxy).await;
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let key = execution.key().to_owned();
    let waiting = tokio::spawn(async move { execution.result().await });
    followed(&mut raw, &docket, &key).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    proxy.cut();

    let error = within(10, waiting).await.unwrap().unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
}

#[tokio::test]
async fn result_of_a_failure_without_its_error_says_it_failed() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let _: i64 = raw
        .hset(
            format!("{}:runs:{}", docket.name(), execution.key()),
            "state",
            "failed",
        )
        .await
        .unwrap();

    let error = within(10, execution.result()).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("task {} failed: the task failed", execution.key())
    );
}

#[tokio::test]
async fn result_reports_a_stored_result_it_cannot_read() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("hi")).await.unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    let _: () = raw
        .set(
            format!("{}:results:{}", docket.name(), execution.key()),
            "garbage",
        )
        .await
        .unwrap();

    let error = within(10, execution.result()).await.unwrap_err();

    assert!(matches!(error, docket::Error::Json(_)), "{error}");
}

#[tokio::test]
async fn result_of_a_task_without_output_is_unit() {
    let docket = docket().await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let execution = docket.add(Noop).await.unwrap();
    let waiting = tokio::spawn(async move { execution.result().await });

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    within(10, waiting).await.unwrap().unwrap();
}

/// A task that takes the name of `Echo` but returns a number.
#[derive(Clone, Debug, Serialize, Deserialize, Task)]
#[task(name = "echo", output = u64)]
struct Counted {
    text: String,
}

#[tokio::test]
async fn result_reports_an_output_of_another_type() {
    let docket = docket().await;
    docket.register(|_ctx, _: Counted| async { Ok::<_, std::io::Error>(7) });
    let execution = docket.add(Echo::new("seven")).await.unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let error = within(10, execution.result()).await.unwrap_err();

    assert!(matches!(error, docket::Error::Json(_)), "{error}");
}

#[tokio::test]
async fn following_a_forgotten_run_starts_with_its_progress() {
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url())
        .execution_ttl(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let execution = docket.add(Noop).await.unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let mut events = execution.subscribe().await.unwrap();

    let first = within(10, events.next()).await.unwrap().unwrap();
    assert!(matches!(first, Event::Progress(_)), "{first:?}");
}
