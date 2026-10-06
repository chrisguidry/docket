use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use docket::{Disposition, State, Task};
use serde::{Deserialize, Serialize};

use crate::support::{Echo, docket, within, worker};

#[tokio::test]
async fn a_task_added_now_runs_and_returns_its_output() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("hi")).await.unwrap();
    assert_eq!(execution.disposition(), &Disposition::Scheduled);

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "hi");
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_future_task_waits_for_its_time() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket
        .add(Echo::new("later"))
        .after(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Scheduled
    );

    let started = Utc::now();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(Utc::now() - started >= chrono::Duration::milliseconds(250));
    assert_eq!(execution.result().await.unwrap(), "later");
}

#[tokio::test]
async fn adding_a_key_twice_schedules_it_once() {
    let docket = docket().await;
    let first = docket
        .add(Echo::new("a"))
        .key("same")
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let second = docket.add(Echo::new("b")).key("same").await.unwrap();
    assert_eq!(first.disposition(), &Disposition::Scheduled);
    assert_eq!(second.disposition(), &Disposition::AlreadyScheduled);
    assert_eq!(docket.snapshot().await.unwrap().future.len(), 1);
}

#[tokio::test]
async fn replace_swaps_the_scheduled_task() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket
        .add(Echo::new("old"))
        .key("k")
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let replaced = docket
        .replace(Echo::new("new"), "k", Utc::now())
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(replaced.result().await.unwrap(), "new");
}

#[tokio::test]
async fn cancel_removes_a_scheduled_task() {
    let docket = docket().await;
    let execution = docket
        .add(Echo::new("x"))
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    docket.cancel(execution.key()).await.unwrap();
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
    assert!(matches!(
        execution.result().await,
        Err(docket::Error::TaskCancelled { .. })
    ));
    assert_eq!(docket.snapshot().await.unwrap().future, []);
}

#[tokio::test]
async fn a_strike_during_a_batch_leaves_the_other_dispositions_in_place() {
    let docket = docket().await;
    docket
        .add(Echo::new("taken"))
        .key("taken")
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let batch = docket.add_many([
        docket.call(Echo::new("taken")).key("taken"),
        docket.call(Echo::new("free")).key("free"),
    ]);
    let strike = docket.strike(docket::Strike::task::<Echo>().field("text").eq("taken"));
    let (executions, struck) = tokio::join!(batch, strike);
    struck.unwrap();

    let executions = executions.unwrap();
    assert_eq!(executions[1].disposition(), &Disposition::Scheduled);
}

/// A task whose arguments have keys that JSON cannot hold, once it has any.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Task)]
#[task(name = "bytes-keyed")]
struct BytesKeyed {
    by_bytes: HashMap<Vec<u8>, u8>,
}

impl BytesKeyed {
    fn unencodable() -> Self {
        Self {
            by_bytes: HashMap::from([(vec![1], 1)]),
        }
    }
}

#[tokio::test]
async fn arguments_that_are_not_json_are_refused() {
    let docket = docket().await;
    let added = docket.add(BytesKeyed::unencodable()).await;
    let replaced = docket
        .replace(BytesKeyed::unencodable(), "k", Utc::now())
        .await;
    let batched = docket
        .add_many([docket.call(BytesKeyed::unencodable())])
        .await;
    assert!(matches!(added, Err(docket::Error::Json(_))));
    assert!(matches!(replaced, Err(docket::Error::Json(_))));
    assert!(matches!(batched, Err(docket::Error::Json(_))));
}

#[tokio::test]
async fn arguments_of_the_same_task_that_are_json_go_in() {
    let docket = docket().await;
    let added = docket.add(BytesKeyed::default()).await.unwrap();
    let replaced = docket
        .replace(BytesKeyed::default(), "k", Utc::now())
        .await
        .unwrap();
    assert_eq!(added.disposition(), &Disposition::Scheduled);
    assert_eq!(replaced.disposition(), &Disposition::Scheduled);
}
