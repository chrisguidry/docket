use std::collections::HashMap;
use std::time::Duration;

use docket::{Disposition, State, Task, Timestamp};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::snapshots::raw;
use crate::support::{Echo, Noop, docket, within, worker};

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

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    // The worker records when it started the task, so a slow step in the
    // test itself cannot move either side of the comparison.
    let started_at = execution.status().await.unwrap().unwrap().started_at;
    assert!(started_at.unwrap() >= execution.when());
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
        .replace(Echo::new("new"), "k", Timestamp::now())
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
        .replace(BytesKeyed::unencodable(), "k", Timestamp::now())
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
        .replace(BytesKeyed::default(), "k", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(added.disposition(), &Disposition::Scheduled);
    assert_eq!(replaced.disposition(), &Disposition::Scheduled);
}

#[tokio::test]
async fn one_scheduler_pass_moves_every_due_task() {
    // One stream_due_tasks.lua call moves at most 1000 due tasks, so 1500
    // take two calls.  With an hour between scheduler ticks, only the first
    // pass can move them.
    let docket = docket().await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let soon = Timestamp::now() + Duration::from_millis(100);
    let due = (0..1500).map(|n| docket.call(Noop).key(format!("due-{n}")).at(soon));
    docket.add_many(due).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let worker = worker(&docket).scheduling_resolution(Duration::from_secs(3600));
    within(30, worker.run_until_finished()).await.unwrap();

    let last = docket.execution("due-1499").await.unwrap().unwrap();
    assert_eq!(
        last.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_run_without_parked_data_fails() {
    // A scheduled run whose parked data is gone can never start, so the
    // scheduler fails it: a caller waiting on its result gets the reason,
    // and the key accepts a new task.
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let lost = docket
        .add(Echo::new("lost"))
        .key("lost")
        .after(Duration::from_millis(100))
        .await
        .unwrap();
    let _: i64 = raw.del(format!("{}:lost", docket.name())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let failure = within(10, lost.result()).await.unwrap_err();
    assert!(failure.to_string().contains("parked data"));
    assert!(matches!(failure, docket::Error::TaskFailed { .. }));
    let again = docket.add(Echo::new("again")).key("lost").await.unwrap();
    assert_eq!(again.disposition(), &Disposition::Scheduled);
}
