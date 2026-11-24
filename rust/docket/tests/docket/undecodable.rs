//! What docket does with a task whose data in Redis is not UTF-8 text, as a
//! task that pydocket added is: pydocket pickles its arguments.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::{Docket, Event, Perpetual, State};
use futures::StreamExt;
use redis::AsyncCommands;

use crate::logs::Logs;
use crate::support::telemetry::value;
use crate::support::{Echo, Noop, docket, shared_url, within, worker};

/// The start of a pickle, which is not UTF-8.
pub const PICKLED: &[u8] = &[0x80, 0x05, 0x95, 0x0b, 0x00, b')', 0x94, b'.'];

/// The Redis key of one of the docket's own keys, such as `runs:<key>`.
pub fn docket_key(docket: &Docket, suffix: &str) -> String {
    if shared_url(docket).starts_with("redis+cluster://") {
        format!("{{{}}}:{suffix}", docket.name())
    } else {
        format!("{}:{suffix}", docket.name())
    }
}

/// Registers `Echo`, and adds one task that runs normally.
async fn echo_with_a_normal_task(docket: &Docket) -> docket::Execution<String> {
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket.add(Echo::new("normal")).await.unwrap()
}

pub fn assert_no_disruption(docket: &Docket, logs: &Logs) {
    assert!(!logs.contains("Redis is unavailable"));
    assert_eq!(value(docket, "docket_redis_disruptions", &[]), None);
}

#[tokio::test]
async fn a_task_runs_when_its_runs_hash_holds_bytes_that_are_not_text() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("hello")).key("kept").await.unwrap();
    let mut redis = docket.redis().await.unwrap();
    let () = redis
        .hset(docket_key(&docket, "runs:kept"), "kwargs", PICKLED)
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, execution.result()).await.unwrap(), "hello");
    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Completed);
    let snapshot = docket.execution("kept").await.unwrap().unwrap();
    assert_eq!(snapshot.key(), "kept");
    assert_no_disruption(&docket, &logs);
}

/// A task that pydocket added, with pickled arguments in its message and its
/// runs hash, fails on its arguments, and the worker runs the next task.
#[tokio::test]
async fn a_task_with_pickled_arguments_fails_and_the_worker_goes_on() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let normal = echo_with_a_normal_task(&docket).await;
    let pickled = docket
        .add(Echo::new("unused"))
        .key("pickled")
        .after(Duration::from_millis(100))
        .await
        .unwrap();
    let mut redis = docket.redis().await.unwrap();
    let () = redis
        .hset(docket_key(&docket, "pickled"), "args", PICKLED)
        .await
        .unwrap();
    for field in ["args", "kwargs"] {
        let () = redis
            .hset(docket_key(&docket, "runs:pickled"), field, PICKLED)
            .await
            .unwrap();
    }

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, normal.result()).await.unwrap(), "normal");
    let status = pickled.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert!(
        status.error.as_deref().unwrap().contains("expected value"),
        "{status:?}"
    );
    let error = within(10, pickled.result()).await.unwrap_err();
    assert!(error.to_string().contains("task pickled failed"), "{error}");
    assert_no_disruption(&docket, &logs);
}

/// A failed run whose runs hash has a field name that is not UTF-8 reads as
/// failed through every public read.
#[tokio::test]
async fn a_failed_run_with_a_field_name_that_is_not_text_reads_as_failed() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move {
        Err::<String, _>(std::io::Error::other(format!("{} broke", args.text)))
    });
    let garbled = docket
        .add(Echo::new("garbled"))
        .key("garbled")
        .await
        .unwrap();
    let mut redis = docket.redis().await.unwrap();
    let () = redis
        .hset(docket_key(&docket, "runs:garbled"), PICKLED, "value")
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let status = garbled.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert_eq!(status.error.as_deref(), Some("garbled broke"));
    let error = within(10, garbled.result()).await.unwrap_err();
    assert_eq!(error.to_string(), "task garbled failed: garbled broke");
    let found = docket.execution("garbled").await.unwrap().unwrap();
    assert_eq!(found.status().await.unwrap().unwrap().state, State::Failed);
    assert_no_disruption(&docket, &logs);
}

/// A run cancelled before its claim, whose runs hash has a field name that is
/// not UTF-8, stays cancelled.
#[tokio::test]
async fn a_cancelled_run_with_a_field_name_that_is_not_text_stays_cancelled() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let normal = echo_with_a_normal_task(&docket).await;
    let cancelled = docket.add(Echo::new("late")).key("late").await.unwrap();
    let runs = docket_key(&docket, "runs:late");
    let mut redis = docket.redis().await.unwrap();
    let () = redis.hset(&runs, "state", "cancelled").await.unwrap();
    let () = redis.hset(&runs, PICKLED, "value").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, normal.result()).await.unwrap(), "normal");
    let status = cancelled.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Cancelled);
    let pending: i64 = redis.xlen(docket_key(&docket, "stream")).await.unwrap();
    assert_eq!(pending, 0);
    assert_no_disruption(&docket, &logs);
}

/// A stale delivery, whose runs hash has a newer generation and a field name
/// that is not UTF-8, ends as superseded and publishes nothing.
#[tokio::test]
async fn a_superseded_run_with_a_field_name_that_is_not_text_publishes_nothing() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let normal = echo_with_a_normal_task(&docket).await;
    let stale = docket.add(Echo::new("stale")).key("stale").await.unwrap();
    let runs = docket_key(&docket, "runs:stale");
    let mut redis = docket.redis().await.unwrap();
    let () = redis.hset(&runs, "generation", 99).await.unwrap();
    let () = redis.hset(&runs, PICKLED, "value").await.unwrap();
    let before = stale.status().await.unwrap().unwrap().state;
    let mut events = stale.subscribe().await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, normal.result()).await.unwrap(), "normal");
    // The subscription repeats the current state, and the worker publishes
    // no other.
    let mut states = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(300), events.next()).await
    {
        if let Event::State(event) = event.unwrap() {
            states.push(event.state);
        }
    }
    assert_ne!(states.len(), 0);
    assert!(states.iter().all(|state| *state == before), "{states:?}");
    assert_eq!(stale.status().await.unwrap().unwrap().state, before);
    let superseded = value(&docket, "docket_tasks_superseded", &["docket.where=worker"]);
    assert_eq!(superseded, Some(1.0));
    assert_no_disruption(&docket, &logs);
}

/// A run whose state is not UTF-8 text cannot be read, and every public read
/// says so at once rather than waiting for a state it cannot see.
#[tokio::test]
async fn a_run_whose_state_is_not_text_reads_as_an_error() {
    let docket = docket().await;
    let execution = docket
        .add(Echo::new("garbled"))
        .key("garbled")
        .await
        .unwrap();
    let mut redis = docket.redis().await.unwrap();
    let () = redis
        .hset(docket_key(&docket, "runs:garbled"), "state", PICKLED)
        .await
        .unwrap();

    let unreadable =
        "a run's record in Redis cannot be read: its \"state\" field is not UTF-8 text";
    let error = execution.status().await.unwrap_err();
    assert_eq!(error.to_string(), unreadable);
    assert!(!error.is_redis_unavailable());
    let error = within(5, execution.result()).await.unwrap_err();
    assert_eq!(error.to_string(), unreadable);
    let error = docket.execution("garbled").await.unwrap_err();
    assert_eq!(error.to_string(), unreadable);
}

/// A runs hash whose `error` field, left by an earlier run, is not UTF-8 text
/// cannot be read on its claim, so the run fails without running.
#[tokio::test]
async fn a_claim_whose_runs_hash_has_a_field_that_is_not_text_fails_the_run() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let normal = echo_with_a_normal_task(&docket).await;
    let garbled = docket
        .add(Echo::new("garbled"))
        .key("garbled")
        .await
        .unwrap();
    let mut redis = docket.redis().await.unwrap();
    let () = redis
        .hset(docket_key(&docket, "runs:garbled"), "error", PICKLED)
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, normal.result()).await.unwrap(), "normal");
    let status = garbled.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    let error = status.error.unwrap();
    assert!(
        error.contains("its \"error\" field is not UTF-8 text"),
        "{error}"
    );
    assert_eq!(value(&docket, "docket_tasks_failed", &[]), Some(1.0));
    assert_no_disruption(&docket, &logs);
}

#[tokio::test]
async fn an_automatic_task_is_scheduled_when_its_runs_hash_holds_bytes_that_are_not_text() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_mins(1)).automatic());
    let mut redis = docket.redis().await.unwrap();
    let runs_key = docket_key(&docket, "runs:noop");
    let () = redis.hset(&runs_key, "state", "completed").await.unwrap();
    let () = redis.hset(&runs_key, "kwargs", PICKLED).await.unwrap();

    let run = tokio::spawn(worker(&docket).run_forever());
    within(10, async {
        while runs.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    run.abort();

    assert_no_disruption(&docket, &logs);
}
