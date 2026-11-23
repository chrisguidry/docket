//! What docket does with a task whose data in Redis is not UTF-8 text, as a
//! task that pydocket added is: pydocket pickles its arguments.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::{Docket, Perpetual, State};
use redis::AsyncCommands;

use crate::logs::Logs;
use crate::support::telemetry::value;
use crate::support::{Echo, Noop, docket, shared_url, within, worker};

/// The start of a pickle, which is not UTF-8.
const PICKLED: &[u8] = &[0x80, 0x05, 0x95, 0x0b, 0x00, b')', 0x94, b'.'];

/// The Redis key of one of the docket's own keys, such as `runs:<key>`.
fn docket_key(docket: &Docket, suffix: &str) -> String {
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

fn assert_no_disruption(docket: &Docket, logs: &Logs) {
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

/// A runs hash with a field name that is not UTF-8 makes the reply to the
/// claim undecodable.  The task fails, its message is acknowledged, and the
/// worker runs the next task.
#[tokio::test]
async fn a_task_whose_claim_reply_cannot_be_decoded_fails_and_the_worker_goes_on() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let normal = echo_with_a_normal_task(&docket).await;
    docket
        .add(Echo::new("garbled"))
        .key("garbled")
        .await
        .unwrap();
    let runs = docket_key(&docket, "runs:garbled");
    let mut redis = docket.redis().await.unwrap();
    let () = redis.hset(&runs, PICKLED, "value").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(within(10, normal.result()).await.unwrap(), "normal");
    let state: String = redis.hget(&runs, "state").await.unwrap();
    assert_eq!(state, "failed");
    let error: String = redis.hget(&runs, "error").await.unwrap();
    assert!(error.contains("Cannot convert from UTF-8"), "{error}");
    let pending: i64 = redis.xlen(docket_key(&docket, "stream")).await.unwrap();
    assert_eq!(pending, 0);
    assert!(logs.contains("\"garbled\""));
    assert!(logs.contains("Cannot convert from UTF-8"));
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
