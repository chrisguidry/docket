//! Adding and replacing many tasks at once, and the names a task answers to.

use std::time::Duration;

use docket::{Disposition, Retry, State, Task};
use serde::{Deserialize, Serialize};

use crate::support::{Echo, Noop, docket, docket_through, proxy, within, worker};

// The proxy counts a pipeline by the reads that carry it, so each batch here
// stays small enough to arrive in one read of its 16 KB buffer.

#[tokio::test]
async fn add_many_sends_a_batch_in_chunks_of_the_given_size() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let calls = (0..10).map(|n| docket.call(Noop).key(format!("chunked-{n}")));

    docket.add_many(calls).chunk_size(4).await.unwrap();

    assert_eq!(proxy.batches("EVALSHA"), 3);
}

#[tokio::test]
async fn add_many_sends_a_small_batch_in_one_pipeline_by_default() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let calls = (0..10).map(|n| docket.call(Noop).key(format!("whole-{n}")));

    docket.add_many(calls).await.unwrap();

    assert_eq!(proxy.batches("EVALSHA"), 1);
}

#[tokio::test]
async fn one_pipeline_sends_the_whole_batch_at_once() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let calls = (0..10).map(|n| docket.call(Noop).key(format!("once-{n}")));

    docket.replace_many(calls).one_pipeline().await.unwrap();

    assert_eq!(proxy.batches("EVALSHA"), 1);
}

#[tokio::test]
async fn a_chunk_size_of_zero_is_refused_even_for_an_empty_batch() {
    let docket = docket().await;

    let refused = docket.add_many([]).chunk_size(0).await;

    assert!(matches!(refused, Err(docket::Error::Invalid(_))));
}

#[tokio::test]
async fn replace_many_gives_a_call_without_a_key_a_new_one() {
    let docket = docket().await;

    let executions = docket
        .replace_many([docket.call(Noop), docket.call(Noop)])
        .await
        .unwrap();

    assert_eq!(
        executions
            .iter()
            .map(|execution| execution.disposition().clone())
            .collect::<Vec<_>>(),
        [Disposition::Scheduled, Disposition::Scheduled]
    );
    assert_ne!(executions[0].key(), executions[1].key());
}

/// The task `Echo` was called before it was renamed: the same arguments
/// under its old name.
#[derive(Clone, Debug, Serialize, Deserialize, Task)]
#[task(name = "echo-v1", output = String)]
struct OldEcho {
    text: String,
}

#[tokio::test]
async fn a_task_also_runs_under_its_other_names_with_its_behaviors() {
    let docket = docket().await;
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counted = std::sync::Arc::clone(&attempts);
    docket
        .register(move |ctx, args: Echo| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                match ctx.attempt() {
                    1 => Err(std::io::Error::other("first attempt fails")),
                    _ => Ok(args.text),
                }
            }
        })
        .also_named("echo-v1")
        .with(Retry::attempts(2).delay(Duration::from_millis(10)));
    let old = docket
        .add(OldEcho {
            text: "renamed".into(),
        })
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(old.status().await.unwrap().unwrap().state, State::Completed);
    assert_eq!(old.result().await.unwrap(), "renamed");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(docket.task_names().contains(&"echo-v1".to_owned()));
}
