//! Adding and replacing many tasks at once, and the names a task answers to.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{Disposition, Docket, Perpetual, Retry, State, Task};
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
async fn a_batch_that_fails_partway_keeps_the_chunks_that_went_in() {
    let Some(proxy) = proxy().await else { return };
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), proxy.url())
        .response_timeout(Duration::from_millis(300))
        .connect()
        .await
        .unwrap();
    proxy.silence_after("EVALSHA", 4);
    let calls = (0..10).map(|n| docket.call(Noop).key(format!("partway-{n}")));

    let executions = within(10, docket.add_many(calls).chunk_size(4).into_future())
        .await
        .unwrap();

    let scheduled: Vec<bool> = executions
        .iter()
        .map(|execution| *execution.disposition() == Disposition::Scheduled)
        .collect();
    assert_eq!(scheduled, [[true; 4].as_slice(), &[false; 6]].concat());
    assert!(
        executions[4..]
            .iter()
            .all(|execution| matches!(execution.disposition(), Disposition::Failed(_)))
    );
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

#[tokio::test]
async fn a_perpetual_task_under_another_name_runs_next_under_its_own() {
    let docket = docket().await;
    let names = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&names);
    docket
        .register(move |ctx, args: Echo| {
            recorded.lock().unwrap().push(ctx.function().to_owned());
            async move { Ok::<_, std::io::Error>(args.text) }
        })
        .also_named("echo-v1")
        .with(Perpetual::every(Duration::from_millis(10)));
    let old = OldEcho {
        text: "renamed".into(),
    };
    docket.add(old).key("renamed").await.unwrap();

    let limits = HashMap::from([("renamed".to_owned(), 2)]);
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    assert_eq!(*names.lock().unwrap(), ["echo-v1", "echo"]);
}
