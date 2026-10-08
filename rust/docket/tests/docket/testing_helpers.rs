use std::time::Duration;

use docket::Docket;
use docket::testing::{
    assert_args_scheduled, assert_no_tasks, assert_not_scheduled, assert_scheduled,
    assert_scheduled_count, assert_task_count, assert_task_not_scheduled, assert_task_scheduled,
};

use crate::support::{Echo, Noop, docket};

/// A docket with an echo of "hello" under the key "greeting", due later, and
/// a noop due now.
async fn scheduled() -> Docket {
    let docket = docket().await;
    docket
        .add(Echo::new("hello"))
        .key("greeting")
        .after(Duration::from_secs(3600))
        .await
        .unwrap();
    docket.add(Noop).await.unwrap();
    docket
}

#[tokio::test]
async fn a_scheduled_task_passes_the_assertions() {
    let docket = scheduled().await;
    assert_task_scheduled(&docket, "echo", None).await;
    assert_task_scheduled(&docket, "echo", "greeting").await;
    assert_task_not_scheduled(&docket, "echo", "farewell").await;
    assert_task_not_scheduled(&docket, "missing", None).await;
    assert_args_scheduled(&docket, &Echo::new("hello")).await;
    assert_task_count(&docket, "echo", 1).await;
    assert_task_count(&docket, None, 2).await;
}

#[tokio::test]
async fn a_scheduled_task_passes_the_typed_assertions() {
    let docket = scheduled().await;
    assert_scheduled::<Echo>(&docket, None).await;
    assert_scheduled::<Echo>(&docket, "greeting").await;
    assert_not_scheduled::<Echo>(&docket, "farewell").await;
    assert_scheduled_count::<Echo>(&docket, 1).await;
    assert_scheduled_count::<Noop>(&docket, 1).await;
}

#[tokio::test]
async fn an_empty_docket_has_no_tasks() {
    let docket = docket().await;
    assert_no_tasks(&docket).await;
}

#[tokio::test]
#[should_panic(
    expected = "no echo task is scheduled with key Some(\"farewell\"); scheduled:\n  - "
)]
async fn a_missing_key_fails_the_scheduled_assertion() {
    let docket = scheduled().await;
    assert_task_scheduled(&docket, "echo", "farewell").await;
}

#[tokio::test]
#[should_panic(
    expected = "no echo task is scheduled with key None; scheduled:\nno tasks are scheduled"
)]
async fn an_empty_docket_fails_the_scheduled_assertion() {
    let docket = docket().await;
    assert_task_scheduled(&docket, "echo", None).await;
}

#[tokio::test]
#[should_panic(expected = "a echo task is scheduled with key Some(\"greeting\")")]
async fn a_scheduled_task_fails_the_not_scheduled_assertion() {
    let docket = scheduled().await;
    assert_task_not_scheduled(&docket, "echo", "greeting").await;
}

#[tokio::test]
#[should_panic(expected = "no echo task is scheduled with Echo { text: \"goodbye\" }")]
async fn other_arguments_fail_the_arguments_assertion() {
    let docket = scheduled().await;
    assert_args_scheduled(&docket, &Echo::new("goodbye")).await;
}

#[tokio::test]
#[should_panic(expected = "expected 3 scheduled tasks, found 2")]
async fn a_wrong_count_fails_the_count_assertion() {
    let docket = scheduled().await;
    assert_task_count(&docket, None, 3).await;
}

#[tokio::test]
#[should_panic(expected = "expected 0 scheduled tasks, found 2")]
async fn a_scheduled_task_fails_the_no_tasks_assertion() {
    let docket = scheduled().await;
    assert_no_tasks(&docket).await;
}

#[tokio::test]
#[should_panic(expected = "no echo task is scheduled with key Some(\"farewell\")")]
async fn a_missing_key_fails_the_typed_scheduled_assertion() {
    let docket = scheduled().await;
    assert_scheduled::<Echo>(&docket, "farewell").await;
}

#[tokio::test]
#[should_panic(expected = "a noop task is scheduled with key None")]
async fn a_scheduled_task_fails_the_typed_not_scheduled_assertion() {
    let docket = scheduled().await;
    assert_not_scheduled::<Noop>(&docket, None).await;
}

#[tokio::test]
#[should_panic(expected = "expected 2 scheduled tasks, found 1")]
async fn a_wrong_count_fails_the_typed_count_assertion() {
    let docket = scheduled().await;
    assert_scheduled_count::<Echo>(&docket, 2).await;
}
