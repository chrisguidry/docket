//! Assertions for the tests of applications that use docket.  Each one
//! reads the docket's snapshot and panics with what it found instead.
//!
//! "Scheduled" means added and not yet started, now or in the future, the
//! same as in pydocket's `docket.testing`.

use std::fmt::Debug;

use crate::docket::{Docket, TaskSummary};
use crate::task::Task;

async fn scheduled(docket: &Docket) -> Vec<TaskSummary> {
    docket
        .snapshot()
        .await
        .expect("the docket's snapshot can be read")
        .future
}

fn describe(tasks: &[TaskSummary]) -> String {
    if tasks.is_empty() {
        return "no tasks are scheduled".to_owned();
    }
    tasks
        .iter()
        .map(|task| format!("  - {}({}) key={}", task.function, task.args, task.key))
        .collect::<Vec<_>>()
        .join("\n")
}

fn matches(task: &TaskSummary, function: &str, key: Option<&str>) -> bool {
    task.function == function && key.is_none_or(|key| task.key == key)
}

/// Asserts that a task named `function` is scheduled, with `key` when one
/// is given.
///
/// # Panics
///
/// When no such task is scheduled, or the snapshot cannot be read.
pub async fn assert_task_scheduled<'a>(
    docket: &Docket,
    function: &str,
    key: impl Into<Option<&'a str>>,
) {
    let key = key.into();
    let tasks = scheduled(docket).await;
    assert!(
        tasks.iter().any(|task| matches(task, function, key)),
        "no {function} task is scheduled with key {key:?}; scheduled:\n{}",
        describe(&tasks)
    );
}

/// Asserts that no task named `function` is scheduled, with `key` when one
/// is given.
///
/// # Panics
///
/// When such a task is scheduled, or the snapshot cannot be read.
pub async fn assert_task_not_scheduled<'a>(
    docket: &Docket,
    function: &str,
    key: impl Into<Option<&'a str>>,
) {
    let key = key.into();
    let tasks = scheduled(docket).await;
    assert!(
        !tasks.iter().any(|task| matches(task, function, key)),
        "a {function} task is scheduled with key {key:?}; scheduled:\n{}",
        describe(&tasks)
    );
}

/// Asserts that a task with exactly these arguments is scheduled.
///
/// # Panics
///
/// When no such task is scheduled, or the snapshot cannot be read.
pub async fn assert_args_scheduled<T: Task + PartialEq + Debug>(docket: &Docket, args: &T) {
    let tasks = scheduled(docket).await;
    let found = tasks.iter().any(|task| {
        task.function == T::NAME
            && serde_json::from_value::<T>(task.args.clone()).is_ok_and(|found| &found == args)
    });
    assert!(
        found,
        "no {} task is scheduled with {args:?}; scheduled:\n{}",
        T::NAME,
        describe(&tasks)
    );
}

/// Asserts that exactly `count` tasks are scheduled, of the task named
/// `function` when one is given.
///
/// # Panics
///
/// When the count differs, or the snapshot cannot be read.
pub async fn assert_task_count<'a>(
    docket: &Docket,
    function: impl Into<Option<&'a str>>,
    count: usize,
) {
    let function = function.into();
    let tasks = scheduled(docket).await;
    let found = tasks
        .iter()
        .filter(|task| function.is_none_or(|f| task.function == f))
        .count();
    assert_eq!(
        found,
        count,
        "expected {count} scheduled tasks, found {found}; scheduled:\n{}",
        describe(&tasks)
    );
}

/// Asserts that no task is scheduled.
///
/// # Panics
///
/// When a task is scheduled, or the snapshot cannot be read.
pub async fn assert_no_tasks(docket: &Docket) {
    assert_task_count(docket, None, 0).await;
}
