//! Helpers for the tests of applications that use docket.
//!
//! The assertions read the docket's snapshot and panic with what they found
//! instead.  "Scheduled" means added and not yet started, now or in the
//! future, the same as in pydocket's `docket.testing`.
//!
//! [`ContextBuilder`] makes the [`Context`] for calling a task's handler
//! straight from a test, without a worker.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::behaviors::PerpetualControl;
use crate::context::{Context, Delivery, Run};
use crate::docket::{Docket, TaskSummary};
use crate::execution::Message;
use crate::task::Task;

/// Makes a [`Context`] for calling a task's handler straight from a test,
/// the way pydocket's tests call a task function with the dependencies they
/// give it.  The run is a first attempt, due now, under a new key, unless
/// the builder says otherwise.
///
/// ```no_run
/// # use docket::{Context, Task};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Task)]
/// # #[task(name = "sync")]
/// # struct Sync { cursor: u32 }
/// # async fn sync(ctx: Context, args: Sync) -> Result<(), std::io::Error> { Ok(()) }
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use docket::testing::ContextBuilder;
///
/// let docket = docket::Docket::connect("tests", "memory://sync").await?;
/// let args = Sync { cursor: 0 };
/// let ctx = ContextBuilder::new(&docket, &args).perpetual().build();
/// sync(ctx.clone(), args).await?;
/// assert!(ctx.perpetual().unwrap().is_cancelled());
/// # Ok(())
/// # }
/// ```
pub struct ContextBuilder {
    docket: Docket,
    function: String,
    args: serde_json::Value,
    key: String,
    attempt: u32,
    when: DateTime<Utc>,
    worker: String,
    behaviors: Vec<Box<dyn Any + Send + Sync>>,
    pub(crate) redelivery_timeout: Duration,
}

impl ContextBuilder {
    /// A context for a run of `T` with `args`.
    ///
    /// # Panics
    ///
    /// When `args` does not convert to JSON.
    #[must_use]
    pub fn new<T: Task>(docket: &Docket, args: &T) -> Self {
        let args = serde_json::to_value(args).expect("task arguments convert to JSON");
        Self::named(docket, T::NAME, args)
    }

    pub(crate) fn named(docket: &Docket, function: &str, args: serde_json::Value) -> Self {
        Self {
            docket: docket.clone(),
            function: function.to_owned(),
            args,
            key: uuid::Uuid::now_v7().to_string(),
            attempt: 1,
            when: Utc::now(),
            worker: "test-worker".to_owned(),
            behaviors: Vec::new(),
            redelivery_timeout: Duration::from_mins(5),
        }
    }

    /// The task's key.
    #[must_use]
    pub fn key(mut self, key: &str) -> Self {
        key.clone_into(&mut self.key);
        self
    }

    /// The attempt number, from 1.
    #[must_use]
    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    /// When the task was due.
    #[must_use]
    pub fn when(mut self, when: DateTime<Utc>) -> Self {
        self.when = when;
        self
    }

    /// The name of the worker running the task.
    #[must_use]
    pub fn worker(mut self, worker: &str) -> Self {
        worker.clone_into(&mut self.worker);
        self
    }

    /// Gives the run a [`PerpetualControl`], as [`Perpetual`](crate::Perpetual)
    /// and [`Cron`](crate::Cron) do, which the test reads back after the
    /// handler returns.
    #[must_use]
    pub fn perpetual(self) -> Self {
        self.behavior(PerpetualControl::new())
    }

    /// Gives the run a value that the handler reads with
    /// [`Context::behavior`], as a behavior's
    /// [`Hooks::context`](crate::behaviors::Hooks::context) would.
    #[must_use]
    pub fn behavior<V: Any + Send + Sync>(mut self, value: V) -> Self {
        self.behaviors.push(Box::new(value));
        self
    }

    /// The context.
    #[must_use]
    pub fn build(self) -> Context {
        let message = Message {
            key: self.key.clone(),
            when: self.when,
            function: self.function.clone(),
            args: self.args.to_string(),
            attempt: self.attempt,
            generation: 1,
            trace: HashMap::new(),
        };
        Context::new(Run {
            docket: self.docket,
            worker: self.worker,
            key: self.key,
            function: self.function,
            attempt: self.attempt,
            when: self.when,
            args: self.args,
            behaviors: self.behaviors,
            delivery: Delivery {
                message_id: "0-1".to_owned(),
                message,
                redelivered: false,
                redelivery_timeout: self.redelivery_timeout,
            },
        })
    }
}

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
