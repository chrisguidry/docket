use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::docket::Docket;
use crate::execution::Progress;

/// What a task's handler and its behaviors know about the run in progress.
#[derive(Clone)]
pub struct Context {
    inner: Arc<Inner>,
}

struct Inner {
    docket: Docket,
    worker: String,
    key: String,
    function: String,
    attempt: u32,
    when: DateTime<Utc>,
    args: serde_json::Value,
    progress: Progress,
    behaviors: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    delivery: Delivery,
}

/// How the task reached this worker, for behaviors inside docket that act on
/// the delivery itself, such as a concurrency limit parking it.
#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    pub message_id: String,
    pub message: crate::execution::Message,
    pub redelivered: bool,
    pub redelivery_timeout: std::time::Duration,
}

pub(crate) struct Run {
    pub docket: Docket,
    pub worker: String,
    pub key: String,
    pub function: String,
    pub attempt: u32,
    pub when: DateTime<Utc>,
    pub args: serde_json::Value,
    pub behaviors: Vec<Box<dyn Any + Send + Sync>>,
    pub delivery: Delivery,
}

impl Context {
    pub(crate) fn new(run: Run) -> Self {
        let progress = Progress::new(run.docket.clone(), run.key.clone());
        let behaviors = run
            .behaviors
            .into_iter()
            .map(|value| ((*value).type_id(), value))
            .collect();
        Self {
            inner: Arc::new(Inner {
                docket: run.docket,
                worker: run.worker,
                key: run.key,
                function: run.function,
                attempt: run.attempt,
                when: run.when,
                args: run.args,
                progress,
                behaviors,
                delivery: run.delivery,
            }),
        }
    }

    /// The docket the task belongs to, for scheduling more tasks.
    #[must_use]
    pub fn docket(&self) -> &Docket {
        &self.inner.docket
    }

    /// The name of the worker running the task.
    #[must_use]
    pub fn worker(&self) -> &str {
        &self.inner.worker
    }

    /// The task's key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.inner.key
    }

    /// The task's name.
    #[must_use]
    pub fn function(&self) -> &str {
        &self.inner.function
    }

    /// The attempt number, from 1.  A redelivery after a worker dies is not
    /// a new attempt.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.inner.attempt
    }

    /// When the task was due.
    #[must_use]
    pub fn when(&self) -> DateTime<Utc> {
        self.inner.when
    }

    /// The task's arguments as JSON, for behaviors that look at a field.
    #[must_use]
    pub fn args(&self) -> &serde_json::Value {
        &self.inner.args
    }

    /// Reports the task's progress to anyone following it.
    #[must_use]
    pub fn progress(&self) -> &Progress {
        &self.inner.progress
    }

    /// The value a behavior gave this run with
    /// [`Hooks::context`](crate::behaviors::Hooks::context), or `None` when
    /// the task does not have that behavior.
    #[must_use]
    pub fn behavior<V: Any>(&self) -> Option<&V> {
        self.inner
            .behaviors
            .get(&TypeId::of::<V>())
            .and_then(|value| value.downcast_ref())
    }

    pub(crate) fn delivery(&self) -> &Delivery {
        &self.inner.delivery
    }
}

#[cfg(all(test, feature = "memory"))]
impl Context {
    /// A context for a run of `function` with empty arguments, for unit
    /// tests of handlers and hooks.
    pub(crate) fn for_tests(docket: &Docket, key: &str, function: &str) -> Self {
        let mut builder =
            crate::testing::ContextBuilder::named(docket, function, serde_json::json!({}))
                .key(key)
                .worker("worker");
        builder.redelivery_timeout = std::time::Duration::from_millis(40);
        builder.build()
    }
}
