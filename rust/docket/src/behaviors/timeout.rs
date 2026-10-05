//! Stopping a task that runs too long.

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use super::{Behavior, BoxError, Hooks, Outcome, Runtime, TaskFuture};
use crate::context::Context;
use crate::task::Task;

/// Stops a task that runs longer than its time, and fails it with a
/// [`TimedOut`] error, which a retry behavior can retry.  The task can
/// extend its own deadline with [`TimeoutControl::extend`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout {
    base: Duration,
}

impl Timeout {
    /// Stops the task after `base`.
    #[must_use]
    pub fn after(base: Duration) -> Self {
        Self { base }
    }
}

impl<T: Task> Behavior<T> for Timeout {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        let base = self.base;
        hooks.context(move || TimeoutControl {
            base,
            deadline: Mutex::new(None),
        });
        hooks.runtime(self);
    }
}

/// A running task's deadline, from [`Context::timeout`].
#[derive(Debug)]
pub struct TimeoutControl {
    base: Duration,
    deadline: Mutex<Option<Instant>>,
}

impl TimeoutControl {
    /// Moves the deadline `by` later.
    pub fn extend(&self, by: Duration) {
        let mut deadline = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(deadline) = deadline.as_mut() {
            *deadline += by;
        }
    }

    /// Moves the deadline later by the timeout's own length.
    pub fn extend_by_base(&self) {
        self.extend(self.base);
    }

    /// The time left before the deadline.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.deadline().map_or(self.base, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        })
    }

    fn deadline(&self) -> Option<Instant> {
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn start(&self) -> Instant {
        let deadline = Instant::now() + self.base;
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(deadline);
        deadline
    }
}

impl Runtime for Timeout {
    async fn run<'a>(&'a self, ctx: &'a Context, task: TaskFuture<'a>) -> Outcome {
        let control = ctx.timeout().expect("a timeout gives each run its control");
        let mut deadline = control.start();
        tokio::pin!(task);
        loop {
            tokio::select! {
                outcome = &mut task => return outcome,
                () = tokio::time::sleep_until(deadline) => {
                    match control.deadline() {
                        Some(extended) if extended > deadline => deadline = extended,
                        _ => {
                            let error: BoxError = Box::new(TimedOut {
                                key: ctx.key().to_owned(),
                                base: self.base,
                            });
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

/// The error of a task that ran past its deadline.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("Docket task {key} exceeded timeout of {}s", base.as_secs_f64())]
pub struct TimedOut {
    key: String,
    base: Duration,
}

impl Context {
    /// The task's deadline, when it has a [`Timeout`].
    #[must_use]
    pub fn timeout(&self) -> Option<&TimeoutControl> {
        self.behavior()
    }
}

#[cfg(test)]
mod tests;
