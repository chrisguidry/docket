//! Tasks that schedule their own next run.

use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::Instant;

use super::{AfterCompletion, Behavior, Completion, Hooks, Outcome};
use crate::context::Context;
use crate::task::Task;

/// A perpetual task that runs only after something adds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Manual;

/// A perpetual task that every worker adds when it starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Automatic;

/// Runs a task again under the same key each time it finishes, about
/// `every` after the last run started.  The task can stop itself, or move or
/// change its next run, through [`Context::perpetual`].
///
/// [`Perpetual::automatic`] makes every worker add the task when it starts,
/// which needs task arguments with a `Default`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Perpetual<Mode = Manual> {
    every: Duration,
    mode: PhantomData<Mode>,
}

impl Perpetual {
    /// Runs the task again `every` after each run starts.
    #[must_use]
    pub fn every(every: Duration) -> Self {
        Self {
            every,
            mode: PhantomData,
        }
    }

    /// Makes every worker add the task, with default arguments, when it
    /// starts.
    #[must_use]
    pub fn automatic(self) -> Perpetual<Automatic> {
        Perpetual {
            every: self.every,
            mode: PhantomData,
        }
    }
}

fn attach_perpetual<T: Task>(every: Duration, hooks: &mut Hooks<'_, T>) {
    hooks.context(PerpetualControl::new);
    hooks.completion(Next { every });
}

impl<T: Task> Behavior<T> for Perpetual<Manual> {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        attach_perpetual(self.every, hooks);
    }
}

impl<T: Task + Default> Behavior<T> for Perpetual<Automatic> {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        attach_perpetual(self.every, hooks);
        hooks.automatic(|| None);
    }
}

struct Next {
    every: Duration,
}

impl Completion for Next {
    async fn on_complete(&self, ctx: &Context, _outcome: &Outcome) -> AfterCompletion {
        let control = ctx
            .perpetual()
            .expect("a perpetual task gives each run its control");
        // Counting from the start keeps the runs `every` apart however long
        // each one takes, and a run longer than `every` is followed at once.
        let elapsed = control.started.elapsed();
        let wait = self.every.saturating_sub(elapsed);
        control.decide(Utc::now() + chrono::Duration::from_std(wait).unwrap_or_default())
    }
}

/// A perpetual task's next run, from [`Context::perpetual`].
#[derive(Debug)]
pub struct PerpetualControl {
    started: Instant,
    /// The same moment on the wall clock, which a cron schedule counts from.
    pub(crate) started_at: DateTime<Utc>,
    next: Mutex<Upcoming>,
}

#[derive(Debug, Default)]
struct Upcoming {
    cancelled: bool,
    when: Option<DateTime<Utc>>,
    args: Option<serde_json::Value>,
}

impl PerpetualControl {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            started_at: Utc::now(),
            next: Mutex::new(Upcoming::default()),
        }
    }

    /// Stops the task: no next run is scheduled, and the key is cancelled.
    pub fn cancel(&self) {
        self.lock().cancelled = true;
    }

    /// Runs the task next after `delay`, instead of its usual time.
    pub fn after(&self, delay: Duration) {
        self.at(Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX));
    }

    /// Runs the task next at `when`.
    pub fn at(&self, when: DateTime<Utc>) {
        self.lock().when = Some(when);
    }

    /// Runs the task next with these arguments.
    ///
    /// # Panics
    ///
    /// When `args` does not convert to JSON.
    pub fn perpetuate<T: Task>(&self, args: &T) {
        self.lock().args =
            Some(serde_json::to_value(args).expect("task arguments convert to JSON"));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Upcoming> {
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn decide(&self, usual: DateTime<Utc>) -> AfterCompletion {
        let next = self.lock();
        if next.cancelled {
            return AfterCompletion::Cancel;
        }
        AfterCompletion::Reschedule {
            when: next.when.unwrap_or(usual),
            args: next.args.clone(),
        }
    }
}

impl Context {
    /// The task's next run, when it is perpetual or on a cron schedule.
    #[must_use]
    pub fn perpetual(&self) -> Option<&PerpetualControl> {
        self.behavior()
    }
}
