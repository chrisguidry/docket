//! The four hooks that every behavior, built in or written by a user, plugs
//! into.  A behavior attaches itself to a task's hooks when the task is
//! registered.

use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;

use crate::context::Context;
use crate::task::Task;

/// An error from a task's handler.  Any error type converts into it,
/// including `anyhow::Error`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What a task's handler produced: its output as JSON, or its error.
pub type Outcome = Result<serde_json::Value, BoxError>;

/// A running task, which a [`Runtime`] hook wraps.
pub type TaskFuture<'a> = BoxFuture<'a, Outcome>;

/// Something that changes how a task runs, such as a retry or a timeout.
///
/// A behavior attaches itself to the hooks of each task it is registered
/// with.  It is generic over the task, so a behavior can require more of the
/// task's arguments; an automatic perpetual task, for example, requires
/// arguments with a `Default`.
pub trait Behavior<T: Task> {
    /// Puts the behavior into the task's hooks.
    fn attach(self, hooks: &mut Hooks<'_, T>);
}

/// Decides whether a task may start now.  Admission hooks run in the order
/// they were attached, after the worker claims the task and before the
/// handler runs.
pub trait Admission: Send + Sync + 'static {
    /// Admits the task, or blocks it.
    fn admit(
        &self,
        ctx: &Context,
    ) -> impl Future<Output = Result<Admitted, AdmissionBlocked>> + Send;
}

/// Wraps the call to the handler.  A task has at most one; attaching a
/// second replaces the first.
pub trait Runtime: Send + Sync + 'static {
    /// Runs the task, for example with a deadline.
    fn run<'a>(
        &'a self,
        ctx: &'a Context,
        task: TaskFuture<'a>,
    ) -> impl Future<Output = Outcome> + Send + 'a;
}

/// Decides what a failure means.  A task has at most one.
pub trait Failure: Send + Sync + 'static {
    /// Retries the task, or lets it fail.
    fn on_failure(
        &self,
        ctx: &Context,
        error: &BoxError,
    ) -> impl Future<Output = AfterFailure> + Send;
}

/// Runs after the task is done, unless it is retried.  A task has at most one.
pub trait Completion: Send + Sync + 'static {
    /// Decides what happens after the task, such as its next run.
    fn on_complete(
        &self,
        ctx: &Context,
        outcome: &Outcome,
    ) -> impl Future<Output = AfterCompletion> + Send;
}

/// A task that an [`Admission`] hook let through.
pub struct Admitted {
    pub(crate) release: Option<Release>,
}

pub(crate) type Release = Box<dyn FnOnce(Released) -> BoxFuture<'static, ()> + Send>;

/// Why an admission is released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Released {
    /// The task ran, whatever its outcome.
    Ran,
    /// A later admission hook blocked the task, so it did not run.
    Blocked,
}

impl Admitted {
    /// Admits the task with nothing to undo afterward.
    #[must_use]
    pub fn now() -> Self {
        Self { release: None }
    }

    /// Admits the task, and runs `release` when the task no longer holds
    /// what the hook gave it, such as a concurrency slot.
    pub fn with_release<F, Fut>(release: F) -> Self
    where
        F: FnOnce(Released) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            release: Some(Box::new(move |released| Box::pin(release(released)))),
        }
    }
}

/// A task that may not start now.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("admission blocked: {reason}")]
pub struct AdmissionBlocked {
    pub(crate) reason: String,
    pub(crate) retry_delay: Option<Duration>,
    pub(crate) reschedule: bool,
    /// The hook already put the task somewhere, such as a concurrency
    /// waiter stream, so the worker leaves it alone.
    pub(crate) handled: bool,
}

impl AdmissionBlocked {
    /// Blocks the task and tries it again shortly.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retry_delay: None,
            reschedule: true,
            handled: false,
        }
    }

    /// Tries the task again after `delay`.
    #[must_use]
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = Some(delay);
        self
    }

    /// Drops the task instead of trying it again.  It ends as cancelled.
    #[must_use]
    pub fn drop_task(mut self) -> Self {
        self.reschedule = false;
        self
    }

    pub(crate) fn handled(mut self) -> Self {
        self.handled = true;
        self
    }

    /// Why the task was blocked.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// What a [`Failure`] hook decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterFailure {
    /// The task ends as failed.
    Fail,
    /// The task runs again at this time, as its next attempt.
    RetryAt(DateTime<Utc>),
}

/// What a [`Completion`] hook decides.
#[derive(Clone, Debug, PartialEq)]
pub enum AfterCompletion {
    /// The task ends normally.
    Finish,
    /// The task runs again under the same key at this time.  `args`
    /// replaces its arguments when it is set.
    Reschedule {
        /// When the next run is due.
        when: DateTime<Utc>,
        /// The next run's arguments as JSON, or the same arguments.
        args: Option<serde_json::Value>,
    },
    /// The task's key is cancelled, so that no other copy of it runs, and
    /// this run ends normally.
    Cancel,
}

/// The hooks of one task, which behaviors attach to.
pub struct Hooks<'a, T> {
    pub(crate) erased: &'a mut ErasedHooks,
    pub(crate) task: PhantomData<fn(T)>,
}

impl<T: Task> Hooks<'_, T> {
    /// Adds an admission hook.
    pub fn admission(&mut self, hook: impl Admission) {
        let hook = Arc::new(hook);
        self.erased.admissions.push(Arc::new(move |ctx: Context| {
            let hook = Arc::clone(&hook);
            Box::pin(async move { hook.admit(&ctx).await }) as BoxFuture<'static, _>
        }));
    }

    /// Sets the runtime hook.
    pub fn runtime(&mut self, hook: impl Runtime) {
        self.erased.runtime = Some(Arc::new(ErasedRuntimeHook(hook)));
    }

    /// Sets the failure hook.
    pub fn failure(&mut self, hook: impl Failure) {
        let hook = Arc::new(hook);
        self.erased.failure = Some(Arc::new(move |ctx: Context, error: Arc<BoxError>| {
            let hook = Arc::clone(&hook);
            Box::pin(async move { hook.on_failure(&ctx, &error).await }) as BoxFuture<'static, _>
        }));
    }

    /// Sets the completion hook.
    pub fn completion(&mut self, hook: impl Completion) {
        let hook = Arc::new(hook);
        self.erased.completion = Some(Arc::new(move |ctx: Context, outcome: Arc<Outcome>| {
            let hook = Arc::clone(&hook);
            Box::pin(async move { hook.on_complete(&ctx, &outcome).await }) as BoxFuture<'static, _>
        }));
    }

    /// Gives each run of the task a fresh value from `make`, which the
    /// handler and the hooks read with
    /// [`Context::behavior`](crate::Context::behavior).
    pub fn context<V: Send + Sync + 'static>(
        &mut self,
        make: impl Fn() -> V + Send + Sync + 'static,
    ) {
        self.erased.contexts.push(Arc::new(move || {
            Box::new(make()) as Box<dyn Any + Send + Sync>
        }));
    }
}

impl<T: Task + Default> Hooks<'_, T> {
    /// Makes the task automatic: every worker that runs it schedules it at
    /// startup, with default arguments, at the time `first_run` gives, or now.
    ///
    /// # Panics
    ///
    /// When the task's default arguments do not convert to JSON.
    pub fn automatic(
        &mut self,
        first_run: impl Fn() -> Option<DateTime<Utc>> + Send + Sync + 'static,
    ) {
        let args = serde_json::to_string(&T::default())
            .expect("an automatic task's default arguments convert to JSON");
        self.erased.automatic = Some(Automatic {
            args,
            first_run: Arc::new(first_run),
        });
    }
}

type AdmissionFn =
    Arc<dyn Fn(Context) -> BoxFuture<'static, Result<Admitted, AdmissionBlocked>> + Send + Sync>;
type FailureFn =
    Arc<dyn Fn(Context, Arc<BoxError>) -> BoxFuture<'static, AfterFailure> + Send + Sync>;
type CompletionFn =
    Arc<dyn Fn(Context, Arc<Outcome>) -> BoxFuture<'static, AfterCompletion> + Send + Sync>;
type ContextFn = Arc<dyn Fn() -> Box<dyn Any + Send + Sync> + Send + Sync>;

/// A task's hooks with the behaviors' types erased.
#[derive(Clone, Default)]
pub(crate) struct ErasedHooks {
    pub admissions: Vec<AdmissionFn>,
    pub runtime: Option<Arc<dyn ErasedRuntime>>,
    pub failure: Option<FailureFn>,
    pub completion: Option<CompletionFn>,
    pub contexts: Vec<ContextFn>,
    pub automatic: Option<Automatic>,
    /// A concurrency limit needs the task that wakes its parked waiters.
    pub needs_safeguard: bool,
}

#[derive(Clone)]
pub(crate) struct Automatic {
    /// The default arguments as JSON text.
    pub args: String,
    pub first_run: Arc<dyn Fn() -> Option<DateTime<Utc>> + Send + Sync>,
}

pub(crate) trait ErasedRuntime: Send + Sync {
    fn run<'a>(&'a self, ctx: &'a Context, task: TaskFuture<'a>) -> BoxFuture<'a, Outcome>;
}

struct ErasedRuntimeHook<R>(R);

impl<R: Runtime> ErasedRuntime for ErasedRuntimeHook<R> {
    fn run<'a>(&'a self, ctx: &'a Context, task: TaskFuture<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(self.0.run(ctx, task))
    }
}
