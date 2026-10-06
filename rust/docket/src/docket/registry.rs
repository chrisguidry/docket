use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::{FutureExt, TryFutureExt, future};

use super::Docket;
use crate::behaviors::{
    Automatic, Behavior, BoxError, ErasedHooks, Hooks, SafeguardWake, TaskFuture, safeguard_wake,
};
use crate::context::Context;
use crate::task::{Task, TaskField};

/// A handler with its argument and output types erased: it takes the
/// arguments as JSON text and returns the output as JSON.
pub(crate) type Handler = Arc<dyn Fn(Context, &str) -> TaskFuture<'static> + Send + Sync>;

/// A task's handler and the hooks its behaviors attached.
#[derive(Clone)]
pub(crate) struct Registered {
    pub handler: Handler,
    pub hooks: ErasedHooks,
    /// The task's fields, for the call its log lines show.
    pub fields: &'static [TaskField],
}

impl Registered {
    pub fn new<T, F, Fut, E>(handler: F) -> Self
    where
        T: Task,
        F: Fn(Context, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T::Output, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        // Errors pass through the combinators untouched, so this code, which
        // compiles once for every task, has no branch of its own.
        let handler: Handler = Arc::new(move |ctx: Context, args: &str| {
            let called = serde_json::from_str::<T>(args).map(|args| {
                handler(ctx, args).map(|outcome| outcome.map_err(Into::<BoxError>::into))
            });
            future::ready(called.map_err(BoxError::from))
                .try_flatten()
                .and_then(|output| {
                    future::ready(serde_json::to_value(output).map_err(BoxError::from))
                })
                .boxed()
        });
        Self {
            handler,
            hooks: ErasedHooks::default(),
            fields: T::FIELDS,
        }
    }

    /// A handler for any task, which takes the arguments as JSON.
    pub fn fallback<F, Fut, E>(handler: F) -> Self
    where
        F: Fn(Context, serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        let handler: Handler = Arc::new(move |ctx: Context, args: &str| {
            let called = serde_json::from_str(args).map(|args| {
                handler(ctx, args).map(|outcome| outcome.map_err(Into::<BoxError>::into))
            });
            future::ready(called.map_err(BoxError::from))
                .try_flatten()
                .boxed()
        });
        Self {
            handler,
            hooks: ErasedHooks::default(),
            fields: &[],
        }
    }
}

impl std::fmt::Debug for Registered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registered").finish_non_exhaustive()
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    tasks: HashMap<String, Registered>,
}

impl Registry {
    pub fn insert(&mut self, name: &str, registered: Registered) {
        self.tasks.insert(name.to_owned(), registered);
    }

    pub fn get(&self, name: &str) -> Option<Registered> {
        self.tasks.get(name).cloned()
    }

    pub fn change<R>(&mut self, name: &str, change: impl FnOnce(&mut Registered) -> R) -> R {
        change(
            self.tasks
                .get_mut(name)
                .expect("a registration changes only a registered task"),
        )
    }

    /// The automatic tasks, by name.
    pub fn automatic(&self) -> Vec<(String, Automatic)> {
        self.tasks
            .iter()
            .filter_map(|(name, registered)| {
                registered
                    .hooks
                    .automatic
                    .clone()
                    .map(|automatic| (name.clone(), automatic))
            })
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tasks.keys().cloned().collect();
        names.sort();
        names
    }
}

/// A registered task, which behaviors attach to.
///
/// ```no_run
/// # use docket::{Docket, ExponentialRetry, Task, Timeout};
/// # use std::time::Duration;
/// # #[derive(serde::Serialize, serde::Deserialize, Task)]
/// # #[task(name = "charge")]
/// # struct Charge { customer: u64 }
/// # async fn example(docket: Docket) {
/// docket
///     .register(|_ctx, _args: Charge| async { Ok::<_, std::io::Error>(()) })
///     .with(ExponentialRetry::attempts(5))
///     .with(Timeout::after(Duration::from_secs(30)));
/// # }
/// ```
pub struct Registration<T> {
    docket: Docket,
    task: PhantomData<fn(T)>,
}

impl<T: Task> Registration<T> {
    pub(crate) fn new(docket: Docket) -> Self {
        Self {
            docket,
            task: PhantomData,
        }
    }

    /// Attaches a behavior to the task.
    #[expect(
        clippy::return_self_not_must_use,
        reason = "a registration chain ends in a statement, and that must not warn"
    )]
    pub fn with(self, behavior: impl Behavior<T>) -> Self {
        let needs_safeguard = self.docket.with_registered(T::NAME, |registered| {
            behavior.attach(&mut Hooks {
                erased: &mut registered.hooks,
                task: PhantomData,
            });
            registered.hooks.needs_safeguard
        });
        register_safeguard(&self.docket, needs_safeguard);
        self
    }
}

/// Registers the task that wakes parked waiters, when a behavior needs it
/// and it is not registered yet.
fn register_safeguard(docket: &Docket, needed: bool) {
    if needed && docket.registered(SafeguardWake::NAME).is_none() {
        docket.register(safeguard_wake);
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
