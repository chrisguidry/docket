use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::FutureExt;

use super::Docket;
use crate::behaviors::{
    Behavior, BoxError, ErasedHooks, Hooks, SafeguardWake, TaskFuture, safeguard_wake,
};
use crate::context::Context;
use crate::task::Task;

/// A handler with its argument and output types erased: it takes the
/// arguments as JSON text and returns the output as JSON.
pub(crate) type Handler = Arc<dyn Fn(Context, &str) -> TaskFuture<'static> + Send + Sync>;

/// A task's handler and the hooks its behaviors attached.
#[derive(Clone)]
pub(crate) struct Registered {
    pub handler: Handler,
    pub hooks: ErasedHooks,
}

impl Registered {
    pub fn new<T, F, Fut, E>(handler: F) -> Self
    where
        T: Task,
        F: Fn(Context, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T::Output, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        let handler: Handler = Arc::new(
            move |ctx: Context, args: &str| match serde_json::from_str::<T>(args) {
                Ok(args) => handler(ctx, args)
                    .map(|outcome| {
                        let output = outcome.map_err(Into::into)?;
                        Ok(serde_json::to_value(output)?)
                    })
                    .boxed(),
                Err(error) => {
                    let error: BoxError = error.into();
                    std::future::ready(Err(error)).boxed()
                }
            },
        );
        Self {
            handler,
            hooks: ErasedHooks::default(),
        }
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
        if needs_safeguard && self.docket.registered(SafeguardWake::NAME).is_none() {
            self.docket.register(safeguard_wake);
        }
        self
    }
}
