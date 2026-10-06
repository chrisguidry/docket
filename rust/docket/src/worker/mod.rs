//! Workers run the tasks in a docket.

mod cancellation;
mod execute;
mod heartbeat;
mod perpetuals;
mod session;
mod sweep;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::docket::{Docket, Registered};
use crate::error::{Error, Result};

/// Runs the tasks in a docket.
///
/// ```no_run
/// # async fn example(docket: docket::Docket) -> docket::Result<()> {
/// docket::Worker::new(docket)
///     .concurrency(20)
///     .run_until(async { tokio::signal::ctrl_c().await.unwrap() })
///     .await
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Worker {
    docket: Docket,
    settings: Settings,
}

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub name: String,
    pub concurrency: usize,
    pub redelivery_timeout: Duration,
    pub reconnection_delay: Duration,
    pub minimum_check_interval: Duration,
    pub scheduling_resolution: Duration,
    pub schedule_automatic_tasks: bool,
    pub automatic_tasks_interval: Duration,
    pub message_batch: usize,
    /// Runs tasks that have no handler.
    pub fallback: Option<Registered>,
}

/// When a worker stops.
#[derive(Clone, Debug)]
pub(crate) enum Until {
    /// When the shutdown token fires.  Running tasks finish first.
    Shutdown(CancellationToken),
    /// When nothing is left to run, now or later.
    Finished,
}

impl Worker {
    /// A worker for `docket`, with the default settings.
    #[must_use]
    pub fn new(docket: Docket) -> Self {
        Self {
            docket,
            settings: Settings {
                name: default_name(),
                concurrency: 10,
                redelivery_timeout: Duration::from_mins(5),
                reconnection_delay: Duration::from_secs(5),
                minimum_check_interval: Duration::from_millis(250),
                scheduling_resolution: Duration::from_millis(250),
                schedule_automatic_tasks: true,
                automatic_tasks_interval: Duration::from_mins(1),
                message_batch: 1000,
                fallback: None,
            },
        }
    }

    /// The worker's name.  The default is `{hostname}#{pid}`.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.settings.name = name.into();
        self
    }

    /// How many tasks run at once.  The default is 10.
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.settings.concurrency = concurrency;
        self
    }

    /// How long a task's delivery may go without a renewal before another
    /// worker takes it over.  A worker renews the deliveries of its running
    /// tasks every quarter of this.  The default is 5 minutes.
    #[must_use]
    pub fn redelivery_timeout(mut self, timeout: Duration) -> Self {
        self.settings.redelivery_timeout = timeout;
        self
    }

    /// How long the worker waits to reconnect after Redis goes away.  The
    /// default is 5 seconds.
    #[must_use]
    pub fn reconnection_delay(mut self, delay: Duration) -> Self {
        self.settings.reconnection_delay = delay;
        self
    }

    /// How long one read waits for new tasks.  The default is 250 ms.
    #[must_use]
    pub fn minimum_check_interval(mut self, interval: Duration) -> Self {
        self.settings.minimum_check_interval = interval;
        self
    }

    /// How often the worker moves due future tasks onto the stream.  The
    /// default is 250 ms.
    #[must_use]
    pub fn scheduling_resolution(mut self, resolution: Duration) -> Self {
        self.settings.scheduling_resolution = resolution;
        self
    }

    /// Whether the worker schedules automatic perpetual tasks when it
    /// starts.  The default is yes.
    #[must_use]
    pub fn schedule_automatic_tasks(mut self, schedule: bool) -> Self {
        self.settings.schedule_automatic_tasks = schedule;
        self
    }

    /// How often the worker schedules its automatic tasks again, in case
    /// one was cancelled or lost.  The default is 1 minute.
    #[must_use]
    pub fn automatic_tasks_interval(mut self, interval: Duration) -> Self {
        self.settings.automatic_tasks_interval = interval;
        self
    }

    /// How many deliveries one read takes at most.  The default is 1000.
    #[must_use]
    pub fn message_batch(mut self, batch: usize) -> Self {
        self.settings.message_batch = batch;
        self
    }

    /// Runs the tasks that no handler is registered for, such as tasks that
    /// an older or newer version of the application added.  The handler gets
    /// the arguments as JSON, and [`Context::function`](crate::Context::function)
    /// names the task.  Without one, the worker logs a warning and completes
    /// such a task.
    #[must_use]
    pub fn fallback<F, Fut, E>(mut self, handler: F) -> Self
    where
        F: Fn(crate::Context, serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<serde_json::Value, E>> + Send + 'static,
        E: Into<crate::behaviors::BoxError>,
    {
        self.settings.fallback = Some(Registered::fallback(handler));
        self
    }

    /// The worker's name.
    #[must_use]
    pub fn worker_name(&self) -> &str {
        &self.settings.name
    }

    /// Runs tasks until `shutdown` completes, then lets the running tasks
    /// finish and returns.
    pub async fn run_until(self, shutdown: impl Future<Output = ()> + Send) -> Result<()> {
        self.run_until_boxed(Box::pin(shutdown)).await
    }

    /// [`Worker::run_until`] for any shutdown future, compiled once.
    async fn run_until_boxed(
        self,
        shutdown: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
    ) -> Result<()> {
        let token = CancellationToken::new();
        let run = self.run(Until::Shutdown(token.clone()), HashMap::new());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => return result,
            () = shutdown => token.cancel(),
        }
        run.await
    }

    /// Runs tasks until the task that runs this future is cancelled or the
    /// future is dropped.  Running tasks stop with it; use
    /// [`Worker::run_until`] to let them finish.
    pub async fn run_forever(self) -> Result<()> {
        self.run_until(std::future::pending()).await
    }

    /// Runs tasks until nothing is left to run, now or in the future.
    pub async fn run_until_finished(self) -> Result<()> {
        self.run(Until::Finished, HashMap::new()).await
    }

    /// Runs tasks until nothing is left, running each listed key at most the
    /// given number of times.  This is for testing perpetual tasks, which
    /// otherwise run forever.
    pub async fn run_at_most(self, iterations: HashMap<String, u32>) -> Result<()> {
        self.run(Until::Finished, iterations).await
    }

    async fn run(self, until: Until, iterations: HashMap<String, u32>) -> Result<()> {
        if self.settings.concurrency == 0 {
            return Err(Error::Invalid(
                "a worker's concurrency must be at least 1".into(),
            ));
        }
        if self.settings.message_batch == 0 {
            return Err(Error::Invalid(
                "a worker's message batch must be at least 1".into(),
            ));
        }
        let worker = Arc::new(session::Shared::new(self.docket, self.settings, iterations));
        // The session future is large, so it lives on the heap rather than
        // in every caller's future.
        Box::pin(session::run(worker, until)).await
    }
}

fn default_name() -> String {
    format!(
        "{}#{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    )
}
