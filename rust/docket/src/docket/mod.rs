//! A docket: a named set of tasks in one Redis.

mod builder;
mod registry;
mod run_limits;
mod schedule;
mod snapshot;

use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::Utc;
use opentelemetry::context::FutureExt as _;
use redis::AsyncCommands;

use crate::behaviors::BoxError;
use crate::clock::Clock;
use crate::connection::{Backend, Handle, RedisConnection, Shared};
use crate::context::Context;
use crate::error::Result;
use crate::keys::Keys;
use crate::scripts;
use crate::strikes::{Monitor, SharedStrikes, Strike, Strikes};
use crate::task::Task;
use crate::telemetry::{self, Telemetry};
use crate::wire::iso;

pub use builder::DocketBuilder;
pub use registry::Registration;
pub(crate) use registry::{Registered, Registry};
pub(crate) use run_limits::Limited;
pub(crate) use schedule::Placement;
pub use schedule::{Add, Batch, Call};
pub use snapshot::{Snapshot, TaskSummary, WorkerSummary};

/// A named set of tasks in one Redis.  Producers add tasks to it, and
/// workers run them.
///
/// A `Docket` is cheap to clone, and every clone shares one connection.
///
/// ```no_run
/// # async fn example() -> docket::Result<()> {
/// let docket = docket::Docket::connect("orders", "redis://localhost:6379/0").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Docket {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    keys: Keys,
    shared: Arc<Shared>,
    registry: RwLock<Registry>,
    strikes: SharedStrikes,
    monitor: Monitor,
    settings: Settings,
    telemetry: Arc<Telemetry>,
    run_limits: Mutex<run_limits::RunLimits>,
    clock: Clock,
}

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub execution_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub missed_heartbeats: u32,
}

impl Docket {
    /// Connects to the docket `name` in the Redis at `url`, with the default
    /// settings.
    ///
    /// `url` takes the same forms as in pydocket: `redis://`,
    /// `redis+cluster://`, `redis+sentinel://host:port/service`, their
    /// `rediss` forms with the default `tls` feature, `unix://`, and, with
    /// the `memory` feature, `memory://` for an in-process Redis.
    pub async fn connect(name: impl Into<String>, url: impl Into<String>) -> Result<Self> {
        Self::builder(name, url).connect().await
    }

    /// Starts a docket with settings other than the defaults.
    pub fn builder(name: impl Into<String>, url: impl Into<String>) -> DocketBuilder {
        DocketBuilder::new(name.into(), url.into())
    }

    /// The docket's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Opens a connection to the docket's Redis, for an application's own
    /// keys, as pydocket's `docket.redis()` does.  Each call opens a new
    /// connection, so a blocking command on it holds up nothing of docket's.
    /// As in pydocket, a command on it waits for Redis as long as it takes:
    /// the docket's [`response_timeout`](DocketBuilder::response_timeout)
    /// does not apply.
    ///
    /// It reaches a `memory://` docket's in-process Redis too, which no
    /// other client can.  That Redis knows the commands docket itself sends:
    /// those for strings, hashes, sets, sorted sets, streams, and scripts,
    /// and not, for example, `INCR` or lists.
    pub async fn redis(&self) -> Result<RedisConnection> {
        Ok(RedisConnection(
            self.inner
                .shared
                .backend()
                .connect_for_application()
                .await?,
        ))
    }

    /// Registers the handler for a task.  The argument type names the task.
    /// Behaviors attach to the returned registration with
    /// [`Registration::with`].
    ///
    /// Registering a task again replaces its handler and its behaviors.
    pub fn register<T, F, Fut, E>(&self, handler: F) -> Registration<T>
    where
        T: Task,
        F: Fn(Context, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T::Output, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        self.inner
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(T::NAME, Registered::new(handler));
        Registration::new(self.clone())
    }

    /// The names of the registered tasks.
    #[must_use]
    pub fn task_names(&self) -> Vec<String> {
        self.inner
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .names()
    }

    /// Cancels a task.  A scheduled task is removed, and a running task is
    /// stopped at its next `.await`.
    pub async fn cancel(&self, key: &str) -> Result<()> {
        let mut attributes = self.labels();
        attributes.push(opentelemetry::KeyValue::new("docket.key", key.to_owned()));
        let span = self.telemetry().producer_span("docket.cancel", attributes);
        let cancelled = self.cancel_quietly(key, 0).with_context(span.clone()).await;
        telemetry::end(&span, cancelled.as_ref().map(|_| Vec::new()));
        cancelled?;
        self.telemetry()
            .metrics
            .tasks_cancelled
            .add(1, &self.labels());
        Ok(())
    }

    /// Cancels a task without a span or a count, for docket's own cancels,
    /// which pydocket does not count either.
    ///
    /// A run that cancels its own key passes its generation, and the cancel
    /// leaves the key alone when a newer generation holds it.  0 skips that
    /// check.  Returns whether the cancel happened.
    pub(crate) async fn cancel_quietly(&self, key: &str, expected_generation: i64) -> Result<bool> {
        let keys = &self.inner.keys;
        let completed_at = iso(self.now());
        let payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": "cancelled",
            "completed_at": completed_at,
        });
        let call = scripts::CancelTask {
            stream_key: keys.stream(),
            known_key: keys.known(key),
            parked_key: keys.parked(key),
            queue_key: keys.queue(),
            stream_id_key: keys.stream_id(key),
            runs_key: keys.runs(key),
            progress_key: keys.progress(key),
            state_channel: keys.state(key),
            task_key: key.to_owned(),
            completed_at,
            state_payload: payload.to_string(),
            expected_generation,
        }
        .call();
        let mut connection = self.handle();
        let reply: String = call.run(&mut connection).await?;
        // The newer generation owns the runs hash and the running task, so
        // neither its lifetime nor a cancel signal may touch them.
        if reply == "SUPERSEDED" {
            return Ok(false);
        }
        self.expire_runs(&mut connection, key).await?;
        let _: i64 = connection.publish(keys.cancel(key), key).await?;
        Ok(true)
    }

    /// Strikes a task, or the calls that match a condition.
    pub async fn strike(&self, strike: Strike) -> Result<()> {
        self.send_strike(strike, false).await
    }

    /// Removes a strike.
    pub async fn restore(&self, strike: Strike) -> Result<()> {
        self.send_strike(strike, true).await
    }

    async fn send_strike(&self, strike: Strike, restore: bool) -> Result<()> {
        let mut attributes = self.labels();
        attributes.extend(crate::strikes::labels(&strike));
        let name = if restore {
            "docket.restore"
        } else {
            "docket.strike"
        };
        let span = self.telemetry().producer_span(name, attributes);
        let fields = crate::strikes::instruction(&strike, restore);
        let mut connection = self.handle();
        let sent: Result<String> = connection
            .xadd(self.inner.keys.strikes(), "*", &fields)
            .with_context(span.clone())
            .await
            .map_err(Into::into);
        telemetry::end(&span, sent.as_ref().map(|_| Vec::new()));
        sent?;
        self.inner.strikes.apply(&strike, restore);
        Ok(())
    }

    /// Waits until every strike that existed when the docket connected is
    /// in force here.
    pub async fn strikes_loaded(&self) {
        self.inner.monitor.loaded().await;
    }

    pub(crate) fn telemetry(&self) -> &Telemetry {
        &self.inner.telemetry
    }

    /// `docket.name`, which every metric and span carries.
    pub(crate) fn labels(&self) -> Vec<opentelemetry::KeyValue> {
        telemetry::docket_labels(&self.inner.name)
    }

    /// The time on the docket's clock, which a test can move on `memory://`.
    pub(crate) fn now(&self) -> chrono::DateTime<Utc> {
        self.inner.clock.now()
    }

    pub(crate) fn clock(&self) -> &Clock {
        &self.inner.clock
    }

    pub(crate) fn keys(&self) -> &Keys {
        &self.inner.keys
    }

    pub(crate) fn settings(&self) -> &Settings {
        &self.inner.settings
    }

    pub(crate) fn backend(&self) -> &Arc<Backend> {
        self.inner.shared.backend()
    }

    pub(crate) fn strikes(&self) -> &Strikes {
        &self.inner.strikes
    }

    pub(crate) fn automatic_tasks(&self) -> Vec<(String, crate::behaviors::Automatic)> {
        self.inner
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .automatic()
    }

    pub(crate) fn registered(&self, function: &str) -> Option<Registered> {
        self.inner
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(function)
    }

    pub(crate) fn also_name(&self, name: &str, other: &str) {
        self.inner
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .also_name(name, other);
    }

    pub(crate) fn with_registered<R>(
        &self,
        function: &str,
        change: impl FnOnce(&mut Registered) -> R,
    ) -> R {
        self.inner
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .change(function, change)
    }

    /// A handle on the shared connection, once it is open.
    pub(crate) async fn connection(&self) -> Result<Handle> {
        Ok(self.inner.shared.get().await?)
    }

    /// A handle on the shared connection, which connects on its first
    /// command.
    pub(crate) fn handle(&self) -> Handle {
        self.inner.shared.handle()
    }

    /// Lets a finished task's run state expire, or deletes it when the
    /// docket keeps nothing.
    pub(crate) async fn expire_runs(&self, connection: &mut Handle, key: &str) -> Result<()> {
        let runs = self.inner.keys.runs(key);
        match self.ttl_seconds() {
            0 => connection.del::<_, ()>(runs).await?,
            ttl => connection.expire::<_, ()>(runs, ttl).await?,
        }
        Ok(())
    }

    /// The execution TTL in whole seconds, the unit Redis expires keys in.
    pub(crate) fn ttl_seconds(&self) -> i64 {
        i64::try_from(self.inner.settings.execution_ttl.as_secs()).unwrap_or(i64::MAX)
    }
}

impl std::fmt::Debug for Docket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Docket")
            .field("name", &self.inner.name)
            .finish_non_exhaustive()
    }
}
