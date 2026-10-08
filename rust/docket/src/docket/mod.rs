//! A docket: a named set of tasks in one Redis.

mod registry;
mod run_limits;
mod schedule;
mod snapshot;

use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::Utc;
use opentelemetry::context::FutureExt as _;
use redis::{AsyncCommands, Value};

use crate::behaviors::BoxError;
use crate::connection::{self, Backend, Handle, Provider, Shared};
use crate::context::Context;
use crate::error::Result;
use crate::keys::Keys;
use crate::scripts;
use crate::strikes::{Monitor, SharedStrikes, Strike, Strikes};
use crate::task::Task;
use crate::telemetry::{self, Telemetry};
use crate::wire::iso;

pub use registry::Registration;
pub(crate) use registry::{Registered, Registry};
pub(crate) use run_limits::Limited;
pub(crate) use schedule::Placement;
pub use schedule::{Add, Call};
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
}

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub execution_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub missed_heartbeats: u32,
}

/// Opens a docket with settings other than the defaults.
#[derive(Clone)]
pub struct DocketBuilder {
    name: String,
    url: String,
    settings: Settings,
    connection: connection::Settings,
    credentials: Option<Provider>,
}

impl std::fmt::Debug for DocketBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocketBuilder")
            .field("name", &self.name)
            .field("url", &connection::redact(&self.url))
            .field("settings", &self.settings)
            .field("connection", &self.connection)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl DocketBuilder {
    /// How long a finished task's state and result stay in Redis.  Zero
    /// deletes them as soon as the task ends.  The default is 15 minutes.
    #[must_use]
    pub fn execution_ttl(mut self, ttl: Duration) -> Self {
        self.settings.execution_ttl = ttl;
        self
    }

    /// How often each worker reports that it is alive.  The default is 2
    /// seconds.
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.settings.heartbeat_interval = interval;
        self
    }

    /// How many heartbeats a worker may miss before the docket stops listing
    /// it.  The default is 5.
    #[must_use]
    pub fn missed_heartbeats(mut self, missed: u32) -> Self {
        self.settings.missed_heartbeats = missed;
        self
    }

    /// How long a connect to Redis may take before it fails.  The default
    /// is 10 seconds.
    #[must_use]
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection.connection_timeout = timeout;
        self
    }

    /// How long a command may wait for Redis to answer before it fails.
    /// Blocking reads, such as a worker's wait for new tasks, block for at
    /// most half of it, so that their answers arrive in time.  The default
    /// is 10 seconds.
    #[must_use]
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.connection.response_timeout = timeout;
        self
    }

    /// How many times a command to a Redis Cluster is sent again after a
    /// redirect, a lost node, or a busy node, before it fails.  A cluster
    /// redirects commands while it moves slots between nodes, and with no
    /// retries those commands fail.  The default is 10.  Connections to one
    /// server do not retry.
    #[must_use]
    pub fn retries(mut self, retries: u32) -> Self {
        self.connection.retries = retries;
        self
    }

    /// The shortest and longest waits before a Redis Cluster command is
    /// retried.  The wait grows exponentially from `min` to `max`.  The
    /// defaults are 10 milliseconds and 1 second.
    #[must_use]
    pub fn retry_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.connection.min_retry_wait = min;
        self.connection.max_retry_wait = max;
        self
    }

    /// TCP keepalive for every connection to Redis: after `idle` without
    /// traffic, the kernel sends a probe every `interval`, and drops the
    /// connection after `probes` of them go unanswered.  This finds a Redis
    /// that vanished without closing the connection, which the response
    /// timeout cannot do for a subscription that only waits for messages.
    /// The defaults are 30 seconds, 5 seconds, and 3 probes.  Some systems
    /// do not let a program set the interval or the number of probes, and
    /// there only `idle` applies.
    #[must_use]
    pub fn tcp_keepalive(mut self, idle: Duration, interval: Duration, probes: u32) -> Self {
        self.connection.keepalive = connection::Keepalive {
            idle,
            interval,
            probes,
        };
        self
    }

    /// Takes the username and password from `provider`, in place of the URL,
    /// for servers whose passwords are tokens that rotate, such as Azure
    /// Entra ID.  Command connections renew their credentials when the
    /// provider gives new ones, and subscriptions take the credentials in
    /// force when they open.  A URL that carries credentials of its own is
    /// refused.
    #[must_use]
    pub fn credentials_provider(
        mut self,
        provider: impl redis::StreamingCredentialsProvider + 'static,
    ) -> Self {
        self.credentials = Some(Provider(Arc::new(provider)));
        self
    }

    /// Connects to the docket.  The connection itself opens on first use, so
    /// this fails only on a URL or settings docket cannot use.
    pub async fn connect(self) -> Result<Docket> {
        let backend = Arc::new(Backend::open(&self.url, self.credentials, self.connection)?);
        let keys = Keys::new(backend.prefix(&self.name));
        let strikes: SharedStrikes = Arc::new(Strikes::default());
        let telemetry = Arc::new(Telemetry::new());
        let monitor = Monitor::start(
            Arc::clone(&backend),
            keys.strikes(),
            Arc::clone(&strikes),
            self.name.clone(),
            Arc::clone(&telemetry),
        );
        Ok(Docket {
            inner: Arc::new(Inner {
                name: self.name,
                keys,
                shared: Arc::new(Shared::new(backend)),
                registry: RwLock::new(Registry::default()),
                strikes,
                monitor,
                settings: self.settings,
                telemetry,
                run_limits: Mutex::new(run_limits::RunLimits::default()),
            }),
        })
    }
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
        DocketBuilder {
            name: name.into(),
            url: url.into(),
            settings: Settings {
                execution_ttl: Duration::from_mins(15),
                heartbeat_interval: Duration::from_secs(2),
                missed_heartbeats: 5,
            },
            connection: connection::Settings::default(),
            credentials: None,
        }
    }

    /// The docket's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
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
        let cancelled = self.cancel_quietly(key).with_context(span.clone()).await;
        telemetry::end(&span, cancelled.as_ref().map(|()| Vec::new()));
        cancelled?;
        self.telemetry()
            .metrics
            .tasks_cancelled
            .add(1, &self.labels());
        Ok(())
    }

    /// Cancels a task without a span or a count, for docket's own cancels,
    /// which pydocket does not count either.
    pub(crate) async fn cancel_quietly(&self, key: &str) -> Result<()> {
        let keys = &self.inner.keys;
        let completed_at = iso(Utc::now());
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
        }
        .call();
        let mut connection = self.handle();
        call.run::<Value, _>(&mut connection).await?;
        self.expire_runs(&mut connection, key).await?;
        let _: i64 = connection.publish(keys.cancel(key), key).await?;
        Ok(())
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
