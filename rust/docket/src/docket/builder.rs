//! Opening a docket with settings other than the defaults.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::{Docket, Inner, Registry, Settings, run_limits};
use crate::connection::{self, Backend, Provider, Shared};
use crate::error::Result;
use crate::keys::Keys;
use crate::strikes::{Monitor, SharedStrikes, Strikes};
use crate::telemetry::Telemetry;

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
    pub(super) fn new(name: String, url: String) -> Self {
        Self {
            name,
            url,
            settings: Settings {
                execution_ttl: Duration::from_mins(15),
                heartbeat_interval: Duration::from_secs(2),
                missed_heartbeats: 5,
            },
            connection: connection::Settings::default(),
            credentials: None,
        }
    }

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
    /// most half of it, so that their answers arrive in time.
    ///
    /// By default a command waits as long as it takes, as in pydocket, and
    /// TCP keepalive finds a Redis that is gone.  With a timeout, a Lua
    /// script that runs longer than it, such as a
    /// [`clear`](crate::Docket::clear) of a large docket, fails with a
    /// timeout while Redis still finishes it.
    #[must_use]
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.connection.response_timeout = Some(timeout);
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
    /// The defaults are 30 seconds, 5 seconds, and 3 probes.  The kernel
    /// takes `idle` and `interval` in whole seconds, from 1 to 32767, and
    /// from 1 to 127 probes; other values fail at
    /// [`connect`](Self::connect).  Some systems do not let a program set
    /// the interval or the number of probes, and there only `idle` applies.
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
        let clock = backend.clock();
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
                clock,
            }),
        })
    }
}
