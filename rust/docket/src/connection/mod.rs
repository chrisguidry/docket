//! Connections to the Redis behind a docket, whatever kind of server it is.

mod credentials;
mod settings;
#[cfg(test)]
pub(crate) mod slots;
#[cfg(test)]
mod tests;
mod url;

use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionLike, MultiplexedConnection, PubSub};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, IntoConnectionInfo, Pipeline, RedisFuture,
    RedisResult, TlsMode, Value,
};
use tokio::sync::Mutex;

use crate::error::Result;
pub(crate) use credentials::Provider;
pub(crate) use settings::{Keepalive, Settings};
use url::Target;
pub(crate) use url::redact;

/// How to reach the Redis behind a docket.
pub(crate) struct Backend {
    kind: Kind,
    credentials: Option<Provider>,
    settings: Settings,
}

enum Kind {
    Standalone(Client),
    Cluster {
        client: Box<ClusterClient>,
        /// Pub/sub goes through a plain connection to one node, because every
        /// node relays every published message.
        pubsub: Client,
    },
    Sentinel(Mutex<SentinelClient>),
    #[cfg(feature = "memory")]
    Memory(crate::memory::MemoryServer),
}

impl Backend {
    pub fn open(url: &str, credentials: Option<Provider>, settings: Settings) -> Result<Self> {
        #[cfg(not(feature = "tls"))]
        if url.starts_with("rediss") {
            return Err(crate::Error::url(
                url,
                "rediss:// needs docket's tls feature",
            ));
        }
        settings.validate()?;
        let kind = match url::parse(url)? {
            Target::Standalone(url) => client(&url, &settings).map(Kind::Standalone),
            Target::Cluster(node) => client(&node, &settings).and_then(|pubsub| {
                let (min_wait, max_wait) = settings.retry_millis();
                let builder = ClusterClient::builder([node.as_str()])
                    .connection_timeout(settings.connection_timeout)
                    .response_timeout(settings.response_timeout)
                    .retries(settings.retries)
                    .min_retry_wait(min_wait)
                    .max_retry_wait(max_wait)
                    .tcp_settings(settings.tcp());
                let builder = match &credentials {
                    Some(provider) => builder.set_credentials_provider(provider.clone()),
                    None => builder,
                };
                builder.build().map(|client| Kind::Cluster {
                    client: Box::new(client),
                    pubsub,
                })
            }),
            Target::Sentinel(sentinel) => sentinel_client(sentinel, &settings)
                .map(|client| Kind::Sentinel(Mutex::new(client))),
            #[cfg(feature = "memory")]
            Target::Memory(url) => Ok(Kind::Memory(crate::memory::MemoryServer::open(&url))),
            #[cfg(not(feature = "memory"))]
            Target::Memory(url) => {
                return Err(crate::Error::url(
                    &url,
                    "memory:// needs docket's memory feature",
                ));
            }
        }?;
        let url_credentials = match &kind {
            Kind::Standalone(client) | Kind::Cluster { pubsub: client, .. } => {
                credentials::has_credentials(client)
            }
            Kind::Sentinel(_) => false,
            #[cfg(feature = "memory")]
            Kind::Memory(_) => false,
        };
        if credentials.is_some() && url_credentials {
            return Err(crate::Error::url(
                url,
                "it carries credentials, and a credential provider gives them too",
            ));
        }
        Ok(Self {
            kind,
            credentials,
            settings,
        })
    }

    /// The prefix of every key in the docket.  On a cluster it is a hash tag,
    /// so that every key of the docket lands in one slot and the Lua scripts
    /// can touch them together.
    pub fn prefix(&self, name: &str) -> String {
        match self.kind {
            Kind::Cluster { .. } => format!("{{{name}}}"),
            _ => name.to_owned(),
        }
    }

    /// Opens a new connection.  Blocking reads hold their connection for the
    /// whole block, so each loop that blocks opens its own.
    pub async fn connect(&self) -> RedisResult<Connection> {
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.settings.connection_timeout))
            .set_response_timeout(Some(self.settings.response_timeout));
        let config = match &self.credentials {
            Some(provider) => config.set_credentials_provider(provider.clone()),
            None => config,
        };
        match &self.kind {
            Kind::Standalone(client) => client
                .get_multiplexed_async_connection_with_config(&config)
                .await
                .map(Connection::Single),
            Kind::Cluster { client, .. } => {
                client.get_async_connection().await.map(Connection::Cluster)
            }
            Kind::Sentinel(client) => {
                // The master can move after a failover, so each new connection
                // asks the sentinels where it is now.
                let client = client.lock().await.async_get_client().await?;
                client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .map(Connection::Single)
            }
            #[cfg(feature = "memory")]
            Kind::Memory(server) => server.connection().await.map(Connection::Single),
        }
    }

    /// Opens a connection subscribed to `channels`.
    pub async fn subscribe(&self, channels: &[String]) -> RedisResult<PubSub> {
        let mut pubsub = self.pubsub().await?;
        within(self.settings.response_timeout, pubsub.subscribe(channels))
            .await
            .map(|()| pubsub)
    }

    /// Opens a connection subscribed to the channels that match `pattern`.
    pub async fn psubscribe(&self, pattern: String) -> RedisResult<PubSub> {
        let mut pubsub = self.pubsub().await?;
        within(self.settings.response_timeout, pubsub.psubscribe(pattern))
            .await
            .map(|()| pubsub)
    }

    /// Opens a connection for subscriptions.  redis-rs takes no timeouts
    /// for these, so docket bounds the connect and the subscribe itself.
    /// Once subscribed, the connection waits for messages as long as it
    /// takes, and TCP keepalive finds a peer that is gone.
    async fn pubsub(&self) -> RedisResult<PubSub> {
        within(self.settings.connection_timeout, self.open_pubsub()).await
    }

    async fn open_pubsub(&self) -> RedisResult<PubSub> {
        let client = match &self.kind {
            Kind::Standalone(client) | Kind::Cluster { pubsub: client, .. } => client.clone(),
            Kind::Sentinel(client) => client.lock().await.async_get_client().await?,
            #[cfg(feature = "memory")]
            Kind::Memory(server) => return server.pubsub().await,
        };
        match &self.credentials {
            Some(provider) => {
                provider
                    .authenticate(&client)
                    .await?
                    .get_async_pubsub()
                    .await
            }
            None => client.get_async_pubsub().await,
        }
    }

    /// The clock of the dockets on this backend.
    pub fn clock(&self) -> crate::clock::Clock {
        match &self.kind {
            #[cfg(feature = "memory")]
            Kind::Memory(server) => server.clock(),
            _ => crate::clock::Clock::default(),
        }
    }

    /// How long a blocking read may block when it would like to block for
    /// `wanted`.
    pub fn block(&self, wanted: Duration) -> Duration {
        self.settings.block(wanted)
    }

    /// Whether this backend is a cluster, whose nodes cannot be trusted to
    /// keep a script loaded for pipelined `EVALSHA`.
    pub fn is_cluster(&self) -> bool {
        matches!(self.kind, Kind::Cluster { .. })
    }
}

/// A client for one server, with docket's TCP settings.
fn client(url: &str, settings: &Settings) -> RedisResult<Client> {
    let info = url.into_connection_info()?.set_tcp_settings(settings.tcp());
    Client::open(info)
}

/// `future`, or a timeout error once `timeout` passes.
async fn within<T>(
    timeout: Duration,
    future: impl Future<Output = RedisResult<T>>,
) -> RedisResult<T> {
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into()))
}

fn sentinel_client(sentinel: url::SentinelUrl, settings: &Settings) -> RedisResult<SentinelClient> {
    let tls = sentinel.tls.then_some(TlsMode::Secure);
    let addresses = sentinel
        .sentinels
        .into_iter()
        .map(|(host, port)| match tls {
            Some(_) => ConnectionAddr::TcpTls {
                host,
                port,
                insecure: false,
                tls_params: None,
            },
            None => ConnectionAddr::Tcp(host, port),
        });
    // Neither step fails for the addresses and TLS settings built here.
    SentinelClientBuilder::new(addresses, sentinel.service, SentinelServerType::Master).and_then(
        |builder| {
            let mut builder = builder
                .set_client_to_redis_db(sentinel.db)
                .set_client_to_redis_tcp_settings(settings.tcp())
                .set_client_to_sentinel_tcp_settings(settings.tcp());
            if let Some(tls) = tls {
                builder = builder
                    .set_client_to_redis_tls_mode(tls)
                    .set_client_to_sentinel_tls_mode(tls);
            }
            if let Some(username) = sentinel.username {
                builder = builder.set_client_to_redis_username(username);
            }
            if let Some(password) = sentinel.password {
                builder = builder.set_client_to_redis_password(password);
            }
            if let Some(username) = sentinel.daemon_username {
                builder = builder.set_client_to_sentinel_username(username);
            }
            if let Some(password) = sentinel.daemon_password {
                builder = builder.set_client_to_sentinel_password(password);
            }
            builder.build()
        },
    )
}

/// One connection to a single server or to a cluster.
#[derive(Clone)]
pub(crate) enum Connection {
    Single(MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Single(connection) => connection.req_packed_command(cmd),
            Self::Cluster(connection) => connection.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Self::Single(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Single(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

/// A connection to the Redis behind a docket, for an application's own
/// keys; see [`Docket::redis`](crate::Docket::redis).  redis-rs's
/// [`AsyncCommands`](redis::AsyncCommands) work on it.
#[derive(Clone)]
pub struct RedisConnection(pub(crate) Connection);

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.0.req_packed_commands(pipeline, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}

/// The connection that a docket shares among its callers.  It opens the
/// connection on first use, and opens a new one after Redis drops it, so a
/// producer survives a Redis restart without help.
pub(crate) struct Shared {
    backend: Arc<Backend>,
    current: Mutex<Option<Connection>>,
}

impl Shared {
    pub fn new(backend: Arc<Backend>) -> Self {
        Self {
            backend,
            current: Mutex::new(None),
        }
    }

    pub fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }

    /// A handle on the shared connection, which connects on its first
    /// command.
    pub fn handle(self: &Arc<Self>) -> Handle {
        Handle {
            shared: Arc::clone(self),
        }
    }

    /// A handle on the shared connection, once the connection is open.
    pub async fn get(self: &Arc<Self>) -> RedisResult<Handle> {
        self.connection().await?;
        Ok(self.handle())
    }

    async fn connection(&self) -> RedisResult<Connection> {
        let mut current = self.current.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.clone());
        }
        let connection = self.backend.connect().await?;
        *current = Some(connection.clone());
        Ok(connection)
    }

    async fn forget(&self) {
        *self.current.lock().await = None;
    }
}

/// A handle on the shared connection.  Each command runs on the connection
/// the docket holds at that moment, so a handle kept across a Redis restart
/// goes on working once Redis is back.
pub(crate) struct Handle {
    shared: Arc<Shared>,
}

impl Handle {
    /// Forgets the shared connection when a command fails because the
    /// connection is gone, so that the next command opens a new one.
    async fn check<T>(&self, result: RedisResult<T>) -> RedisResult<T> {
        if let Err(error) = &result
            && (error.is_connection_dropped() || error.is_io_error())
        {
            self.shared.forget().await;
        }
        result
    }
}

impl ConnectionLike for Handle {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let mut connection = self.shared.connection().await?;
            let result = connection.req_packed_command(cmd).await;
            self.check(result).await
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let mut connection = self.shared.connection().await?;
            let result = connection
                .req_packed_commands(pipeline, offset, count)
                .await;
            self.check(result).await
        })
    }

    /// The database of the open connection, or 0 before one opens.
    fn get_db(&self) -> i64 {
        self.shared
            .current
            .try_lock()
            .ok()
            .and_then(|current| current.as_ref().map(ConnectionLike::get_db))
            .unwrap_or(0)
    }
}
