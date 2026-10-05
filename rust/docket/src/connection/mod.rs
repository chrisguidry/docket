//! Connections to the Redis behind a docket, whatever kind of server it is.

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
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, Pipeline, RedisFuture, RedisResult,
    TlsMode, Value,
};
use tokio::sync::Mutex;

use crate::error::Result;
use url::Target;

/// A connect that stalls, for example on dropped SYN packets, has no server
/// work to wait for, so it fails after this long.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Blocking reads such as `XREADGROUP BLOCK` wait for the server, so
/// connections have no read timeout.  A cluster connection needs some value,
/// so it gets one far longer than any block docket asks for.
const CLUSTER_RESPONSE_TIMEOUT: Duration = Duration::from_hours(24);

/// How to reach the Redis behind a docket.
pub(crate) enum Backend {
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
    pub fn open(url: &str) -> Result<Self> {
        let backend = match url::parse(url)? {
            Target::Standalone(url) => Client::open(url).map(Self::Standalone),
            Target::Cluster(node) => {
                // Both clients read the same node URL, so they fail together.
                ClusterClient::builder([node.as_str()])
                    .connection_timeout(CONNECT_TIMEOUT)
                    .response_timeout(CLUSTER_RESPONSE_TIMEOUT)
                    .build()
                    .and_then(|client| {
                        Client::open(node).map(|pubsub| Self::Cluster {
                            client: Box::new(client),
                            pubsub,
                        })
                    })
            }
            Target::Sentinel(sentinel) => {
                sentinel_client(sentinel).map(|client| Self::Sentinel(Mutex::new(client)))
            }
            #[cfg(feature = "memory")]
            Target::Memory(url) => Ok(Self::Memory(crate::memory::MemoryServer::open(&url))),
            #[cfg(not(feature = "memory"))]
            Target::Memory(url) => {
                return Err(crate::Error::url(
                    &url,
                    "memory:// needs docket's memory feature",
                ));
            }
        };
        Ok(backend?)
    }

    /// The prefix of every key in the docket.  On a cluster it is a hash tag,
    /// so that every key of the docket lands in one slot and the Lua scripts
    /// can touch them together.
    pub fn prefix(&self, name: &str) -> String {
        match self {
            Self::Cluster { .. } => format!("{{{name}}}"),
            _ => name.to_owned(),
        }
    }

    /// Opens a new connection.  Blocking reads hold their connection for the
    /// whole block, so each loop that blocks opens its own.
    pub async fn connect(&self) -> RedisResult<Connection> {
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(CONNECT_TIMEOUT))
            .set_response_timeout(None);
        match self {
            Self::Standalone(client) => client
                .get_multiplexed_async_connection_with_config(&config)
                .await
                .map(Connection::Single),
            Self::Cluster { client, .. } => {
                client.get_async_connection().await.map(Connection::Cluster)
            }
            Self::Sentinel(client) => {
                // The master can move after a failover, so each new connection
                // asks the sentinels where it is now.
                let client = client.lock().await.async_get_client().await?;
                client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .map(Connection::Single)
            }
            #[cfg(feature = "memory")]
            Self::Memory(server) => server.connection().await.map(Connection::Single),
        }
    }

    /// Opens a connection subscribed to `channels`.
    pub async fn subscribe(&self, channels: &[String]) -> RedisResult<PubSub> {
        let mut pubsub = self.pubsub().await?;
        pubsub.subscribe(channels).await.map(|()| pubsub)
    }

    /// Opens a connection subscribed to the channels that match `pattern`.
    pub async fn psubscribe(&self, pattern: String) -> RedisResult<PubSub> {
        let mut pubsub = self.pubsub().await?;
        pubsub.psubscribe(pattern).await.map(|()| pubsub)
    }

    /// Opens a connection for subscriptions.
    async fn pubsub(&self) -> RedisResult<PubSub> {
        match self {
            Self::Standalone(client) | Self::Cluster { pubsub: client, .. } => {
                client.get_async_pubsub().await
            }
            Self::Sentinel(client) => {
                let client = client.lock().await.async_get_client().await?;
                client.get_async_pubsub().await
            }
            #[cfg(feature = "memory")]
            Self::Memory(server) => server.pubsub().await,
        }
    }

    /// Whether this backend is a cluster, whose nodes cannot be trusted to
    /// keep a script loaded for pipelined `EVALSHA`.
    pub fn is_cluster(&self) -> bool {
        matches!(self, Self::Cluster { .. })
    }
}

fn sentinel_client(sentinel: url::SentinelUrl) -> RedisResult<SentinelClient> {
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
            let mut builder = builder.set_client_to_redis_db(sentinel.db);
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
