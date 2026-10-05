//! Connections to the Redis behind a docket, whatever kind of server it is.

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
        Ok(match url::parse(url)? {
            Target::Standalone(url) => Self::Standalone(Client::open(url)?),
            Target::Cluster(node) => Self::Cluster {
                client: Box::new(
                    ClusterClient::builder([node.as_str()])
                        .connection_timeout(CONNECT_TIMEOUT)
                        .response_timeout(CLUSTER_RESPONSE_TIMEOUT)
                        .build()?,
                ),
                pubsub: Client::open(node)?,
            },
            Target::Sentinel(sentinel) => Self::Sentinel(Mutex::new(sentinel_client(sentinel)?)),
            #[cfg(feature = "memory")]
            Target::Memory(url) => Self::Memory(crate::memory::MemoryServer::open(&url)),
            #[cfg(not(feature = "memory"))]
            Target::Memory(url) => {
                return Err(crate::Error::url(
                    &url,
                    "memory:// needs docket's memory feature",
                ));
            }
        })
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
        Ok(match self {
            Self::Standalone(client) => Connection::Single(
                client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?,
            ),
            Self::Cluster { client, .. } => {
                Connection::Cluster(client.get_async_connection().await?)
            }
            Self::Sentinel(client) => {
                // The master can move after a failover, so each new connection
                // asks the sentinels where it is now.
                let client = client.lock().await.async_get_client().await?;
                Connection::Single(
                    client
                        .get_multiplexed_async_connection_with_config(&config)
                        .await?,
                )
            }
            #[cfg(feature = "memory")]
            Self::Memory(server) => Connection::Single(server.connection().await?),
        })
    }

    /// Opens a connection for subscriptions.
    pub async fn pubsub(&self) -> RedisResult<PubSub> {
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

fn sentinel_client(sentinel: url::SentinelUrl) -> Result<SentinelClient> {
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
    let mut builder =
        SentinelClientBuilder::new(addresses, sentinel.service, SentinelServerType::Master)?
            .set_client_to_redis_db(sentinel.db);
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
    Ok(builder.build()?)
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

    /// A handle on the shared connection.
    pub async fn get(self: &Arc<Self>) -> RedisResult<Handle> {
        let mut current = self.current.lock().await;
        let connection = if let Some(connection) = current.as_ref() {
            connection.clone()
        } else {
            let connection = self.backend.connect().await?;
            *current = Some(connection.clone());
            connection
        };
        Ok(Handle {
            shared: Arc::clone(self),
            connection,
        })
    }

    async fn forget(&self) {
        *self.current.lock().await = None;
    }
}

/// A clone of the shared connection.  When a command fails because the
/// connection is gone, the next [`Shared::get`] opens a new one.
pub(crate) struct Handle {
    shared: Arc<Shared>,
    connection: Connection,
}

impl Handle {
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
            let result = self.connection.req_packed_command(cmd).await;
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
            let result = self
                .connection
                .req_packed_commands(pipeline, offset, count)
                .await;
            self.check(result).await
        })
    }

    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}
