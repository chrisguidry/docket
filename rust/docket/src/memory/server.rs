//! The in-process server behind `memory://` URLs.
//!
//! Each connection is a `tokio::io::duplex` pipe with a session task on the
//! far end that speaks RESP2, so the rest of docket uses stock redis-rs
//! connections and pub/sub for `memory://` exactly as it does for Redis.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{AsyncConnectionConfig, RedisConnectionInfo, RedisResult};
use tokio::io::DuplexStream;
use tokio::task::AbortHandle;

use crate::clock::Clock;
use crate::memory::engine::store::Store;
use crate::memory::session::Session;

/// How much each direction of a connection buffers before the writer waits.
const PIPE_CAPACITY: usize = 64 * 1024;

/// How often keys with a timeout are swept, so they expire even when
/// nothing reads them.  Redis's own active expiry runs ten times a second.
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// The store for each URL, for the life of the process, as in pydocket: two
/// dockets on the same URL share data, and a docket opened again on a URL
/// finds what the last one left there.  Each URL has its clock too, which a
/// test moves for every docket on the URL.
static STORES: LazyLock<Mutex<HashMap<String, Hosted>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// What a URL keeps for the life of the process.
#[derive(Clone)]
struct Hosted {
    store: Arc<Store>,
    clock: Clock,
}

pub(crate) struct MemoryServer {
    store: Arc<Store>,
    clock: Clock,
    /// Started with the first connection, because opening a server needs no
    /// tokio runtime but sweeping does.
    sweeper: OnceLock<AbortHandle>,
}

impl MemoryServer {
    pub(crate) fn open(url: &str) -> Self {
        let Hosted { store, clock } = STORES
            .lock()
            .entry(url.to_owned())
            .or_insert_with(|| Hosted {
                store: Arc::new(Store::new()),
                clock: Clock::movable(),
            })
            .clone();
        Self {
            store,
            clock,
            sweeper: OnceLock::new(),
        }
    }

    pub(crate) fn clock(&self) -> Clock {
        self.clock.clone()
    }

    pub(crate) async fn connection(&self) -> RedisResult<MultiplexedConnection> {
        // Blocking reads wait on the server for as long as they ask to.
        let config = AsyncConnectionConfig::new().set_response_timeout(None);
        MultiplexedConnection::new_with_config(
            &RedisConnectionInfo::default(),
            self.serve(),
            config,
        )
        .await
        .map(|(connection, driver)| {
            tokio::spawn(driver);
            connection
        })
    }

    pub(crate) async fn pubsub(&self) -> RedisResult<PubSub> {
        PubSub::new(&RedisConnectionInfo::default(), self.serve()).await
    }

    #[cfg(test)]
    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Starts a session and returns the client's end of its pipe.
    fn serve(&self) -> DuplexStream {
        self.sweeper.get_or_init(|| {
            let store = Arc::clone(&self.store);
            tokio::spawn(sweep(store)).abort_handle()
        });
        let (client, server) = tokio::io::duplex(PIPE_CAPACITY);
        let session = Session::new(Arc::clone(&self.store), server);
        // The session ends with an I/O error only when the client is gone.
        tokio::spawn(async move { session.run().await.ok() });
        client
    }
}

impl Drop for MemoryServer {
    fn drop(&mut self) {
        if let Some(sweeper) = self.sweeper.get() {
            sweeper.abort();
        }
    }
}

async fn sweep(store: Arc<Store>) {
    let mut ticks = tokio::time::interval(SWEEP_INTERVAL);
    loop {
        ticks.tick().await;
        store.sweep_expired();
    }
}
