//! Credentials from a provider, in place of credentials in the URL, for
//! servers whose passwords are tokens that rotate.
//!
//! A backend checks each new set of credentials from the provider before
//! any of its connections sees it: it authenticates once with the set on a
//! connection of its own.  A set the server refuses is logged and skipped,
//! so the connections keep the credentials they have, and the next set is
//! checked again.  redis-rs's renewal task stops for good after one AUTH
//! fails, so a refused set that reached it would leave every command
//! connection on the old credentials until each one drops.

use std::future::ready;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::sentinel::SentinelClient;
use redis::{
    AsyncConnectionConfig, BasicAuth, Client, ErrorKind, ProtocolVersion, RedisError, RedisResult,
    StreamingCredentialsProvider,
};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::{CancellationToken, DropGuard};

#[cfg(test)]
mod tests;

/// The credentials a provider gives, the first now and each later one when
/// they rotate.
pub(crate) type Credentials = Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>>;

/// A credentials provider that several connections share.
#[derive(Clone)]
pub(crate) struct Provider(pub Arc<dyn StreamingCredentialsProvider>);

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Provider(..)")
    }
}

impl StreamingCredentialsProvider for Provider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        self.0.subscribe()
    }
}

/// The server that a backend checks new credentials against: its one
/// server, the cluster node its subscriptions use, or the primary that the
/// sentinels name now.
#[derive(Clone)]
pub(crate) enum Server {
    Direct(Box<Client>),
    Sentinel(Arc<Mutex<SentinelClient>>),
}

impl Server {
    async fn client(&self) -> RedisResult<Client> {
        match self {
            Self::Direct(client) => Ok(Client::clone(client)),
            Self::Sentinel(sentinel) => sentinel.lock().await.async_get_client().await,
        }
    }
}

/// A provider's credentials as a backend's connections take them: the first
/// set as the provider gives it, and each later set once the server accepts
/// it.
///
/// One task follows the provider for the whole backend and writes each set
/// it passes into a `watch` channel, which every connection reads.  So the
/// server sees one check for each rotation, however many connections the
/// backend holds.  A provider can also wait for each subscriber to take one
/// set before it sends the next: redis-rs's Entra ID provider gives each
/// subscriber a channel that holds one.  The task is the provider's only
/// subscriber and always reads, so a connection that is slow to take a set
/// never holds up the refresh for the others.
#[derive(Clone)]
pub(crate) struct Checked(Arc<Checker>);

struct Checker {
    provider: Provider,
    server: Server,
    timeout: Duration,
    /// `None` until the provider gives its first set.
    latest: watch::Sender<Option<RedisResult<BasicAuth>>>,
    /// The task starts with the first subscriber, because a backend can be
    /// opened outside a Tokio runtime, and a task can only start inside one.
    started: Once,
    stop: CancellationToken,
}

impl Checked {
    /// Checks each new set from `provider` against `server`, giving up on a
    /// check after `timeout`.
    pub(crate) fn new(provider: Provider, server: Server, timeout: Duration) -> Self {
        Self(Arc::new(Checker {
            provider,
            server,
            timeout,
            latest: watch::channel(None).0,
            started: Once::new(),
            stop: CancellationToken::new(),
        }))
    }

    /// A guard that ends the task following the provider when it drops.  A
    /// connection that outlives its backend keeps the credentials it has.
    pub(crate) fn guard(&self) -> DropGuard {
        self.0.stop.clone().drop_guard()
    }

    /// The number of streams reading the checked credentials.
    #[cfg(test)]
    pub(crate) fn followers(&self) -> usize {
        self.0.latest.receiver_count()
    }

    fn start(&self) {
        let checker = &self.0;
        checker.started.call_once(|| {
            let following = follow(
                checker.provider.subscribe(),
                checker.server.clone(),
                checker.timeout,
                checker.latest.clone(),
            );
            let stop = checker.stop.clone();
            tokio::spawn(async move { stop.run_until_cancelled(following).await });
        });
    }
}

impl StreamingCredentialsProvider for Checked {
    /// The set in force now, once the provider has given one, then each set
    /// the task passes.
    fn subscribe(&self) -> Credentials {
        self.start();
        let mut receiver = self.0.latest.subscribe();
        receiver.mark_changed();
        let sets = stream::unfold(receiver, |mut receiver| async move {
            receiver.changed().await.ok().map(|()| {
                let latest = receiver.borrow_and_update().clone();
                (latest, receiver)
            })
        });
        Box::pin(sets.filter_map(ready))
    }
}

/// Passes the provider's first set on as it comes, so that a provider that
/// fails from the start, or ends without a set, fails the connections, and
/// each later set once `server` accepts it.
async fn follow(
    mut credentials: Credentials,
    server: Server,
    timeout: Duration,
    latest: watch::Sender<Option<RedisResult<BasicAuth>>>,
) {
    latest.send_replace(Some(next(&mut credentials).await));
    while let Some(next) = credentials.next().await {
        match next {
            Ok(auth) => {
                if accepts(&server, &auth, timeout).await {
                    latest.send_replace(Some(Ok(auth)));
                }
            }
            Err(error) => tracing::warn!(%error, "the credential provider failed to refresh"),
        }
    }
}

/// Whether `server` takes `auth`, from one short-lived connection that
/// authenticates with it.  A connection kept between rotations would need
/// its own reconnects, and rotations come minutes or hours apart.  Only a
/// refusal counts against the set: when the check can't reach the server
/// within `timeout`, the connections would fail the same way, so the set
/// goes on to them.
async fn accepts(server: &Server, auth: &BasicAuth, timeout: Duration) -> bool {
    let checking = async {
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(None);
        as_user(&server.client().await?, auth)
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map(drop)
    };
    match tokio::time::timeout(timeout, checking).await {
        Ok(Err(error)) if refused(&error) => {
            tracing::warn!(
                %error,
                "the server refused the credential provider's new credentials, \
                 so the connections keep the ones they have"
            );
            false
        }
        _ => true,
    }
}

/// Whether the server refused the credentials themselves: redis-rs reports
/// a refused AUTH as an authentication failure, and a refused HELLO as the
/// server's `WRONGPASS`.
fn refused(error: &RedisError) -> bool {
    error.kind() == ErrorKind::AuthenticationFailed || error.code() == Some("WRONGPASS")
}

/// The next credentials from `credentials`, which for a stream that has
/// given none yet are the ones in force now.
pub(crate) async fn next(credentials: &mut Credentials) -> RedisResult<BasicAuth> {
    credentials.next().await.unwrap_or_else(|| {
        Err(redis::RedisError::from((
            redis::ErrorKind::AuthenticationFailed,
            "the credential provider gave no credentials",
        )))
    })
}

/// `client` with `auth` in place of the credentials in its URL.
fn as_user(client: &Client, auth: &BasicAuth) -> Client {
    let info = client.get_connection_info().clone();
    let settings = info
        .redis_settings()
        .clone()
        .set_username(auth.username())
        .set_password(auth.password());
    Client::open(info.set_redis_settings(settings))
        .expect("a client opens from a ConnectionInfo without checking it")
}

/// `client` speaking RESP3, with `auth` in place of the credentials in its
/// URL.  Under RESP3 a subscribed connection takes any command, so a
/// subscription can send AUTH on its own connection when the credentials
/// rotate.
pub(crate) fn resp3(client: &Client, auth: &BasicAuth) -> Client {
    let info = client.get_connection_info().clone();
    let settings = info
        .redis_settings()
        .clone()
        .set_protocol(ProtocolVersion::RESP3);
    let client = Client::open(info.set_redis_settings(settings))
        .expect("a client opens from a ConnectionInfo without checking it");
    as_user(&client, auth)
}

/// Whether a client's URL carries a username or a password.
pub(crate) fn has_credentials(client: &Client) -> bool {
    let settings = client.get_connection_info().redis_settings();
    settings.username().is_some() || settings.password().is_some()
}
