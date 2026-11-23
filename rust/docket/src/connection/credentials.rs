//! Credentials from a provider, in place of credentials in the URL, for
//! servers whose passwords are tokens that rotate.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use redis::{BasicAuth, Client, RedisResult, StreamingCredentialsProvider};
use tokio::sync::watch;

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

/// The latest credentials from `credentials`, starting with `first`, read by
/// a task of its own until the stream ends or the receiver is dropped.
///
/// A provider can wait for each subscriber to take one set of credentials
/// before it sends the next: redis-rs's Entra ID provider gives each
/// subscriber a channel that holds one.  A subscription reads only when its
/// owner does, so without this task, one that nobody reads would hold up
/// every refresh, and the command connections' credentials would expire.
pub(crate) fn follow(first: BasicAuth, mut credentials: Credentials) -> watch::Receiver<BasicAuth> {
    let (sender, receiver) = watch::channel(first);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = sender.closed() => return,
                next = credentials.next() => match next {
                    Some(Ok(auth)) => {
                        sender.send_replace(auth);
                    }
                    Some(Err(error)) => {
                        tracing::warn!(%error, "the credential provider failed to refresh");
                    }
                    None => return,
                },
            }
        }
    });
    receiver
}

/// `client` with `auth` in place of the credentials in its URL.  redis-rs
/// renews credentials only on command connections, so a subscription takes
/// the ones it opens with, and moves to a new connection when they rotate.
pub(crate) fn authenticate(client: &Client, auth: &BasicAuth) -> Client {
    let info = client.get_connection_info().clone();
    let settings = info
        .redis_settings()
        .clone()
        .set_username(auth.username())
        .set_password(auth.password());
    Client::open(info.set_redis_settings(settings))
        .expect("a client opens from a ConnectionInfo without checking it")
}

/// Whether a client's URL carries a username or a password.
pub(crate) fn has_credentials(client: &Client) -> bool {
    let settings = client.get_connection_info().redis_settings();
    settings.username().is_some() || settings.password().is_some()
}
