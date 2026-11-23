//! Credentials from a provider, in place of credentials in the URL, for
//! servers whose passwords are tokens that rotate.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use redis::{BasicAuth, Client, RedisResult, StreamingCredentialsProvider};

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

/// `client` with `auth` in place of the credentials in its URL.  redis-rs
/// renews credentials only on command connections, so a subscription takes
/// the ones it opens with, and moves to a new connection when they rotate.
pub(crate) fn authenticate(client: &Client, auth: &BasicAuth) -> RedisResult<Client> {
    let info = client.get_connection_info().clone();
    let settings = info
        .redis_settings()
        .clone()
        .set_username(auth.username())
        .set_password(auth.password());
    Client::open(info.set_redis_settings(settings))
}

/// Whether a client's URL carries a username or a password.
pub(crate) fn has_credentials(client: &Client) -> bool {
    let settings = client.get_connection_info().redis_settings();
    settings.username().is_some() || settings.password().is_some()
}
