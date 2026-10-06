//! Credentials from a provider, in place of credentials in the URL, for
//! servers whose passwords are tokens that rotate.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use redis::{BasicAuth, Client, RedisResult, StreamingCredentialsProvider};

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

impl Provider {
    /// The credentials the provider gives now.
    async fn current(&self) -> RedisResult<BasicAuth> {
        self.subscribe().next().await.unwrap_or_else(|| {
            Err(redis::RedisError::from((
                redis::ErrorKind::AuthenticationFailed,
                "the credential provider gave no credentials",
            )))
        })
    }

    /// `client` with the provider's current credentials.  redis-rs renews
    /// credentials only on command connections, so a subscription takes the
    /// ones in force when it opens; docket's subscribers open a new one when
    /// Redis drops theirs.
    pub async fn authenticate(&self, client: &Client) -> RedisResult<Client> {
        let auth = self.current().await?;
        let info = client.get_connection_info().clone();
        let settings = info
            .redis_settings()
            .clone()
            .set_username(auth.username())
            .set_password(auth.password());
        Client::open(info.set_redis_settings(settings))
    }
}

/// Whether a client's URL carries a username or a password.
pub(crate) fn has_credentials(client: &Client) -> bool {
    let settings = client.get_connection_info().redis_settings();
    settings.username().is_some() || settings.password().is_some()
}
