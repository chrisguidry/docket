//! A subscription that stays authenticated when the credentials rotate.
//!
//! Some servers close a connection a fixed time after its last AUTH:
//! `ElastiCache` with IAM authentication closes it after 12 hours.  Pub/sub
//! doesn't redeliver, so anything published before docket subscribes again
//! would be lost.
//!
//! So with a credentials provider, a subscription speaks RESP3, under which
//! a subscribed connection takes any command, and it sends AUTH on its own
//! connection each time the backend passes on new credentials.  The
//! connection stays open and subscribed throughout, so no message is lost,
//! repeated, or reordered.  The backend checks each new set against the
//! server first, and skips and logs a set the server refuses, so the
//! subscription never sends one.  When the server refuses an AUTH anyway,
//! the connection keeps the credentials it had and goes on delivering, and
//! the next rotation tries again.
//!
//! redis-rs can send these AUTHs itself, but its renewal task stops for good
//! after one AUTH fails, for example on a timeout, so docket sends them.
//! Without a provider there is nothing to renew, and a subscription uses
//! redis-rs's `PubSub`.

use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt, stream};
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{
    AsyncConnectionConfig, Client, Msg, PushInfo, PushKind, RedisResult,
    StreamingCredentialsProvider,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{Checked, Settings, credentials};

#[cfg(test)]
mod tests;

/// What a subscription listens to.
#[derive(Clone, Debug)]
pub(crate) enum Topics {
    Channels(Vec<String>),
    Pattern(String),
}

impl Topics {
    pub(super) async fn subscribe_pubsub(&self, pubsub: &mut PubSub) -> RedisResult<()> {
        match self {
            Topics::Channels(channels) => pubsub.subscribe(channels).await,
            Topics::Pattern(pattern) => pubsub.psubscribe(pattern).await,
        }
    }

    async fn subscribe_resp3(&self, connection: &mut MultiplexedConnection) -> RedisResult<()> {
        match self {
            Topics::Channels(channels) => connection.subscribe(channels).await,
            Topics::Pattern(pattern) => connection.psubscribe(pattern).await,
        }
    }
}

/// The messages published to some topics.  It ends only when Redis drops
/// the connection.
pub(crate) struct Subscription {
    messages: Pin<Box<dyn Stream<Item = Msg> + Send>>,
    /// `None` without a credentials provider.  The subscription holds it so
    /// that dropping the subscription closes the connection; only tests
    /// read it.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "held until the subscription drops")
    )]
    renewal: Option<Renewal>,
}

/// The connection of a subscription that follows a credentials provider,
/// and the task that sends AUTH on it.
struct Renewal {
    /// Keeps the connection open after the provider's stream ends and the
    /// task with it.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "held until the subscription drops")
    )]
    connection: MultiplexedConnection,
    task: JoinHandle<()>,
}

impl Drop for Renewal {
    /// The task holds a clone of the connection, and redis-rs closes a
    /// connection only once every clone is gone, so a subscription dropped
    /// without this would stay subscribed.
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A RESP3 connection to the server, opened with a provider's credentials
/// and not yet subscribed.
pub(super) struct Resp3 {
    connection: MultiplexedConnection,
    pushes: mpsc::UnboundedReceiver<PushInfo>,
    /// The later credentials that the backend passes on.
    rotations: credentials::Credentials,
}

impl Resp3 {
    /// Opens a RESP3 connection to `client` with the provider's current
    /// credentials.  It has no connection timeout of its own, because the
    /// caller bounds the whole open, server lookup included, with one.  Its
    /// commands wait as long as the response timeout.
    pub(super) async fn open(
        client: &Client,
        provider: &Checked,
        settings: &Settings,
    ) -> RedisResult<Self> {
        let mut rotations = provider.subscribe();
        let auth = credentials::next(&mut rotations).await?;
        let (sender, pushes) = mpsc::unbounded_channel();
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(settings.response_timeout)
            .set_push_sender(sender);
        credentials::resp3(client, &auth)
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map(|connection| Self {
                connection,
                pushes,
                rotations,
            })
    }
}

impl Stream for Subscription {
    type Item = Msg;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Msg>> {
        self.messages.as_mut().poll_next(cx)
    }
}

impl Subscription {
    /// The messages on `pubsub`, which is already subscribed.
    pub(super) fn plain(pubsub: PubSub) -> Self {
        Self {
            messages: Box::pin(pubsub.into_on_message()),
            renewal: None,
        }
    }

    /// Subscribes `opened` to `topics`, and sends AUTH on it each time the
    /// provider's credentials rotate.
    pub(super) async fn renewed(opened: Resp3, topics: &Topics) -> RedisResult<Self> {
        let Resp3 {
            mut connection,
            mut pushes,
            rotations,
        } = opened;
        topics.subscribe_resp3(&mut connection).await?;
        let task = tokio::spawn(renew(connection.clone(), rotations));
        // redis-rs sends a Disconnection push when the server drops the
        // connection, then drops its sender, which ends the stream.
        let messages = stream::poll_fn(move |cx| pushes.poll_recv(cx))
            .take_while(|push: &PushInfo| ready(push.kind != PushKind::Disconnection))
            .filter_map(|push| ready(Msg::from_push_info(push)));
        Ok(Self {
            messages: Box::pin(messages),
            renewal: Some(Renewal { connection, task }),
        })
    }

    /// The subscribed connection, when the subscription follows a provider.
    #[cfg(test)]
    fn connection(&self) -> Option<MultiplexedConnection> {
        self.renewal
            .as_ref()
            .map(|renewal| renewal.connection.clone())
    }
}

/// Sends AUTH on `connection` with each new set of credentials that the
/// backend passes on.  The backend never passes on an error after the
/// first set, so the loop runs until the subscription drops or the backend
/// and every connection with it are gone.  An AUTH the server
/// refuses leaves the connection with the credentials it had, so the task
/// logs it and waits for the next set.
async fn renew(mut connection: MultiplexedConnection, mut rotations: credentials::Credentials) {
    while let Some(Ok(auth)) = rotations.next().await {
        let authenticated: RedisResult<()> = redis::cmd("AUTH")
            .arg(auth.username())
            .arg(auth.password())
            .query_async(&mut connection)
            .await;
        if let Err(error) = authenticated {
            tracing::warn!(%error, "re-authenticating a subscription failed");
        }
    }
}
