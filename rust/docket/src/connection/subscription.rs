//! A subscription that moves to a new connection when the credentials rotate.
//!
//! redis-rs renews credentials only on command connections, so a subscribed
//! connection stays authenticated with the credentials it opened with.  Some
//! servers close a connection a fixed time after its last AUTH: `ElastiCache`
//! with IAM authentication closes it after 12 hours.  Pub/sub doesn't
//! redeliver, so anything published before docket subscribes again is lost.
//!
//! So each time the provider gives new credentials, the subscription opens a
//! second connection with them and waits for the server to confirm its
//! subscription.  Only then does it unsubscribe the first connection and take
//! what that connection received before it was unsubscribed.  The two
//! connections are subscribed at the same time for one round trip, so a
//! message published then arrives on both, and the subscription yields it
//! twice.  No message is lost.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{FutureExt, Stream, StreamExt, stream};
use redis::aio::{PubSub, PubSubSink, PubSubStream};
use redis::{BasicAuth, Msg, RedisResult, StreamingCredentialsProvider};

use super::credentials::{self, Credentials};
use super::{Backend, within};

#[cfg(test)]
mod tests;

/// What a subscription listens to.
#[derive(Clone, Debug)]
pub(crate) enum Topics {
    Channels(Vec<String>),
    Pattern(String),
}

impl Topics {
    pub(super) async fn subscribe(&self, pubsub: &mut PubSub) -> RedisResult<()> {
        match self {
            Topics::Channels(channels) => pubsub.subscribe(channels).await,
            Topics::Pattern(pattern) => pubsub.psubscribe(pattern).await,
        }
    }

    async fn unsubscribe(&self, sink: &mut PubSubSink) -> RedisResult<()> {
        match self {
            Topics::Channels(channels) => sink.unsubscribe(channels).await,
            Topics::Pattern(pattern) => sink.punsubscribe(pattern).await,
        }
    }
}

/// The messages published to some topics.  It ends only when Redis drops
/// the connection.
pub(crate) struct Subscription {
    inner: Pin<Box<dyn Stream<Item = Msg> + Send>>,
}

impl Stream for Subscription {
    type Item = Msg;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Msg>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Subscription {
    pub(super) async fn open(backend: Arc<Backend>, topics: Topics) -> RedisResult<Self> {
        let mut rotations = backend
            .credentials
            .as_ref()
            .map(StreamingCredentialsProvider::subscribe);
        let auth = match &mut rotations {
            Some(rotations) => Some(credentials::next(rotations).await?),
            None => None,
        };
        let (sink, messages) = backend.subscribed(&topics, auth.as_ref()).await?;
        let state = State {
            backend,
            topics,
            rotations,
            sink,
            messages,
            received: VecDeque::new(),
        };
        Ok(Self {
            inner: Box::pin(stream::unfold(state, State::next)),
        })
    }
}

struct State {
    backend: Arc<Backend>,
    topics: Topics,
    /// The provider's later credentials, or `None` without a provider or
    /// once its stream ends.
    rotations: Option<Credentials>,
    sink: PubSubSink,
    messages: PubSubStream,
    /// What the previous connection received before it was unsubscribed,
    /// which comes before anything on this one.
    received: VecDeque<Msg>,
}

enum Next {
    Message(Option<Msg>),
    Rotation(Option<RedisResult<BasicAuth>>),
}

impl State {
    async fn next(mut self) -> Option<(Msg, Self)> {
        loop {
            if let Some(message) = self.received.pop_front() {
                return Some((message, self));
            }
            let rotations = &mut self.rotations;
            let next = tokio::select! {
                message = self.messages.next() => Next::Message(message),
                auth = async {
                    match rotations {
                        Some(rotations) => rotations.next().await,
                        None => std::future::pending().await,
                    }
                } => Next::Rotation(auth),
            };
            match next {
                Next::Message(message) => return message.map(|message| (message, self)),
                Next::Rotation(Some(Ok(auth))) => {
                    if let Err(error) = self.move_to(&auth).await {
                        tracing::warn!(%error, "moving a subscription to rotated credentials failed");
                    }
                }
                Next::Rotation(Some(Err(error))) => {
                    tracing::warn!(%error, "the credential provider failed to refresh");
                }
                Next::Rotation(None) => self.rotations = None,
            }
        }
    }

    /// Subscribes a connection with `auth`, then unsubscribes the current one.
    async fn move_to(&mut self, auth: &BasicAuth) -> RedisResult<()> {
        let (sink, messages) = self.backend.subscribed(&self.topics, Some(auth)).await?;
        let mut old_sink = std::mem::replace(&mut self.sink, sink);
        let mut old_messages = std::mem::replace(&mut self.messages, messages);
        // redis-rs passes on each message that arrives before the server
        // confirms the unsubscribe, so once the confirmation is in, the old
        // stream already holds everything the old connection will deliver.
        let unsubscribed = within(
            self.backend.settings.response_timeout,
            self.topics.unsubscribe(&mut old_sink),
        )
        .await;
        while let Some(Some(message)) = old_messages.next().now_or_never() {
            self.received.push_back(message);
        }
        unsubscribed
    }
}
