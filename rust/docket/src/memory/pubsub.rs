//! One connection's subscriptions, and the messages they let through.
//!
//! The store sends every published message to every subscriber's receiver,
//! so each connection keeps its own channels and patterns to filter by.  It
//! also registers them with the store, which counts them for PUBLISH.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::broadcast;

use crate::memory::engine::store::{PubSubMessage, Store};
use crate::memory::resp::Reply;

/// What a SUBSCRIBE-family command works on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Channel,
    Pattern,
}

pub(crate) struct Subscriptions {
    store: Arc<Store>,
    subscriber: Option<Subscriber>,
}

struct Subscriber {
    id: u64,
    messages: broadcast::Receiver<PubSubMessage>,
    channels: HashSet<Bytes>,
    patterns: HashSet<Bytes>,
}

impl Subscriptions {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            subscriber: None,
        }
    }

    /// A connection with any subscription accepts only the SUBSCRIBE family
    /// and PING.
    pub(crate) fn is_active(&self) -> bool {
        self.subscriber.as_ref().is_some_and(|subscriber| {
            !subscriber.channels.is_empty() || !subscriber.patterns.is_empty()
        })
    }

    /// The receiver is made before the first registration, so no message
    /// published after a subscription is confirmed can be missed.
    fn subscriber(&mut self) -> &mut Subscriber {
        self.subscriber.get_or_insert_with(|| {
            let (id, messages) = self.store.new_subscriber();
            Subscriber {
                id,
                messages,
                channels: HashSet::new(),
                patterns: HashSet::new(),
            }
        })
    }

    /// One confirmation per name, as Redis sends them.
    pub(crate) fn subscribe(&mut self, kind: Kind, names: Vec<Bytes>) -> Vec<Reply> {
        let store = Arc::clone(&self.store);
        let subscriber = self.subscriber();
        let (label, counts) = match kind {
            Kind::Channel => {
                subscriber.channels.extend(names.iter().cloned());
                ("subscribe", store.subscribe(subscriber.id, names))
            }
            Kind::Pattern => {
                subscriber.patterns.extend(names.iter().cloned());
                ("psubscribe", store.psubscribe(subscriber.id, names))
            }
        };
        confirmations(label, counts)
    }

    /// No names means every name of that kind.  With nothing to remove,
    /// Redis still confirms once, with a nil name.
    pub(crate) fn unsubscribe(&mut self, kind: Kind, names: Vec<Bytes>) -> Vec<Reply> {
        let store = Arc::clone(&self.store);
        let subscriber = self.subscriber();
        let (label, counts) = match kind {
            Kind::Channel => {
                let counts = store.unsubscribe(subscriber.id, names);
                for (name, _) in &counts {
                    subscriber.channels.remove(name);
                }
                ("unsubscribe", counts)
            }
            Kind::Pattern => {
                let counts = store.punsubscribe(subscriber.id, names);
                for (name, _) in &counts {
                    subscriber.patterns.remove(name);
                }
                ("punsubscribe", counts)
            }
        };
        if counts.is_empty() {
            let total = subscriber.channels.len() + subscriber.patterns.len();
            return vec![Reply::Array(vec![
                Reply::bulk(label),
                Reply::Nil,
                Reply::integer(total),
            ])];
        }
        confirmations(label, counts)
    }

    /// Waits for the next message this connection subscribes to.  Without a
    /// subscriber, it waits forever.
    pub(crate) async fn next_message(&mut self) -> Reply {
        let Some(subscriber) = self.subscriber.as_mut() else {
            return std::future::pending().await;
        };
        loop {
            // A lagging receiver skips the messages it lost and carries on.
            // The store holds the sender, so the channel never closes.
            if let Ok(message) = subscriber.messages.recv().await
                && let Some(reply) = subscriber.deliver(message)
            {
                return reply;
            }
        }
    }
}

impl Subscriber {
    fn deliver(&self, message: PubSubMessage) -> Option<Reply> {
        match message.pattern {
            None if self.channels.contains(&message.channel) => Some(Reply::bulks([
                Bytes::from("message"),
                message.channel,
                message.data,
            ])),
            Some(pattern) if self.patterns.contains(&pattern) => Some(Reply::bulks([
                Bytes::from("pmessage"),
                pattern,
                message.channel,
                message.data,
            ])),
            _ => None,
        }
    }
}

fn confirmations(label: &'static str, counts: Vec<(Bytes, i64)>) -> Vec<Reply> {
    counts
        .into_iter()
        .map(|(name, total)| {
            Reply::Array(vec![
                Reply::bulk(label),
                Reply::Bulk(name),
                Reply::Integer(total),
            ])
        })
        .collect()
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        if let Some(subscriber) = &self.subscriber {
            self.store.stop_subscriber_listener(subscriber.id);
        }
    }
}
