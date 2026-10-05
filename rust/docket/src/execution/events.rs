//! Following a run's state and progress events over pub/sub.

use std::pin::Pin;
use std::task::{Context, Poll};

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::Deserialize;

use super::{State, Status, progress};
use crate::docket::Docket;
use crate::error::{Error, Result};

/// One change to a run.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// The run's state changed.
    State(StateEvent),
    /// The run reported progress.
    Progress(ProgressEvent),
}

/// A run's new state.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct StateEvent {
    /// The new state.
    #[serde(deserialize_with = "state")]
    pub state: State,
    /// When the task is due, for a scheduled or queued task.
    #[serde(default, deserialize_with = "time")]
    pub when: Option<DateTime<Utc>>,
    /// The worker running the task.
    #[serde(default)]
    pub worker: Option<String>,
    /// When the worker started it.
    #[serde(default, deserialize_with = "time")]
    pub started_at: Option<DateTime<Utc>>,
    /// When it ended.
    #[serde(default, deserialize_with = "time")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Why it failed.
    #[serde(default)]
    pub error: Option<String>,
}

/// A run's progress.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct ProgressEvent {
    /// How far the task has come, or `None` before it starts.
    pub current: Option<i64>,
    /// Where it is going.
    pub total: i64,
    /// What it says it is doing.
    #[serde(default)]
    pub message: Option<String>,
    /// When it last reported.
    #[serde(default, deserialize_with = "time")]
    pub updated_at: Option<DateTime<Utc>>,
}

fn state<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<State, D::Error> {
    let text = String::deserialize(deserializer)?;
    State::parse(&text).ok_or_else(|| serde::de::Error::custom(format!("{text} is not a state")))
}

fn time<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.and_then(|text| crate::wire::parse_iso(&text)))
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Payload {
    State(StateEvent),
    Progress(ProgressEvent),
}

/// A run's events.  It ends only when Redis drops the subscription.
pub struct Events {
    inner: Pin<Box<dyn Stream<Item = Result<Event>> + Send>>,
}

impl Stream for Events {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

pub(super) async fn subscribe(docket: &Docket, key: &str) -> Result<Events> {
    let keys = docket.keys();
    let mut pubsub = docket.backend().pubsub().await?;
    pubsub
        .subscribe(&[keys.state(key), keys.progress(key)])
        .await?;

    // Reading after the subscription is confirmed means no change can fall
    // between the read and the first message.
    let mut initial = Vec::new();
    let mut connection = docket.connection().await?;
    let hash: std::collections::HashMap<String, String> =
        redis::AsyncCommands::hgetall(&mut connection, keys.runs(key)).await?;
    if let Some(status) = Status::from_hash(&hash) {
        initial.push(Ok(Event::State(StateEvent {
            state: status.state,
            when: status.when,
            worker: status.worker,
            started_at: status.started_at,
            completed_at: status.completed_at,
            error: status.error,
        })));
    }
    let snapshot = progress::read(docket, key).await?;
    initial.push(Ok(Event::Progress(ProgressEvent {
        current: snapshot.current,
        total: snapshot.total,
        message: snapshot.message,
        updated_at: snapshot.updated_at,
    })));

    let messages = pubsub.into_on_message().map(|message| {
        let payload: String = message.get_payload().map_err(Error::from)?;
        Ok(match serde_json::from_str::<Payload>(&payload)? {
            Payload::State(event) => Event::State(event),
            Payload::Progress(event) => Event::Progress(event),
        })
    });
    Ok(Events {
        inner: Box::pin(stream::iter(initial).chain(messages)),
    })
}
