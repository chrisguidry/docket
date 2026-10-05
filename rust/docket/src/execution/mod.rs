//! One scheduled run of a task, as a producer sees it.

mod events;
mod message;
mod progress;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use redis::{AsyncCommands, Value};
use serde::de::DeserializeOwned;

use crate::docket::Docket;
use crate::error::{Error, Result};
use crate::wire::parse_iso;

pub use events::{Event, Events, ProgressEvent, StateEvent};
pub(crate) use message::Message;
pub use progress::{Progress, ProgressSnapshot};

/// What happened when a task was added.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Disposition {
    /// The task is in the docket.
    Scheduled,
    /// A task with the same key is already scheduled or running, so nothing
    /// changed.
    AlreadyScheduled,
    /// A strike blocks the task, so it was not added.
    Struck,
    /// A newer copy of the task took its key first.
    Superseded,
    /// Redis refused the add, in a batch where other adds went through.
    Failed(String),
}

impl Disposition {
    pub(crate) fn from_reply(reply: &str) -> Self {
        match reply {
            "EXISTS" => Self::AlreadyScheduled,
            "SUPERSEDED" => Self::Superseded,
            _ => Self::Scheduled,
        }
    }

    pub(crate) fn from_value(value: &Value) -> Self {
        match value {
            Value::SimpleString(reply) => Self::from_reply(reply),
            Value::BulkString(reply) => Self::from_reply(&String::from_utf8_lossy(reply)),
            _ => Self::Scheduled,
        }
    }
}

/// Where a task is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum State {
    /// Waiting for its time.
    Scheduled,
    /// Due, and waiting for a worker.
    Queued,
    /// A worker is running it.
    Running,
    /// It finished.
    Completed,
    /// It failed, and will not be retried.
    Failed,
    /// It was cancelled.
    Cancelled,
}

impl State {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "scheduled" => Self::Scheduled,
            "queued" => Self::Queued,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the task is done for good.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// What the docket knows about a run right now.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Status {
    /// Where the task is in its life.
    pub state: State,
    /// When the task is or was due.
    pub when: Option<DateTime<Utc>>,
    /// The worker that ran or runs it.
    pub worker: Option<String>,
    /// When a worker started it.
    pub started_at: Option<DateTime<Utc>>,
    /// When it ended.
    pub completed_at: Option<DateTime<Utc>>,
    /// Why it failed.
    pub error: Option<String>,
}

impl Status {
    pub(crate) fn from_hash(hash: &HashMap<String, String>) -> Option<Self> {
        let time = |field: &str| hash.get(field).and_then(|text| parse_iso(text));
        Some(Self {
            state: State::parse(hash.get("state")?)?,
            when: hash
                .get("when")
                .and_then(|text| text.parse::<f64>().ok())
                .and_then(seconds_to_time),
            worker: hash.get("worker").cloned(),
            started_at: time("started_at"),
            completed_at: time("completed_at"),
            error: hash.get("error").cloned(),
        })
    }
}

fn seconds_to_time(seconds: f64) -> Option<DateTime<Utc>> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "microseconds since the epoch fit in i64"
    )]
    DateTime::from_timestamp_micros((seconds * 1_000_000.0).round() as i64)
}

/// One scheduled run of a task.  `O` is the type of the task's output.
pub struct Execution<O = serde_json::Value> {
    docket: Docket,
    key: String,
    function: String,
    when: DateTime<Utc>,
    disposition: Disposition,
    output: PhantomData<fn() -> O>,
}

impl<O> Execution<O> {
    pub(crate) fn new(docket: Docket, message: &Message, disposition: Disposition) -> Self {
        Self {
            docket,
            key: message.key.clone(),
            function: message.function.clone(),
            when: message.when,
            disposition,
            output: PhantomData,
        }
    }

    /// An execution read back from the docket, which was added earlier.
    pub(crate) fn found(
        docket: Docket,
        key: String,
        function: String,
        when: DateTime<Utc>,
    ) -> Self {
        Self {
            docket,
            key,
            function,
            when,
            disposition: Disposition::Scheduled,
            output: PhantomData,
        }
    }

    /// The task's key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The task's name.
    #[must_use]
    pub fn function(&self) -> &str {
        &self.function
    }

    /// When the task was asked to run.
    #[must_use]
    pub fn when(&self) -> DateTime<Utc> {
        self.when
    }

    /// What happened when the task was added.
    #[must_use]
    pub fn disposition(&self) -> &Disposition {
        &self.disposition
    }

    /// The run's current status, or `None` when the docket knows nothing of
    /// it, for example after its state expired.
    pub async fn status(&self) -> Result<Option<Status>> {
        status(&self.docket, &self.key).await
    }

    /// The run's progress.
    pub async fn progress(&self) -> Result<ProgressSnapshot> {
        let mut connection = self.docket.handle();
        progress::read(&mut connection, &self.docket, &self.key).await
    }

    /// Follows the run's state and progress events as they happen.  The
    /// stream starts with the current state and progress, and it can repeat
    /// an event.
    pub async fn subscribe(&self) -> Result<Events> {
        events::subscribe(&self.docket, &self.key).await
    }
}

impl<O: DeserializeOwned> Execution<O> {
    /// Waits for the task to end, and returns its output.  A failed task
    /// returns [`Error::TaskFailed`], and a cancelled one
    /// [`Error::TaskCancelled`].  Bound the wait with `tokio::time::timeout`.
    pub async fn result(&self) -> Result<O> {
        output(&self.docket, &self.key)
            .await
            .and_then(|output| serde_json::from_value(output).map_err(Error::from))
    }
}

// The work of an execution lives in functions of the docket and key, so it
// compiles once rather than once for each output type.

async fn status(docket: &Docket, key: &str) -> Result<Option<Status>> {
    let hash: HashMap<String, String> = docket.handle().hgetall(docket.keys().runs(key)).await?;
    Ok(Status::from_hash(&hash))
}

/// Waits for the run of `key` to end, and returns its output.
async fn output(docket: &Docket, key: &str) -> Result<serde_json::Value> {
    let mut events = events::subscribe(docket, key).await?;
    loop {
        if let Some(status) = status(docket, key).await?
            && status.state.is_terminal()
        {
            return outcome(docket, key, &status).await;
        }
        // Wait for the next state event before reading the status again,
        // so that the final read sees the terminal state and its fields.
        loop {
            match events.next().await {
                Some(Ok(Event::State(event))) if event.state.is_terminal() => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error),
                None => break,
            }
        }
    }
}

async fn outcome(docket: &Docket, key: &str, status: &Status) -> Result<serde_json::Value> {
    match status.state {
        State::Cancelled => Err(Error::TaskCancelled {
            key: key.to_owned(),
        }),
        State::Failed => Err(Error::TaskFailed {
            key: key.to_owned(),
            message: status
                .error
                .clone()
                .unwrap_or_else(|| "the task failed".to_owned()),
        }),
        _ => {
            let stored: Option<String> = docket.handle().get(docket.keys().result(key)).await?;
            match stored {
                Some(stored) => Ok(serde_json::from_str::<StoredResult>(&stored)?.ok),
                None => Ok(serde_json::Value::Null),
            }
        }
    }
}

/// What a worker stores for a task that completed.
#[derive(serde::Deserialize)]
struct StoredResult {
    ok: serde_json::Value,
}

impl<O> std::fmt::Debug for Execution<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Execution")
            .field("key", &self.key)
            .field("function", &self.function)
            .field("when", &self.when)
            .field("disposition", &self.disposition)
            .finish_non_exhaustive()
    }
}
