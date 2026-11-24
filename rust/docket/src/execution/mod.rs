//! One scheduled run of a task, as a producer sees it.

mod events;
mod message;
mod progress;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::marker::PhantomData;

use futures::StreamExt;
use redis::{AsyncCommands, Value};
use serde::de::DeserializeOwned;

use crate::Timestamp;
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

    /// The disposition as pydocket names it on a span.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::AlreadyScheduled => "already_scheduled",
            Self::Struck => "struck",
            Self::Superseded => "superseded",
            Self::Failed(_) => "failed",
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
    pub when: Option<Timestamp>,
    /// The worker that ran or runs it.
    pub worker: Option<String>,
    /// When a worker started it.
    pub started_at: Option<Timestamp>,
    /// When it ended.
    pub completed_at: Option<Timestamp>,
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

/// The fields of a runs hash whose text docket-rs reads.
const READ_FIELDS: [&str; 10] = [
    "state",
    "when",
    "worker",
    "started_at",
    "completed_at",
    "error",
    "function",
    "generation",
    "waiter_stream",
    "waiter_entry_id",
];

/// A runs hash as text, for reading only.  A field that docket-rs reads must
/// be UTF-8 text, or the hash is unreadable.  Any other field, such as the
/// pickled arguments pydocket writes, has its bytes that are not UTF-8
/// replaced with U+FFFD.
pub(crate) fn runs_text(runs: HashMap<Vec<u8>, Vec<u8>>) -> Result<HashMap<String, String>> {
    runs_fields(runs).map_err(Error::Undecodable)
}

/// [`runs_text`], with the reason a hash is unreadable as text.
pub(crate) fn runs_fields(
    runs: HashMap<Vec<u8>, Vec<u8>>,
) -> std::result::Result<HashMap<String, String>, String> {
    runs.into_iter()
        .map(|(field, value)| {
            // A field name that is not UTF-8 names no field docket reads.
            let field = String::from_utf8_lossy(&field).into_owned();
            let value = if READ_FIELDS.contains(&field.as_str()) {
                String::from_utf8(value)
                    .map_err(|_| format!("its {field:?} field is not UTF-8 text"))?
            } else {
                String::from_utf8_lossy(&value).into_owned()
            };
            Ok((field, value))
        })
        .collect()
}

fn seconds_to_time(seconds: f64) -> Option<Timestamp> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "microseconds since the epoch fit in i64"
    )]
    Timestamp::from_unix_micros((seconds * 1_000_000.0).round() as i64)
}

/// One scheduled run of a task.  `O` is the type of the task's output.
pub struct Execution<O = serde_json::Value> {
    docket: Docket,
    key: String,
    function: String,
    when: Timestamp,
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
    pub(crate) fn found(docket: Docket, key: String, function: String, when: Timestamp) -> Self {
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
    pub fn when(&self) -> Timestamp {
        self.when
    }

    /// What happened when the task was added.
    #[must_use]
    pub fn disposition(&self) -> &Disposition {
        &self.disposition
    }

    /// The run's current status, or `None` when the docket knows nothing of
    /// it, for example after its state expired.  A run whose record cannot
    /// be read returns [`Error::Undecodable`].
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
    /// an event.  A run whose record cannot be read returns
    /// [`Error::Undecodable`].
    pub async fn subscribe(&self) -> Result<Events> {
        events::subscribe(&self.docket, &self.key).await
    }
}

impl<O: DeserializeOwned> Execution<O> {
    /// Waits for the task to end, and returns its output.  A failed task
    /// returns [`Error::TaskFailed`], a cancelled one
    /// [`Error::TaskCancelled`], and one whose record cannot be read
    /// [`Error::Undecodable`].  Bound the wait with `tokio::time::timeout`.
    pub async fn result(&self) -> Result<O> {
        output(&self.docket, &self.key)
            .await
            .and_then(|output| serde_json::from_value(output).map_err(Error::from))
    }
}

// The work of an execution lives in functions of the docket and key, so it
// compiles once rather than once for each output type.

async fn status(docket: &Docket, key: &str) -> Result<Option<Status>> {
    let runs: HashMap<Vec<u8>, Vec<u8>> = docket.handle().hgetall(docket.keys().runs(key)).await?;
    Ok(Status::from_hash(&runs_text(runs)?))
}

/// Waits for the run of `key` to end, and returns its output.
async fn output(docket: &Docket, key: &str) -> Result<serde_json::Value> {
    // A subscription starts with the state as it is now, and the terminal
    // event carries the state and its error, so the events alone decide.
    // When Redis drops the subscription, the stream ends, and a new
    // subscription reads the state afresh, so no change falls between them.
    loop {
        let mut events = events::subscribe(docket, key).await?;
        while let Some(event) = events.next().await {
            if let Event::State(event) = event?
                && event.state.is_terminal()
            {
                return outcome(docket, key, event.state, event.error).await;
            }
        }
    }
}

async fn outcome(
    docket: &Docket,
    key: &str,
    state: State,
    error: Option<String>,
) -> Result<serde_json::Value> {
    match state {
        State::Cancelled => Err(Error::TaskCancelled {
            key: key.to_owned(),
        }),
        State::Failed => Err(Error::TaskFailed {
            key: key.to_owned(),
            message: error.unwrap_or_else(|| "the task failed".to_owned()),
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
