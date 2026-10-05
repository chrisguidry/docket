//! Adding and replacing tasks, one at a time or in batches.

use std::future::IntoFuture;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use redis::{RedisResult, Value};

use super::Docket;
use crate::error::{Error, Result};
use crate::execution::{Disposition, Execution, Message};
use crate::keys::WORKER_GROUP;
use crate::scripts;
use crate::task::Task;
use crate::wire::{iso, seconds};

/// Where and how one task goes into the docket.
pub(crate) struct Placement {
    pub message: Message,
    pub replace: bool,
    /// The stream entry of the delivery being rescheduled, which the script
    /// acknowledges in the same step.  Empty for a new task.
    pub reschedule_message_id: String,
    /// The generation the caller last saw.  Zero skips the check.
    pub expected_generation: i64,
}

impl Placement {
    pub fn new(message: Message, replace: bool) -> Self {
        Self {
            message,
            replace,
            reschedule_message_id: String::new(),
            expected_generation: 0,
        }
    }
}

impl Docket {
    pub(crate) fn schedule_call(&self, placement: Placement) -> scripts::Call {
        let keys = self.keys();
        let message = &placement.message;
        let key = message.key.as_str();
        let immediate = message.when <= Utc::now();
        let payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": if immediate { "queued" } else { "scheduled" },
            "when": iso(message.when),
        });
        scripts::Schedule {
            stream_key: keys.stream(),
            known_key: keys.known(key),
            parked_key: keys.parked(key),
            queue_key: keys.queue(),
            stream_id_key: keys.stream_id(key),
            runs_key: keys.runs(key),
            state_channel: keys.state(key),
            task_key: key.to_owned(),
            when_timestamp: seconds(message.when),
            is_immediate: immediate,
            replace: placement.replace,
            reschedule_message_id: placement.reschedule_message_id,
            expected_generation: placement.expected_generation,
            worker_group_name: WORKER_GROUP.to_owned(),
            state_payload: payload.to_string(),
            message: message.fields(),
        }
        .call()
    }

    /// Places one task, and returns what the script decided.
    pub(crate) async fn place(&self, placement: Placement) -> Result<Disposition> {
        let call = self.schedule_call(placement);
        let mut connection = self.handle();
        let reply: String = call.run(&mut connection).await?;
        Ok(Disposition::from_reply(&reply))
    }

    /// Adds a task, due now unless [`Add::at`] or [`Add::after`] says
    /// otherwise.  Await the returned builder to add it.
    ///
    /// When a task with the same key is already scheduled or running, the
    /// add does nothing, and the execution's disposition says so.
    pub fn add<T: Task>(&self, args: T) -> Add<'_, T> {
        Add {
            docket: self,
            args,
            key: None,
            when: None,
        }
    }

    /// Replaces the task with this key, or adds it when there is none.
    pub async fn replace<T: Task>(
        &self,
        args: T,
        key: impl Into<String>,
        when: DateTime<Utc>,
    ) -> Result<Execution<T::Output>> {
        let message = Self::message(T::NAME, &args, Some(key.into()), Some(when))?;
        self.submit(message, true).await
    }

    /// Captures a task for [`Docket::add_many`] or [`Docket::replace_many`].
    #[expect(
        clippy::needless_pass_by_value,
        reason = "callers build the arguments in place, as with Docket::add"
    )]
    pub fn call<T: Task>(&self, args: T) -> Call {
        Call::new(&args)
    }

    /// Adds many tasks in one round trip to Redis.
    pub async fn add_many(&self, calls: impl IntoIterator<Item = Call>) -> Result<Vec<Execution>> {
        self.place_many(calls.into_iter().collect(), false).await
    }

    /// Replaces many tasks in one round trip to Redis.  Every call needs a
    /// key.
    pub async fn replace_many(
        &self,
        calls: impl IntoIterator<Item = Call>,
    ) -> Result<Vec<Execution>> {
        self.place_many(calls.into_iter().collect(), true).await
    }

    fn message<T: Task>(
        function: &str,
        args: &T,
        key: Option<String>,
        when: Option<DateTime<Utc>>,
    ) -> Result<Message> {
        Ok(Message {
            key: key.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
            when: when.unwrap_or_else(Utc::now),
            function: function.to_owned(),
            args: serde_json::to_string(args)?,
            attempt: 1,
            generation: 0,
        })
    }

    async fn submit<O>(&self, message: Message, replace: bool) -> Result<Execution<O>> {
        let disposition = if self.is_struck(&message) {
            Disposition::Struck
        } else {
            self.place(Placement::new(message.clone(), replace)).await?
        };
        Ok(Execution::new(self.clone(), &message, disposition))
    }

    pub(crate) fn is_struck(&self, message: &Message) -> bool {
        let args = serde_json::from_str(&message.args).unwrap_or(serde_json::Value::Null);
        self.strikes().is_struck(&message.function, &args)
    }

    async fn place_many(&self, calls: Vec<Call>, replace: bool) -> Result<Vec<Execution>> {
        let mut messages = Vec::new();
        for call in calls {
            if replace && call.key.is_none() {
                return Err(Error::Invalid(format!(
                    "replacing a {} task needs its key",
                    call.function
                )));
            }
            messages.push(Message {
                key: call.key.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                when: call.when.unwrap_or_else(Utc::now),
                function: call.function.to_owned(),
                args: call
                    .args
                    .map_err(|error| Error::Json(serde::ser::Error::custom(error)))?,
                attempt: 1,
                generation: 0,
            });
        }

        // A strike can arrive while the batch is in flight, so each message
        // is judged once, and only the placed ones take a reply.
        let struck: Vec<bool> = messages
            .iter()
            .map(|message| self.is_struck(message))
            .collect();
        let mut pipeline = redis::pipe();
        pipeline.ignore_errors();
        let mut placed = Vec::new();
        for (message, _) in messages.iter().zip(&struck).filter(|(_, struck)| !**struck) {
            let call = self.schedule_call(Placement::new(message.clone(), replace));
            if self.backend().is_cluster() {
                call.queue_eval(&mut pipeline);
            } else {
                call.queue(&mut pipeline);
            }
            placed.push(call);
        }

        let mut replies: Vec<RedisResult<Value>> = Vec::new();
        if let Some(first) = placed.first() {
            let mut connection = self.handle();
            if !self.backend().is_cluster() {
                first.load(&mut connection).await?;
            }
            replies = pipeline.query_async(&mut connection).await?;
        }

        let mut dispositions = replies.into_iter().map(|reply| match reply {
            Ok(value) => Disposition::from_value(&value),
            Err(error) => Disposition::Failed(error.to_string()),
        });
        Ok(messages
            .iter()
            .zip(struck)
            .map(|(message, struck)| {
                let disposition = if struck {
                    Disposition::Struck
                } else {
                    // Redis answers every command of a pipeline.
                    dispositions
                        .next()
                        .unwrap_or(Disposition::Failed("Redis sent no reply".to_owned()))
                };
                Execution::new(self.clone(), message, disposition)
            })
            .collect())
    }
}

/// A task waiting to be added.  Await it to add it.
#[must_use = "an add does nothing until it is awaited"]
pub struct Add<'a, T> {
    docket: &'a Docket,
    args: T,
    key: Option<String>,
    when: Option<DateTime<Utc>>,
}

impl<T: Task> Add<'_, T> {
    /// Gives the task a key.  Adding a task whose key is already scheduled
    /// does nothing, so a key makes the add idempotent.  Without one, the
    /// task gets a new UUID.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Schedules the task for a time.
    pub fn at(mut self, when: DateTime<Utc>) -> Self {
        self.when = Some(when);
        self
    }

    /// Schedules the task for a time after now.
    ///
    /// # Panics
    ///
    /// When `delay` is longer than the time left before the year 262143.
    pub fn after(self, delay: Duration) -> Self {
        let delay = chrono::Duration::from_std(delay).expect("a delay fits in a chrono duration");
        self.at(Utc::now() + delay)
    }
}

impl<'a, T: Task> IntoFuture for Add<'a, T> {
    type Output = Result<Execution<T::Output>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let message = Docket::message(T::NAME, &self.args, self.key, self.when)?;
            self.docket.submit(message, false).await
        })
    }
}

/// A task captured by [`Docket::call`], for a batch.
#[must_use = "a call does nothing until it goes into a batch"]
#[derive(Clone, Debug)]
pub struct Call {
    function: &'static str,
    /// The arguments as JSON text, or why they did not convert.
    args: std::result::Result<String, String>,
    key: Option<String>,
    when: Option<DateTime<Utc>>,
}

impl Call {
    pub(crate) fn new<T: Task>(args: &T) -> Self {
        Self {
            function: T::NAME,
            args: serde_json::to_string(args).map_err(|error| error.to_string()),
            key: None,
            when: None,
        }
    }

    /// Gives the task a key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Schedules the task for a time.
    pub fn at(mut self, when: DateTime<Utc>) -> Self {
        self.when = Some(when);
        self
    }
}
