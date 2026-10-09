//! Adding and replacing tasks one at a time.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use opentelemetry::KeyValue;
use opentelemetry::context::FutureExt as _;

use super::Docket;
use crate::error::Result;
use crate::execution::{Disposition, Execution, Message};
use crate::keys::WORKER_GROUP;
use crate::scripts;
use crate::task::Task;
use crate::telemetry;
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
        let immediate = message.when <= self.now();
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
        let message = Self::message(T::NAME, &args, Some(key.into()), when)?;
        self.submit(message, true).await
    }

    fn message<T: Task>(
        function: &str,
        args: &T,
        key: Option<String>,
        when: DateTime<Utc>,
    ) -> Result<Message> {
        Ok(Message {
            key: key.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
            when,
            function: function.to_owned(),
            args: serde_json::to_string(args)?,
            attempt: 1,
            generation: 0,
            trace: HashMap::new(),
        })
    }

    async fn submit<O>(&self, message: Message, replace: bool) -> Result<Execution<O>> {
        let disposition = self
            .schedule(Placement::new(message.clone(), replace))
            .await?;
        Ok(Execution::new(self.clone(), &message, disposition))
    }

    /// Places one task the way an add or a replace does: under a
    /// `docket.add` or `docket.replace` span, refused when a strike blocks
    /// it, and counted.
    pub(crate) async fn schedule(&self, placement: Placement) -> Result<Disposition> {
        let replace = placement.replace;
        let name = if replace {
            "docket.replace"
        } else {
            "docket.add"
        };
        let mut attributes = self.labels();
        attributes.extend(telemetry::run_attributes(&placement.message));
        let span = self.telemetry().producer_span(name, attributes);
        let function = placement.message.function.clone();
        let disposition = if self.refuse_struck(&placement.message) {
            Ok(Disposition::Struck)
        } else {
            self.place(placement).with_context(span.clone()).await
        };
        telemetry::end(
            &span,
            disposition
                .as_ref()
                .map(|disposition| vec![KeyValue::new("docket.disposition", disposition.as_str())]),
        );
        let disposition = disposition?;
        self.count_scheduled(&function, &disposition, replace);
        Ok(disposition)
    }

    pub(crate) fn is_struck(&self, message: &Message) -> bool {
        let args = serde_json::from_str(&message.args).unwrap_or(serde_json::Value::Null);
        self.has_run_out(&message.key) || self.strikes().is_struck(&message.function, &args)
    }

    /// Whether a strike blocks `message`, and if one does, logs and counts
    /// it the way every scheduling path in pydocket does.
    pub(super) fn refuse_struck(&self, message: &Message) -> bool {
        if !self.is_struck(message) {
            return false;
        }
        tracing::warn!(
            "{:?} is stricken, skipping schedule of {:?}",
            message.function,
            message.key
        );
        let mut labels = telemetry::task_labels(self.name(), &message.function);
        labels.push(KeyValue::new("docket.where", "docket"));
        self.telemetry().metrics.tasks_stricken.add(1, &labels);
        true
    }

    /// Counts a placed task.  One that a strike blocked, that Redis refused,
    /// or that a newer copy superseded was neither added nor replaced, so it
    /// does not count.  A replace also counts as a cancel of what it
    /// replaced.
    pub(super) fn count_scheduled(&self, function: &str, disposition: &Disposition, replace: bool) {
        if matches!(
            disposition,
            Disposition::Struck | Disposition::Failed(_) | Disposition::Superseded
        ) {
            return;
        }
        let metrics = &self.telemetry().metrics;
        let labels = telemetry::task_labels(self.name(), function);
        if replace {
            metrics.tasks_replaced.add(1, &labels);
            metrics.tasks_cancelled.add(1, &labels);
            metrics.tasks_scheduled.add(1, &labels);
        } else {
            metrics.tasks_added.add(1, &labels);
            if *disposition == Disposition::Scheduled {
                metrics.tasks_scheduled.add(1, &labels);
            }
        }
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
        let now = self.docket.now();
        self.at(now + delay)
    }
}

impl<'a, T: Task> IntoFuture for Add<'a, T> {
    type Output = Result<Execution<T::Output>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let when = self.when.unwrap_or_else(|| self.docket.now());
            let message = Docket::message(T::NAME, &self.args, self.key, when)?;
            self.docket.submit(message, false).await
        })
    }
}
