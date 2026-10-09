//! Adding and replacing many tasks at once, in pipelined chunks.

use std::collections::HashMap;
use std::future::IntoFuture;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use opentelemetry::KeyValue;
use opentelemetry::context::FutureExt as _;
use redis::{RedisResult, Value};

use super::Docket;
use super::schedule::Placement;
use crate::error::{Error, Result};
use crate::execution::{Disposition, Execution, Message};
use crate::scripts;
use crate::task::Task;
use crate::telemetry;

impl Docket {
    /// Captures a task for [`Docket::add_many`] or [`Docket::replace_many`].
    #[expect(
        clippy::needless_pass_by_value,
        reason = "callers build the arguments in place, as with Docket::add"
    )]
    pub fn call<T: Task>(&self, args: T) -> Call {
        Call::new(&args)
    }

    /// Adds many tasks, sending them to Redis in pipelines of 1,000 unless
    /// the [`Batch`] says otherwise.  Await it to add them.
    ///
    /// The batch is not atomic.  If Redis fails on the first pipeline, the
    /// await returns the error.  If it fails on a later one, the tasks in the
    /// pipelines before it are in Redis, and the await returns every
    /// execution: the failed pipeline's and those after it have
    /// [`Disposition::Failed`], and some of the failed pipeline's tasks can
    /// still be in Redis.
    pub fn add_many(&self, calls: impl IntoIterator<Item = Call>) -> Batch<'_> {
        Batch::new(self, calls, false)
    }

    /// Replaces many tasks, sending them to Redis in pipelines of 1,000
    /// unless the [`Batch`] says otherwise.  A call without a key gets a new
    /// one, as it does in an add.  Await it to replace them.  A failure
    /// partway through returns what [`Docket::add_many`] does.
    pub fn replace_many(&self, calls: impl IntoIterator<Item = Call>) -> Batch<'_> {
        Batch::new(self, calls, true)
    }

    async fn place_many(
        &self,
        calls: Vec<Call>,
        replace: bool,
        chunk_size: Option<usize>,
    ) -> Result<Vec<Execution>> {
        // Checked before anything else, so a bad size is refused even for an
        // empty or entirely struck batch, as in pydocket.
        if chunk_size == Some(0) {
            return Err(Error::Invalid(
                "a batch's chunk size must be at least 1".into(),
            ));
        }
        let mut messages = Vec::new();
        for call in calls {
            messages.push(Message {
                key: call.key.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                when: call.when.unwrap_or_else(|| self.now()),
                function: call.function.to_owned(),
                args: call
                    .args
                    .map_err(|error| Error::Json(serde::ser::Error::custom(error)))?,
                attempt: 1,
                generation: 0,
                trace: HashMap::new(),
            });
        }

        let name = if replace {
            "docket.replace_many"
        } else {
            "docket.add_many"
        };
        let mut attributes = self.labels();
        attributes.push(KeyValue::new(
            "docket.batch.count",
            i64::try_from(messages.len()).unwrap_or(i64::MAX),
        ));
        let span = self.telemetry().producer_span(name, attributes);
        // A strike can arrive while the batch is in flight, so each message
        // is judged once, and only the placed ones take a reply.
        let struck: Vec<bool> = messages
            .iter()
            .map(|message| self.refuse_struck(message))
            .collect();
        let stricken = struck.iter().filter(|struck| **struck).count();
        let placed = self
            .place_batch(&messages, &struck, replace, chunk_size)
            .with_context(span.clone())
            .await;
        telemetry::end(
            &span,
            match &placed {
                Ok((_, None)) => Ok(vec![KeyValue::new(
                    "docket.batch.stricken",
                    i64::try_from(stricken).unwrap_or(i64::MAX),
                )]),
                Ok((_, Some(error))) | Err(error) => Err(error),
            },
        );
        let (replies, stopped) = placed?;
        let unanswered = stopped.map_or_else(
            || "Redis sent no reply".to_owned(),
            |error| error.to_string(),
        );
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
                    // Redis answers every command of a pipeline it reads, so
                    // only the chunks after a failed one go unanswered.
                    let disposition = dispositions
                        .next()
                        .unwrap_or_else(|| Disposition::Failed(unanswered.clone()));
                    self.count_scheduled(&message.function, &disposition, replace);
                    disposition
                };
                Execution::new(self.clone(), message, disposition)
            })
            .collect())
    }

    /// Sends the messages that no strike blocked, in pipelines of at most
    /// `chunk_size`, or all in one, and returns Redis's reply to each.  When
    /// a chunk fails after an earlier one went in, it returns the replies it
    /// has and the error that stopped it, because those tasks are in Redis
    /// and the caller needs their dispositions.
    async fn place_batch(
        &self,
        messages: &[Message],
        struck: &[bool],
        replace: bool,
        chunk_size: Option<usize>,
    ) -> Result<(Vec<RedisResult<Value>>, Option<Error>)> {
        let placed: Vec<scripts::Call> = messages
            .iter()
            .zip(struck)
            .filter(|(_, struck)| !**struck)
            .map(|(message, _)| self.schedule_call(Placement::new(message.clone(), replace)))
            .collect();
        let mut replies: Vec<RedisResult<Value>> = Vec::new();
        let Some(first) = placed.first() else {
            return Ok((replies, None));
        };
        let mut connection = self.handle();
        if !self.backend().is_cluster() {
            first.load(&mut connection).await?;
        }
        for chunk in placed.chunks(chunk_size.unwrap_or(placed.len())) {
            let mut pipeline = redis::pipe();
            pipeline.ignore_errors();
            for call in chunk {
                if self.backend().is_cluster() {
                    call.queue_eval(&mut pipeline);
                } else {
                    call.queue(&mut pipeline);
                }
            }
            match pipeline.query_async(&mut connection).await {
                Ok(chunk_replies) => replies.extend::<Vec<RedisResult<Value>>>(chunk_replies),
                Err(error) if !replies.is_empty() => return Ok((replies, Some(error.into()))),
                Err(error) => return Err(error.into()),
            }
        }
        Ok((replies, None))
    }
}

/// How many tasks a [`Batch`] sends in each pipeline unless it says
/// otherwise, the same as pydocket's default.
const CHUNK_SIZE: usize = 1000;

/// Many tasks waiting to be added or replaced.  Await it to send them.
#[must_use = "a batch does nothing until it is awaited"]
pub struct Batch<'a> {
    docket: &'a Docket,
    calls: Vec<Call>,
    replace: bool,
    chunk_size: Option<usize>,
}

impl<'a> Batch<'a> {
    fn new(docket: &'a Docket, calls: impl IntoIterator<Item = Call>, replace: bool) -> Self {
        Self {
            docket,
            calls: calls.into_iter().collect(),
            replace,
            chunk_size: Some(CHUNK_SIZE),
        }
    }

    /// Sends at most `size` tasks in each pipeline.  Smaller pipelines hold
    /// less in memory and let other clients in between; larger ones take
    /// fewer round trips.  Awaiting the batch fails when `size` is 0.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Sends the whole batch in one pipeline.
    pub fn one_pipeline(mut self) -> Self {
        self.chunk_size = None;
        self
    }
}

impl<'a> IntoFuture for Batch<'a> {
    type Output = Result<Vec<Execution>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.docket
                .place_many(self.calls, self.replace, self.chunk_size)
                .await
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
