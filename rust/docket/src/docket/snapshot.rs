//! Looking at what is in a docket, and emptying it.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use opentelemetry::context::FutureExt as _;
use redis::streams::{StreamPendingCountReply, StreamRangeReply};
use redis::{AsyncCommands, RedisResult};

use super::Docket;
use crate::error::Result;
use crate::execution::{Execution, Message, Status};
use crate::keys::WORKER_GROUP;
use crate::telemetry;
use crate::wire::seconds;

/// How many stream entries a snapshot reads.
const SNAPSHOT_LIMIT: usize = 1000;

/// What a docket holds at one moment.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Snapshot {
    /// When the snapshot was taken.
    pub taken: DateTime<Utc>,
    /// Every task in the docket, running or not.
    pub total_tasks: usize,
    /// Tasks that no worker has started.
    pub future: Vec<TaskSummary>,
    /// Tasks that a worker has started.
    pub running: Vec<TaskSummary>,
    /// The workers that have reported recently.
    pub workers: Vec<WorkerSummary>,
}

/// One task in a [`Snapshot`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TaskSummary {
    /// The task's key.
    pub key: String,
    /// The task's name.
    pub function: String,
    /// When it is or was due.
    pub when: DateTime<Utc>,
    /// Its attempt number.
    pub attempt: u32,
    /// Its arguments.
    pub args: serde_json::Value,
    /// The worker running it.
    pub worker: Option<String>,
}

impl TaskSummary {
    fn new(message: Message, worker: Option<String>) -> Self {
        Self {
            args: serde_json::from_str(&message.args).unwrap_or(serde_json::Value::Null),
            key: message.key,
            function: message.function,
            when: message.when,
            attempt: message.attempt,
            worker,
        }
    }
}

/// A worker that reported recently.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct WorkerSummary {
    /// The worker's name.
    pub name: String,
    /// When it last reported.
    pub last_seen: DateTime<Utc>,
    /// The tasks it can run.
    pub tasks: Vec<String>,
}

impl Docket {
    /// What the docket holds now.
    pub async fn snapshot(&self) -> Result<Snapshot> {
        let keys = self.keys();
        let taken = Utc::now();
        let mut connection = self.handle();
        self.ensure_group(&mut connection).await?;

        let (stream_length, queue_length, pending, entries, queued): (
            usize,
            usize,
            StreamPendingCountReply,
            StreamRangeReply,
            Vec<String>,
        ) = redis::pipe()
            .xlen(keys.stream())
            .zcard(keys.queue())
            .xpending_count(keys.stream(), WORKER_GROUP, "-", "+", SNAPSHOT_LIMIT)
            .xrange_count(keys.stream(), "-", "+", SNAPSHOT_LIMIT)
            .zrange(keys.queue(), 0, -1)
            .query_async(&mut connection)
            .await?;

        let running_by_id: HashMap<String, String> = pending
            .ids
            .into_iter()
            .map(|entry| (entry.id, entry.consumer))
            .collect();

        let mut future = Vec::new();
        let mut running = Vec::new();
        for entry in entries.ids {
            let Ok(message) = Message::from_entry(&entry) else {
                continue;
            };
            match running_by_id.get(&entry.id) {
                Some(worker) => running.push(TaskSummary::new(message, Some(worker.clone()))),
                None => future.push(TaskSummary::new(message, None)),
            }
        }
        for key in queued {
            let parked: HashMap<String, Vec<u8>> = connection.hgetall(keys.parked(&key)).await?;
            if let Ok(message) = Message::from_fields(&parked) {
                future.push(TaskSummary::new(message, None));
            }
        }
        future.sort_by_key(|task| task.when);

        Ok(Snapshot {
            taken,
            total_tasks: stream_length + queue_length,
            future,
            running,
            workers: self.workers().await?,
        })
    }

    /// The run of the task with this key, or `None` when the docket knows
    /// nothing of it, for example after its state expired.
    pub async fn execution(&self, key: &str) -> Result<Option<Execution>> {
        let mut connection = self.handle();
        let runs: HashMap<String, String> = connection.hgetall(self.keys().runs(key)).await?;
        let Some(function) = runs.get("function") else {
            return Ok(None);
        };
        let status = Status::from_hash(&runs);
        let when = status
            .and_then(|status| status.when)
            .unwrap_or_else(Utc::now);
        Ok(Some(Execution::found(
            self.clone(),
            key.to_owned(),
            function.clone(),
            when,
        )))
    }

    /// The workers that have reported recently.
    pub async fn workers(&self) -> Result<Vec<WorkerSummary>> {
        self.list_workers(self.keys().workers()).await
    }

    /// The workers that have reported recently and can run the task `name`.
    pub async fn task_workers(&self, name: &str) -> Result<Vec<WorkerSummary>> {
        self.list_workers(self.keys().task_workers(name)).await
    }

    async fn list_workers(&self, key: String) -> Result<Vec<WorkerSummary>> {
        let mut connection = self.handle();
        let oldest = seconds(Utc::now()) - self.heartbeat_window().as_secs_f64();
        let _: () = connection.zrembyscore(&key, 0, oldest).await?;
        let seen: Vec<(String, f64)> = connection.zrange_withscores(&key, 0, -1).await?;
        let mut workers = Vec::new();
        for (name, last_seen) in seen {
            let mut tasks: Vec<String> =
                connection.smembers(self.keys().worker_tasks(&name)).await?;
            tasks.sort();
            #[expect(
                clippy::cast_possible_truncation,
                reason = "microseconds since the epoch fit in i64"
            )]
            let last_seen = DateTime::from_timestamp_micros((last_seen * 1_000_000.0) as i64)
                .unwrap_or(DateTime::UNIX_EPOCH);
            workers.push(WorkerSummary {
                name,
                last_seen,
                tasks,
            });
        }
        Ok(workers)
    }

    /// How long a worker stays listed after its last heartbeat.
    pub(crate) fn heartbeat_window(&self) -> std::time::Duration {
        self.settings().heartbeat_interval * self.settings().missed_heartbeats
    }

    /// Removes every task that no worker has started, and returns how many
    /// tasks the docket held.  Running tasks finish.
    pub async fn clear(&self) -> Result<usize> {
        let span = self
            .telemetry()
            .producer_span("docket.clear", self.labels());
        let cleared = self.clear_tasks().with_context(span.clone()).await;
        telemetry::end(&span, cleared.as_ref().map(|_| Vec::new()));
        cleared
    }

    async fn clear_tasks(&self) -> Result<usize> {
        let keys = self.keys();
        let mut connection = self.handle();
        let (stream_length, queue_length, queued, entries): (
            usize,
            usize,
            Vec<String>,
            StreamRangeReply,
        ) = redis::pipe()
            .xlen(keys.stream())
            .zcard(keys.queue())
            .zrange(keys.queue(), 0, -1)
            .xrange_all(keys.stream())
            .query_async(&mut connection)
            .await?;

        let mut task_keys: HashSet<String> = queued.into_iter().collect();
        for entry in entries.ids {
            if let Some(redis::Value::BulkString(key)) = entry.map.get("key") {
                task_keys.insert(String::from_utf8_lossy(key).into_owned());
            }
        }

        let mut pipeline = redis::pipe();
        pipeline
            .cmd("XTRIM")
            .arg(keys.stream())
            .arg("MAXLEN")
            .arg(0)
            .ignore()
            .del(keys.queue())
            .ignore();
        for key in &task_keys {
            pipeline
                .del(&[keys.parked(key), keys.known(key), keys.stream_id(key)])
                .ignore();
            match self.ttl_seconds() {
                0 => pipeline.del(keys.runs(key)).ignore(),
                ttl => pipeline.expire(keys.runs(key), ttl).ignore(),
            };
        }
        let () = pipeline.query_async(&mut connection).await?;
        Ok(stream_length + queue_length)
    }

    /// Creates the workers' consumer group if it does not exist yet.
    pub(crate) async fn ensure_group(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<()> {
        let created: RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(self.keys().stream())
            .arg(WORKER_GROUP)
            .arg("0-0")
            .arg("MKSTREAM")
            .query_async(connection)
            .await;
        match created {
            Err(error) if error.code() != Some("BUSYGROUP") => Err(error.into()),
            _ => Ok(()),
        }
    }
}
