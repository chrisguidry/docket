//! A worker's run: sessions of the poll loop, with a reconnect after Redis
//! goes away.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisResult};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::sweep::Sweep;
use super::{Settings, Until, cancellation, execute, heartbeat, perpetuals, sweep};
use crate::connection::Connection;
use crate::docket::Docket;
use crate::error::{Error, Result};
use crate::execution::Message;
use crate::keys::WORKER_GROUP;

/// What every part of a running worker shares.
pub(crate) struct Shared {
    pub docket: Docket,
    pub settings: Settings,
    iterations: HashMap<String, u32>,
    counts: Mutex<HashMap<String, u32>>,
    active: Mutex<HashMap<String, Active>>,
}

/// A task this worker is running.
#[derive(Clone)]
pub(crate) struct Active {
    pub key: String,
    /// Stops the handler.
    pub cancel: CancellationToken,
    /// Set when the stop came from [`Docket::cancel`], not from shutdown.
    pub cancelled_by_docket: Arc<AtomicBool>,
}

impl Shared {
    pub fn new(docket: Docket, settings: Settings, iterations: HashMap<String, u32>) -> Self {
        Self {
            docket,
            settings,
            iterations,
            counts: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn start(&self, message_id: &str, key: &str) -> Active {
        let active = Active {
            key: key.to_owned(),
            cancel: CancellationToken::new(),
            cancelled_by_docket: Arc::new(AtomicBool::new(false)),
        };
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(message_id.to_owned(), active.clone());
        active
    }

    pub fn finish(&self, message_id: &str) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(message_id);
    }

    /// The stream entries of the running tasks, for lease renewal.
    pub fn active_ids(&self) -> Vec<String> {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    /// Stops the running copy of the task with this key, if there is one.
    pub fn cancel_key(&self, key: &str) {
        for active in self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|active| active.key == key)
        {
            active.cancelled_by_docket.store(true, Ordering::SeqCst);
            active.cancel.cancel();
        }
    }

    /// Counts a run of `key`, and says whether `run_at_most` allows it.
    pub fn allow_run(&self, key: &str) -> bool {
        let Some(limit) = self.iterations.get(key) else {
            return true;
        };
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = counts.entry(key.to_owned()).or_default();
        *count += 1;
        *count <= *limit
    }
}

/// One delivery from the stream.
pub(crate) struct Delivery {
    pub id: String,
    pub message: Message,
    /// The delivery came from the redelivery sweep, after another worker
    /// stopped renewing it.
    pub redelivered: bool,
}

pub(super) async fn run(worker: Arc<Shared>, until: Until) -> Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        match session(&worker, &until, &mut tasks).await {
            Err(error) if error.is_redis_unavailable() => {
                tracing::warn!(%error, "Redis is unavailable; reconnecting");
                let shutdown = shutdown_token(&until);
                tokio::select! {
                    () = tokio::time::sleep(worker.settings.reconnection_delay) => {}
                    () = shutdown.cancelled() => return Ok(()),
                }
            }
            result => return result,
        }
    }
}

fn shutdown_token(until: &Until) -> CancellationToken {
    match until {
        Until::Shutdown(token) => token.clone(),
        Until::Finished => CancellationToken::new(),
    }
}

async fn session(
    worker: &Arc<Shared>,
    until: &Until,
    tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let shutdown = shutdown_token(until);
    let mut infrastructure = JoinSet::new();
    let result = async {
        cancellation::start(worker, &mut infrastructure).await;
        if worker.settings.schedule_automatic_tasks {
            // Seeding waits for the strikes to load, which takes as long as
            // Redis is unreachable, so a shutdown must not wait for it.
            tokio::select! {
                seeded = perpetuals::seed(worker) => seeded?,
                () = shutdown.cancelled() => return Ok(()),
            }
            infrastructure.spawn(perpetuals::reseed(Arc::clone(worker)));
        }
        infrastructure.spawn(scheduler(Arc::clone(worker)));
        infrastructure.spawn(sweep::renew_leases(Arc::clone(worker)));
        infrastructure.spawn(heartbeat::beat(Arc::clone(worker)));
        poll(worker, until, &shutdown, tasks).await
    }
    .await;

    // Running tasks finish before the session ends, while their leases are
    // still renewed, so that no other worker takes them over mid-run.
    let drained = drain(tasks).await;
    infrastructure.shutdown().await;
    heartbeat::remove(worker).await;
    result.and(drained)
}

async fn poll(
    worker: &Arc<Shared>,
    until: &Until,
    shutdown: &CancellationToken,
    tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let docket = &worker.docket;
    let settings = &worker.settings;
    let mut reader = docket.backend().connect().await?;
    docket.ensure_group(&mut reader).await?;
    let mut sweep = Sweep::new(Arc::clone(worker));

    loop {
        while let Some(finished) = tasks.try_join_next() {
            collect(finished)?;
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let available = settings.concurrency.saturating_sub(tasks.len());
        if available == 0 {
            tokio::select! {
                Some(finished) = tasks.join_next() => collect(finished)?,
                () = shutdown.cancelled() => return Ok(()),
            }
            continue;
        }
        let count = available.min(settings.message_batch);

        let mut deliveries = Vec::new();
        if sweep.due().await? {
            deliveries.extend(sweep.claim(count).await?);
        }
        if deliveries.len() < count {
            let read = read(worker, &mut reader, count - deliveries.len());
            let read = tokio::select! {
                read = read => read?,
                () = shutdown.cancelled() => return Ok(()),
            };
            deliveries.extend(read);
        }

        let found = !deliveries.is_empty();
        for delivery in deliveries {
            tasks.spawn(execute::run(Arc::clone(worker), delivery));
        }

        if matches!(until, Until::Finished)
            && !found
            && tasks.is_empty()
            && !has_work(docket).await?
        {
            return Ok(());
        }
    }
}

async fn read(worker: &Shared, reader: &mut Connection, count: usize) -> Result<Vec<Delivery>> {
    let docket = &worker.docket;
    let block = worker
        .settings
        .minimum_check_interval
        .as_millis()
        .try_into()
        .unwrap_or(usize::MAX);
    let options = StreamReadOptions::default()
        .group(WORKER_GROUP, &worker.settings.name)
        .count(count)
        .block(block.max(1));
    let stream = docket.keys().stream();
    let reply: RedisResult<Option<StreamReadReply>> = reader
        .xread_options(&[stream.as_str()], &[">"], &options)
        .await;
    let entries = regrouping(docket, reply)
        .await?
        .flatten()
        .into_iter()
        .flat_map(|reply| reply.keys)
        .flat_map(|key| key.ids);
    Ok(deliveries(docket, entries, false).await)
}

/// The reply to a consumer group command, or `None` when Redis has no
/// group, which happens after someone deletes the stream.  The group is
/// created again, so the next command finds it.
pub(super) async fn regrouping<T>(docket: &Docket, reply: RedisResult<T>) -> Result<Option<T>> {
    regroup(docket, reply.as_ref().err()).await?;
    Ok(reply.ok())
}

/// Creates the group again when `error` says Redis has none, and passes on
/// any other error.
async fn regroup(docket: &Docket, error: Option<&redis::RedisError>) -> Result<()> {
    match error {
        Some(error) if error.code() == Some("NOGROUP") => {
            docket.ensure_group(&mut docket.handle()).await
        }
        Some(error) => Err(error.clone().into()),
        None => Ok(()),
    }
}

/// Turns stream entries into deliveries.  An entry that is not a task
/// message cannot run anywhere, so it is acknowledged and dropped.
pub(super) async fn deliveries(
    docket: &Docket,
    entries: impl Iterator<Item = redis::streams::StreamId>,
    redelivered: bool,
) -> Vec<Delivery> {
    let mut deliveries = Vec::new();
    for entry in entries {
        match Message::from_entry(&entry) {
            Ok(message) => deliveries.push(Delivery {
                id: entry.id,
                message,
                redelivered,
            }),
            Err(error) => {
                tracing::warn!(id = %entry.id, %error, "dropping a stream entry that is not a task");
                let stream = docket.keys().stream();
                let _: RedisResult<()> = redis::pipe()
                    .xack(&stream, WORKER_GROUP, &[&entry.id])
                    .xdel(&stream, &[&entry.id])
                    .query_async(&mut docket.handle())
                    .await;
            }
        }
    }
    deliveries
}

/// Whether the docket holds any task, now or in the future.
async fn has_work(docket: &Docket) -> Result<bool> {
    let mut connection = docket.handle();
    let (stream, queue): (usize, usize) = redis::pipe()
        .xlen(docket.keys().stream())
        .zcard(docket.keys().queue())
        .query_async(&mut connection)
        .await?;
    Ok(stream > 0 || queue > 0)
}

fn collect(finished: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match finished {
        Ok(result) => result,
        Err(error) => Err(Error::Invalid(format!("a task's runner stopped: {error}"))),
    }
}

async fn drain(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    let mut first_error = Ok(());
    while let Some(finished) = tasks.join_next().await {
        if let Err(error) = collect(finished) {
            tracing::warn!(%error, "a task ended with an error");
            if first_error.is_ok() {
                first_error = Err(error);
            }
        }
    }
    first_error
}

/// Moves due future tasks onto the stream.
async fn scheduler(worker: Arc<Shared>) {
    let docket = &worker.docket;
    let keys = docket.keys();
    loop {
        let call = crate::scripts::StreamDueTasks {
            queue_key: keys.queue(),
            stream_key: keys.stream(),
            now_timestamp: crate::wire::seconds(chrono::Utc::now()),
            docket_prefix: keys.prefix().to_owned(),
        }
        .call();
        if let Err(error) = call.run::<redis::Value, _>(&mut docket.handle()).await {
            tracing::warn!(%error, "moving due tasks failed");
        }
        tokio::time::sleep(worker.settings.scheduling_resolution).await;
    }
}
