//! Telling the docket that this worker is alive, and which tasks it runs.

use std::sync::Arc;

use chrono::Utc;
use redis::RedisResult;

use super::session::Shared;
use crate::wire::seconds;

pub(super) async fn beat(worker: Arc<Shared>) {
    let interval = worker.docket.settings().heartbeat_interval;
    loop {
        if let Err(error) = once(&worker).await {
            worker.disrupted();
            tracing::error!(%error, "Error sending worker heartbeat");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn once(worker: &Shared) -> crate::Result<()> {
    let docket = &worker.docket;
    let keys = docket.keys();
    let name = &worker.settings.name;
    let now = seconds(Utc::now());
    let window = docket.heartbeat_window();
    let oldest = now - window.as_secs_f64();
    let tasks = docket.task_names();

    // Each command is correct on its own, so the pipeline is not a
    // transaction.  After a cluster moves the docket's slot, it aborts a
    // transaction whose commands it redirects, and redis-rs neither follows
    // those redirects nor refreshes its slot map.  Each heartbeat would fail
    // until some other command on the connection refreshed the map.
    let mut pipeline = redis::pipe();
    pipeline
        .zrembyscore(keys.workers(), 0, oldest)
        .ignore()
        .zadd(keys.workers(), name, now)
        .ignore();
    for task in &tasks {
        pipeline
            .zrembyscore(keys.task_workers(task), 0, oldest)
            .ignore()
            .zadd(keys.task_workers(task), name, now)
            .ignore();
    }
    if !tasks.is_empty() {
        pipeline.sadd(keys.worker_tasks(name), &tasks).ignore();
    }
    let ttl = i64::try_from(window.as_secs()).unwrap_or(i64::MAX).max(1);
    pipeline.expire(keys.worker_tasks(name), ttl).ignore();

    let mut connection = docket.handle();
    let () = pipeline.query_async(&mut connection).await?;

    // The depths ride on the heartbeat, so each worker reports them as
    // often as it reports that it is alive.
    let (stream, due, future): (u64, u64, u64) = redis::pipe()
        .xlen(keys.stream())
        .zcount(keys.queue(), 0, now)
        .zcount(keys.queue(), now, "+inf")
        .query_async(&mut connection)
        .await?;
    let metrics = &docket.telemetry().metrics;
    let labels = docket.labels();
    metrics.queue_depth.record(stream + due, &labels);
    metrics.schedule_depth.record(future, &labels);
    Ok(())
}

/// Takes this worker off the docket's list when it stops.
pub(super) async fn remove(worker: &Shared) {
    let docket = &worker.docket;
    let keys = docket.keys();
    let name = &worker.settings.name;
    let mut pipeline = redis::pipe();
    pipeline.zrem(keys.workers(), name).ignore();
    for task in docket.task_names() {
        pipeline.zrem(keys.task_workers(&task), name).ignore();
    }
    pipeline.del(keys.worker_tasks(name)).ignore();
    let removed: RedisResult<()> = pipeline.query_async(&mut docket.handle()).await;
    // A worker that loses Redis on the way out ages out of the list on its
    // own, because every heartbeat prunes the members it has not heard from.
    if removed.is_err() {
        tracing::debug!("Could not clear worker heartbeat, Redis is unavailable");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::once;
    use crate::Docket;
    use crate::connection::slots;
    use crate::worker::Worker;
    use crate::worker::session::Shared;

    /// A cluster that moves the docket's slot answers the heartbeat's
    /// commands with redirects, which the heartbeat follows.
    #[tokio::test]
    async fn a_heartbeat_follows_its_slot_to_another_node() {
        let Some(url) = slots::cluster_url() else {
            return;
        };
        let name = format!("heartbeat-{}", uuid::Uuid::now_v7());
        let docket = Docket::connect(name, &url).await.unwrap();
        let settings = Worker::new(docket.clone()).settings;
        let worker = Shared::new(docket.clone(), settings, docket.limit_runs(&HashMap::new()));
        once(&worker).await.unwrap();

        slots::move_slot(&url, &docket.keys().workers()).await;
        once(&worker).await.unwrap();
    }
}
