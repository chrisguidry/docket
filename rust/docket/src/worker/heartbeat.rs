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
            tracing::warn!(%error, "the heartbeat failed");
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

    let mut pipeline = redis::pipe();
    pipeline
        .atomic()
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
    let _: RedisResult<()> = pipeline.query_async(&mut docket.handle()).await;
}
