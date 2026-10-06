//! Scheduling automatic perpetual tasks when a worker starts, and again
//! every minute in case one was cancelled or lost.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use redis::AsyncCommands;

use super::session::Shared;
use crate::docket::Placement;
use crate::error::Result;
use crate::execution::Message;

/// How long one worker holds the seeding lock.
const LOCK_MILLIS: u64 = 10_000;

/// Releases the lock only for the worker that holds it.
const RELEASE: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";

pub(super) async fn reseed(worker: Arc<Shared>) {
    loop {
        tokio::time::sleep(worker.settings.automatic_tasks_interval).await;
        if let Err(error) = seed(&worker).await {
            tracing::warn!(%error, "scheduling automatic tasks failed");
        }
    }
}

pub(super) async fn seed(worker: &Shared) -> Result<()> {
    let docket = &worker.docket;
    let automatic = docket.automatic_tasks();
    if automatic.is_empty() {
        return Ok(());
    }

    let keys = docket.keys();
    let lock = keys.perpetual_lock();
    let holder = format!("{}:{}", worker.settings.name, uuid::Uuid::now_v7());
    let mut connection = docket.handle();
    let taken: Option<String> = redis::cmd("SET")
        .arg(&lock)
        .arg(&holder)
        .arg("NX")
        .arg("PX")
        .arg(LOCK_MILLIS)
        .query_async(&mut connection)
        .await?;
    if taken.is_none() {
        return Ok(());
    }

    let result = async {
        for (name, automatic) in automatic {
            // The task's name is its key, so there is one copy per docket.
            let runs: HashMap<String, String> = connection.hgetall(keys.runs(&name)).await?;
            let live = runs.contains_key("known")
                || runs.get("state").map(String::as_str) == Some("running");
            if live {
                continue;
            }
            let message = Message {
                key: name.clone(),
                when: (automatic.first_run)().unwrap_or_else(Utc::now),
                function: name,
                args: automatic.args,
                attempt: 1,
                generation: 0,
            };
            if !docket.is_struck(&message) {
                docket.place(Placement::new(message, false)).await?;
            }
        }
        Result::Ok(())
    }
    .await;

    let _: redis::RedisResult<i64> = redis::Script::new(RELEASE)
        .key(&lock)
        .arg(&holder)
        .invoke_async(&mut connection)
        .await;
    result
}
