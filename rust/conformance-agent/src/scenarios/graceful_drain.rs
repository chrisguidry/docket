//! Slow tasks that a worker must finish after SIGTERM or SIGINT.

use std::time::Duration;

use docket::{Docket, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

const TASKS_PER_PRODUCE: usize = 4;
const TASK_SECONDS: u64 = 3;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "slow")]
struct Slow;

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    for _ in 0..TASKS_PER_PRODUCE {
        docket
            .add(Slow)
            .key(format!("slow-{}", uuid::Uuid::now_v7()))
            .await?;
    }
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket.register(move |ctx, _: Slow| {
        let events = events.clone();
        async move {
            events.ran(&ctx, "started").await?;
            tokio::time::sleep(Duration::from_secs(TASK_SECONDS)).await;
            events.ran(&ctx, "finished").await
        }
    });
    worker.concurrency(2)
}
