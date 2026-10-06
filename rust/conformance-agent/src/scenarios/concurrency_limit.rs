//! Tasks that share a limit of two at a time, across every worker.

use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

const TASKS: usize = 12;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "limited")]
struct Limited;

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    for number in 0..TASKS {
        docket.add(Limited).key(format!("limited-{number}")).await?;
    }
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Limited| {
            let events = events.clone();
            async move {
                events.ran(&ctx, "started").await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                events.ran(&ctx, "finished").await
            }
        })
        .with(ConcurrencyLimit::new(2));
    worker.concurrency(4)
}
