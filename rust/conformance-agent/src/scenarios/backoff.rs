//! A task that fails every attempt, with exponential backoff between them.

use std::time::Duration;

use docket::{Docket, ExponentialRetry, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "flaky")]
struct Flaky;

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    docket.add(Flaky).key("flaky").await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Flaky| {
            let events = events.clone();
            async move {
                events.ran(&ctx, "attempt").await?;
                Err::<(), _>(docket::Error::Invalid("flaky fails every attempt".into()))
            }
        })
        .with(ExponentialRetry::attempts(4).minimum_delay(Duration::from_millis(500)));
    worker
}
