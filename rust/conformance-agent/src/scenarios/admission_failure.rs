//! A task whose concurrency limit counts by a field the task doesn't have.

use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, Retry, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "unlimited")]
struct Unlimited;

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    docket.add(Unlimited).key("unlimited").await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Unlimited| {
            let events = events.clone();
            async move { events.ran(&ctx, "ran").await }
        })
        .with(ConcurrencyLimit::per_field("customer", 1))
        .with(Retry::attempts(3).delay(Duration::from_millis(100)));
    worker
}
