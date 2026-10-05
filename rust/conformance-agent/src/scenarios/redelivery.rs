//! A slow task whose worker the driver kills while the task runs.

use std::time::Duration;

use docket::{Docket, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "interrupted")]
struct Interrupted;

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    docket.add(Interrupted).key("interrupted").await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket.register(move |ctx, _: Interrupted| {
        let events = events.clone();
        async move {
            events.ran(&ctx, "started").await?;
            tokio::time::sleep(Duration::from_secs(3)).await;
            events.ran(&ctx, "finished").await
        }
    });
    // Another worker takes the task over once the dead worker's lease runs out.
    worker.redelivery_timeout(Duration::from_secs(2))
}
