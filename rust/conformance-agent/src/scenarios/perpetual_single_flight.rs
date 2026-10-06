//! An automatic perpetual task that several workers share.

use std::time::Duration;

use docket::{Docket, Perpetual, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

#[derive(Default, Serialize, Deserialize, Task)]
#[task(name = "beat")]
struct Beat;

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Beat| {
            let events = events.clone();
            async move {
                events.ran(&ctx, "started").await?;
                tokio::time::sleep(Duration::from_millis(200)).await;
                events.ran(&ctx, "finished").await
            }
        })
        .with(Perpetual::every(Duration::from_millis(500)).automatic());
    // The driver kills workers while they run the task, and another worker
    // takes it over once the dead worker's lease runs out.
    worker.redelivery_timeout(Duration::from_secs(2))
}
