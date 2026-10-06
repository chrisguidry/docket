//! An automatic perpetual task, which the worker schedules when it starts.

use std::time::Duration;

use docket::{Docket, Perpetual, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

#[derive(Default, Serialize, Deserialize, Task)]
#[task(name = "tick")]
struct Tick;

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Tick| {
            let events = events.clone();
            async move { events.ran(&ctx, "ran").await }
        })
        .with(Perpetual::every(Duration::from_millis(500)).automatic());
    // The driver can kill the worker in the middle of a run, and the new
    // worker takes the task over only when the dead worker's lease runs out.
    worker.redelivery_timeout(Duration::from_secs(2))
}
