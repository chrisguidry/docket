//! A future task cancelled before it is due, then a task due after it.
//!
//! When the driver sees the sentinel run, the doomed task's time has passed,
//! so a cancel that did not work shows up as a run of the doomed task.

use std::time::Duration;

use docket::{Docket, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::{Event, Events};

#[derive(Serialize, Deserialize, Task)]
#[task(name = "doomed")]
struct Doomed;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "sentinel")]
struct Sentinel;

pub async fn produce(docket: &Docket, events: &Events) -> docket::Result<()> {
    docket
        .add(Doomed)
        .key("doomed")
        .after(Duration::from_millis(1500))
        .await?;
    docket
        .add(Sentinel)
        .key("sentinel")
        .after(Duration::from_millis(2500))
        .await?;
    docket.cancel("doomed").await?;
    events
        .record(Event {
            event: "cancelled",
            task: "doomed",
            key: "doomed",
            attempt: 0,
            worker: "",
        })
        .await
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let doomed = events.clone();
    docket.register(move |ctx, _: Doomed| {
        let events = doomed.clone();
        async move { events.ran(&ctx, "ran").await }
    });
    let sentinel = events.clone();
    docket.register(move |ctx, _: Sentinel| {
        let events = sentinel.clone();
        async move { events.ran(&ctx, "ran").await }
    });
    worker
}
