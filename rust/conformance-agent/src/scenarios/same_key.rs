//! Producers that add and then replace the same key at about the same time.
//!
//! Each producer adds `shared` 2 s out, which does nothing when the key is
//! already scheduled, then replaces it 4 s out.  The last replace wins.

use chrono::Utc;
use docket::{Docket, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::{Event, Events};

#[derive(Serialize, Deserialize, Task)]
#[task(name = "shared")]
struct Shared;

pub async fn produce(docket: &Docket, events: &Events) -> docket::Result<()> {
    let now = Utc::now();
    docket
        .add(Shared)
        .key("shared")
        .at(now + chrono::Duration::seconds(2))
        .await?;
    docket
        .replace(Shared, "shared", now + chrono::Duration::seconds(4))
        .await?;
    events
        .record(Event {
            event: "replaced",
            task: "shared",
            key: "shared",
            attempt: 0,
            worker: "",
        })
        .await
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket.register(move |ctx, _: Shared| {
        let events = events.clone();
        async move { events.ran(&ctx, "ran").await }
    });
    worker
}
