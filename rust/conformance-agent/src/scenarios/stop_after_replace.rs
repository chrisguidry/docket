//! A perpetual task whose first run replaces its own key, then stops itself.
//!
//! The replacement stops the task too, so the key runs exactly twice.

use std::time::Duration;

use chrono::Utc;
use docket::{Docket, Perpetual, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::{Event, Events};

#[derive(Serialize, Deserialize, Task)]
#[task(name = "stopping")]
struct Stopping {
    round: String,
}

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    let first = Stopping {
        round: "first".to_owned(),
    };
    docket.add(first).key("stopping").await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, args: Stopping| {
            let events = events.clone();
            async move {
                events.ran(&ctx, &args.round).await?;
                if args.round == "first" {
                    let replacement = Stopping {
                        round: "replacement".to_owned(),
                    };
                    let later = Utc::now() + chrono::Duration::seconds(1);
                    ctx.docket().replace(replacement, ctx.key(), later).await?;
                    events
                        .record(Event {
                            event: "replaced",
                            task: "stopping",
                            key: ctx.key(),
                            attempt: 0,
                            worker: "",
                        })
                        .await?;
                }
                if let Some(perpetual) = ctx.perpetual() {
                    perpetual.cancel();
                }
                Ok::<_, docket::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    worker
}
