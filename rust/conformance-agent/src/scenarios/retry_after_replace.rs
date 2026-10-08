//! A retried task whose first run replaces its own key, then fails.
//!
//! Only the first attempt replaces the key, so a retry that took the key
//! back would run the first run again instead of the replacement.

use chrono::Utc;
use docket::{Docket, Retry, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::{Event, Events};

#[derive(Serialize, Deserialize, Task)]
#[task(name = "flaky")]
struct Flaky {
    round: String,
}

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    let first = Flaky {
        round: "first".to_owned(),
    };
    docket.add(first).key("flaky").await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, args: Flaky| {
            let events = events.clone();
            async move {
                events.ran(&ctx, &args.round).await?;
                if args.round != "first" {
                    return Ok(());
                }
                if ctx.attempt() == 1 {
                    let replacement = Flaky {
                        round: "replacement".to_owned(),
                    };
                    let later = Utc::now() + chrono::Duration::seconds(1);
                    ctx.docket().replace(replacement, ctx.key(), later).await?;
                    events
                        .record(Event {
                            event: "replaced",
                            task: "flaky",
                            key: ctx.key(),
                            attempt: 0,
                            worker: "",
                        })
                        .await?;
                }
                Err(docket::Error::Invalid("the first run fails".into()))
            }
        })
        .with(Retry::attempts(2));
    worker
}
