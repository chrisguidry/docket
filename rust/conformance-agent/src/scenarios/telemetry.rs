//! A small workload that touches every metric and span the driver checks.
//! pydocket's agent states the workload in full, in
//! `python/conformance-agent/scenarios/telemetry.py`; this is the same one.

use std::time::Duration;

use chrono::Utc;
use docket::{Docket, Perpetual, Retry, Strike, Task, Worker};
use serde::{Deserialize, Serialize};

use crate::events::Events;

/// A perpetual task stops itself on this run.
const PERPETUAL_RUNS: i64 = 3;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "succeed")]
struct Succeed;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "fail")]
struct Fail;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "flaky")]
struct Flaky;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "perpetual")]
struct Repeat;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "later")]
struct Later;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "doomed")]
struct Doomed;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "struck")]
struct Struck;

fn boom() -> std::io::Error {
    std::io::Error::other("boom")
}

pub async fn produce(docket: &Docket) -> docket::Result<()> {
    let prefix = std::env::var("CONFORMANCE_PHASE").expect("the driver names the phase");
    let key = |name: &str| format!("{prefix}:{name}");
    let an_hour = Duration::from_secs(3600);

    docket.add(Succeed).key(key("succeed")).await?;
    docket.add(Fail).key(key("fail")).await?;
    docket.add(Flaky).key(key("flaky")).await?;
    docket.add(Repeat).key(key("perpetual")).await?;

    docket.add(Doomed).key(key("doomed")).after(an_hour).await?;
    docket.cancel(&key("doomed")).await?;

    docket.add(Later).key(key("later")).after(an_hour).await?;
    docket.replace(Later, key("later"), Utc::now()).await?;

    docket.strike(Strike::task::<Struck>()).await?;
    docket.add(Struck).key(key("struck")).await?;
    docket.restore(Strike::task::<Struck>()).await?;
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let ran = |events: &Events| {
        let events = events.clone();
        move |ctx: docket::Context| {
            let events = events.clone();
            async move { events.ran(&ctx, "ran").await }
        }
    };

    let succeed = ran(events);
    docket.register(move |ctx, _: Succeed| succeed(ctx));
    let later = ran(events);
    docket.register(move |ctx, _: Later| later(ctx));
    let doomed = ran(events);
    docket.register(move |ctx, _: Doomed| doomed(ctx));
    let struck = ran(events);
    docket.register(move |ctx, _: Struck| struck(ctx));

    let fail = ran(events);
    docket.register(move |ctx, _: Fail| {
        let ran = fail(ctx);
        async move {
            ran.await?;
            Err::<(), _>(Box::new(boom()) as docket::behaviors::BoxError)
        }
    });

    let flaky = ran(events);
    docket
        .register(move |ctx: docket::Context, _: Flaky| {
            let first = ctx.attempt() == 1;
            let ran = flaky(ctx);
            async move {
                ran.await?;
                if first {
                    return Err(Box::new(boom()) as docket::behaviors::BoxError);
                }
                Ok(())
            }
        })
        .with(Retry::attempts(2));

    let events = events.clone();
    docket
        .register(move |ctx: docket::Context, _: Repeat| {
            let events = events.clone();
            async move {
                events.ran(&ctx, "ran").await?;
                // Each run is a new execution, so only Redis can count the
                // runs.  Each implementation has its own docket and the same
                // keys, so the counter belongs to the docket.
                let hash = format!("conformance:telemetry:{}:runs", ctx.docket().name());
                if events.increment(&hash, ctx.key()).await? >= PERPETUAL_RUNS {
                    ctx.perpetual()
                        .expect("a perpetual task has its control")
                        .cancel();
                }
                Ok::<_, docket::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_millis(100)));
    worker
}
