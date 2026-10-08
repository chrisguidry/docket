//! A small application's tasks, and the tests it writes for them on
//! `memory://`, with nothing to install.
//!
//! The application syncs its inventory every hour and emails receipts,
//! retrying when the mail server turns one away.  Its tests:
//!
//! - call the sync handler straight, with a [`ContextBuilder`], and read
//!   back the next run it chose,
//! - run three hourly syncs, and a receipt's retries ten minutes apart,
//!   without waiting, by skipping the time when nothing is due,
//! - assert on the scheduled tasks by their types,
//! - read the counts the handlers keep in the docket's own Redis, and
//! - stop the worker on the application's own shutdown.
//!
//! `cargo test --example testing --features cli,memory` runs the tests,
//! and `cargo run --example testing --features cli,memory -- --help` shows
//! the worker's options.

use std::time::Duration;

use clap::Parser;
use docket::behaviors::BoxError;
use docket::cli::WorkerArgs;
use docket::{Context, Docket, Perpetual, Retry, Task};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

/// Syncs one page of inventory, then the next page an hour later, and
/// stops after the last page.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "sync-inventory")]
struct SyncInventory {
    page: u32,
}

const LAST_PAGE: u32 = 3;

/// Emails the receipt for an order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "send-receipt")]
struct SendReceipt {
    order: u32,
}

/// The hash where the handlers count what they did, next to the docket's
/// own keys.
fn counts(docket: &Docket) -> String {
    format!("{}:shop-counts", docket.name())
}

async fn sync_inventory(ctx: Context, args: SyncInventory) -> Result<(), BoxError> {
    let mut redis = ctx.docket().redis().await?;
    let () = redis.hincr(counts(ctx.docket()), "syncs", 1).await?;
    let next = ctx.perpetual().expect("the sync is perpetual");
    if args.page == LAST_PAGE {
        next.cancel();
    } else {
        next.perpetuate(&SyncInventory {
            page: args.page + 1,
        });
    }
    Ok(())
}

/// The mail server turns away every first try, so that the retry shows.
async fn send_receipt(ctx: Context, _args: SendReceipt) -> Result<(), BoxError> {
    if ctx.attempt() == 1 {
        return Err("the mail server is busy".into());
    }
    let mut redis = ctx.docket().redis().await?;
    let () = redis.hincr(counts(ctx.docket()), "receipts", 1).await?;
    Ok(())
}

/// Registers the application's tasks, for its worker and its tests alike.
fn register(docket: &Docket) {
    docket
        .register(sync_inventory)
        .with(Perpetual::every(Duration::from_hours(1)));
    docket
        .register(send_receipt)
        .with(Retry::attempts(3).delay(Duration::from_mins(10)));
}

/// Runs the shop's worker.
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    worker: WorkerArgs,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let docket = cli
        .worker
        .docket_builder()
        .execution_ttl(Duration::from_hours(1))
        .connect()
        .await?;
    register(&docket);
    docket
        .add(SyncInventory::default())
        .key("sync-inventory")
        .await?;
    cli.worker.run(&docket).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use docket::Worker;
    use docket::testing::{ContextBuilder, assert_not_scheduled, assert_scheduled, skip_idle_time};

    use super::*;

    /// A docket of the test's own, so that tests running at once share
    /// nothing.
    async fn shop() -> Docket {
        let url = format!("memory://{}", uuid::Uuid::now_v7());
        let docket = Docket::connect("shop", url).await.unwrap();
        register(&docket);
        docket
    }

    /// A worker that checks for due tasks often.  With the defaults, each
    /// skip to the next due task still waits up to a quarter second for
    /// the scheduler's next pass.
    fn worker(docket: &Docket) -> Worker {
        Worker::new(docket.clone())
            .minimum_check_interval(Duration::from_millis(10))
            .scheduling_resolution(Duration::from_millis(10))
    }

    async fn count(docket: &Docket, field: &str) -> u32 {
        let mut redis = docket.redis().await.unwrap();
        redis.hget(counts(docket), field).await.unwrap()
    }

    #[tokio::test]
    async fn a_sync_goes_on_to_the_next_page() {
        let docket = shop().await;
        let args = SyncInventory { page: 1 };
        let ctx = ContextBuilder::new(&docket, &args).perpetual().build();

        sync_inventory(ctx.clone(), args).await.unwrap();

        let next = ctx.perpetual().unwrap();
        assert_eq!(
            next.next_args::<SyncInventory>(),
            Some(SyncInventory { page: 2 })
        );
    }

    #[tokio::test]
    async fn the_sync_stops_after_the_last_page() {
        let docket = shop().await;
        let args = SyncInventory { page: LAST_PAGE };
        let ctx = ContextBuilder::new(&docket, &args).perpetual().build();

        sync_inventory(ctx.clone(), args).await.unwrap();

        assert!(ctx.perpetual().unwrap().is_cancelled());
    }

    #[tokio::test]
    async fn the_hourly_sync_runs_every_page_without_waiting() {
        let docket = shop().await;
        skip_idle_time(&docket);
        docket
            .add(SyncInventory::default())
            .key("sync-inventory")
            .await
            .unwrap();
        assert_scheduled::<SyncInventory>(&docket, "sync-inventory").await;

        worker(&docket).run_until_finished().await.unwrap();

        assert_eq!(count(&docket, "syncs").await, LAST_PAGE + 1);
        assert_not_scheduled::<SyncInventory>(&docket, "sync-inventory").await;
    }

    #[tokio::test]
    async fn a_receipt_is_sent_on_its_retry_ten_minutes_later() {
        let docket = shop().await;
        skip_idle_time(&docket);
        let receipt = docket.add(SendReceipt { order: 7 }).await.unwrap();

        worker(&docket).run_until_finished().await.unwrap();

        receipt.result().await.unwrap();
        assert_eq!(count(&docket, "receipts").await, 1);
    }

    #[tokio::test]
    async fn the_worker_stops_on_the_applications_shutdown() {
        let url = format!("memory://{}", uuid::Uuid::now_v7());
        let cli = Cli::try_parse_from(["shop", "--url", &url]).unwrap();
        let docket = cli.worker.docket().await.unwrap();
        register(&docket);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            cli.worker
                .run_until(&docket, async {
                    stopped.await.ok();
                })
                .await
        });

        stop.send(()).unwrap();

        worker.await.unwrap().unwrap();
    }
}
