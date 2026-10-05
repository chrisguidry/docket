use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use docket::{ConcurrencyLimit, Docket, State, Task};
use serde::{Deserialize, Serialize};

use crate::support::{Noop, docket, within, worker};

/// Counts how many handlers run at once, and the most that ever did.
#[derive(Default)]
struct Overlap {
    running: AtomicU32,
    most: AtomicU32,
}

impl Overlap {
    async fn hold(&self, duration: Duration) {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(duration).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Registers a task that holds its slot for `duration`, under `limit`.
fn overlapping<T: Task<Output = ()>>(
    docket: &Docket,
    limit: ConcurrencyLimit,
    duration: Duration,
) -> Arc<Overlap> {
    let overlap = Arc::new(Overlap::default());
    let held = Arc::clone(&overlap);
    docket
        .register(move |_ctx, _: T| {
            let held = Arc::clone(&held);
            async move {
                held.hold(duration).await;
                Ok::<_, std::io::Error>(())
            }
        })
        .with(limit);
    overlap
}

#[tokio::test]
async fn a_concurrency_limit_caps_the_task() {
    let docket = docket().await;
    let overlap = overlapping::<Noop>(&docket, ConcurrencyLimit::new(2), Duration::from_millis(50));
    for _ in 0..6 {
        docket.add(Noop).await.unwrap();
    }

    within(30, worker(&docket).concurrency(6).run_until_finished())
        .await
        .unwrap();

    assert_eq!(overlap.most.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "per-customer")]
struct PerCustomer {
    customer: u32,
}

#[tokio::test]
async fn a_per_field_limit_counts_each_value_apart() {
    let docket = docket().await;
    let overlap = overlapping::<PerCustomer>(
        &docket,
        ConcurrencyLimit::per_field("customer", 1).scope("tests"),
        Duration::from_millis(50),
    );
    for customer in [1, 1, 2, 2] {
        docket.add(PerCustomer { customer }).await.unwrap();
    }

    within(30, worker(&docket).concurrency(4).run_until_finished())
        .await
        .unwrap();

    assert_eq!(overlap.most.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "per-account")]
struct PerAccount {
    account: String,
}

#[tokio::test]
async fn a_per_field_limit_counts_string_values() {
    let docket = docket().await;
    let overlap = overlapping::<PerAccount>(
        &docket,
        ConcurrencyLimit::per_field("account", 1),
        Duration::from_millis(50),
    );
    for account in ["acme", "acme", "initech"] {
        docket
            .add(PerAccount {
                account: account.to_owned(),
            })
            .await
            .unwrap();
    }

    within(30, worker(&docket).concurrency(3).run_until_finished())
        .await
        .unwrap();

    assert_eq!(overlap.most.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_limit_on_a_missing_field_drops_the_task() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(ConcurrencyLimit::per_field("customer", 1));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[tokio::test]
async fn a_task_renews_its_slot_while_it_runs() {
    let docket = docket().await;
    // Each run lasts longer than the redelivery timeout, so only renewal
    // keeps its slot from looking abandoned to the waiter's safeguard.
    let overlap = overlapping::<Noop>(
        &docket,
        ConcurrencyLimit::new(1),
        Duration::from_millis(600),
    );
    for _ in 0..2 {
        docket.add(Noop).await.unwrap();
    }

    within(
        30,
        worker(&docket)
            .concurrency(4)
            .redelivery_timeout(Duration::from_millis(200))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(overlap.most.load(Ordering::SeqCst), 1);
}

/// Whether the docket has a safeguard scheduled for a parked task.
async fn safeguard_scheduled(docket: &Docket) -> bool {
    let snapshot = docket.snapshot().await.unwrap();
    snapshot
        .future
        .iter()
        .any(|task| task.key.starts_with("__safeguard__:"))
}

#[tokio::test]
async fn the_safeguard_wakes_a_waiter_after_its_slot_holder_dies() {
    let docket = docket().await;
    let hang = Arc::new(AtomicBool::new(true));
    let hanging = Arc::clone(&hang);
    docket
        .register(move |_ctx, _: Noop| {
            let hang = hanging.swap(false, Ordering::SeqCst);
            async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                Ok::<_, std::io::Error>(())
            }
        })
        .with(ConcurrencyLimit::new(1));
    let holder = docket.add(Noop).await.unwrap();
    let waiter = docket.add(Noop).await.unwrap();

    let dying = tokio::spawn(
        worker(&docket)
            .concurrency(2)
            .redelivery_timeout(Duration::from_millis(300))
            .run_forever(),
    );
    within(10, async {
        while !safeguard_scheduled(&docket).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    dying.abort();

    within(
        30,
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(300))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(
        holder.status().await.unwrap().unwrap().state,
        State::Completed
    );
    assert_eq!(
        waiter.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
