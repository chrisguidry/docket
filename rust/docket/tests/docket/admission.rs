use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::behaviors::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks};
use docket::{Context, Cooldown, Debounce, RateLimit, State, Task};
use serde::{Deserialize, Serialize};

use crate::support::{Noop, docket, within, worker};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "per-customer")]
struct PerCustomer {
    customer: u32,
}

#[tokio::test]
async fn debounce_runs_once_after_calls_settle() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Debounce::new(Duration::from_millis(200)));
    for _ in 0..3 {
        docket.add(Noop).await.unwrap();
    }

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cooldown_drops_calls_inside_the_window() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: PerCustomer| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Cooldown::per_field("customer", Duration::from_secs(60)).scope(docket.name()));
    for customer in [1, 1, 2] {
        docket.add(PerCustomer { customer }).await.unwrap();
    }

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_rate_limit_spreads_runs_over_its_window() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(RateLimit::new(2).per(Duration::from_millis(300)));
    for _ in 0..4 {
        docket.add(Noop).await.unwrap();
    }

    let started = tokio::time::Instant::now();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn a_rate_limit_can_drop_the_excess() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: PerCustomer| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(
            RateLimit::per_field("customer", 1)
                .per(Duration::from_secs(60))
                .drop_excess()
                .scope(docket.name()),
        );
    for _ in 0..3 {
        docket.add(PerCustomer { customer: 7 }).await.unwrap();
    }

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

/// A behavior written the way a user would write one: it lets only even
/// customers through.
struct EvenCustomersOnly;

impl Admission for EvenCustomersOnly {
    async fn admit(&self, ctx: &Context) -> Result<Admitted, AdmissionBlocked> {
        match ctx.args()["customer"].as_u64() {
            Some(customer) if customer % 2 == 0 => Ok(Admitted::now()),
            _ => Err(AdmissionBlocked::new("odd customer").drop_task()),
        }
    }
}

impl<T: Task> Behavior<T> for EvenCustomersOnly {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

#[tokio::test]
async fn users_can_write_their_own_admission_behavior() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: PerCustomer| async { Ok::<_, std::io::Error>(()) })
        .with(EvenCustomersOnly);
    let even = docket.add(PerCustomer { customer: 2 }).await.unwrap();
    let odd = docket.add(PerCustomer { customer: 3 }).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        even.status().await.unwrap().unwrap().state,
        State::Completed
    );
    assert_eq!(odd.status().await.unwrap().unwrap().state, State::Cancelled);
}

#[tokio::test]
async fn a_later_block_gives_back_an_earlier_admission() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: PerCustomer| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(
            RateLimit::new(1)
                .per(Duration::from_secs(60))
                .scope(docket.name()),
        )
        .with(EvenCustomersOnly);
    docket.add(PerCustomer { customer: 3 }).await.unwrap();
    let even = docket.add(PerCustomer { customer: 4 }).await.unwrap();

    within(10, worker(&docket).concurrency(1).run_until_finished())
        .await
        .unwrap();

    // The odd customer's run took the one place in the window, then gave it
    // back when it was dropped, so the even customer still ran.
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        even.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
