use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::behaviors::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks, NotAdmitted};
use docket::{Context, Cooldown, Debounce, RateLimit, Retry, State, Task};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::logs::Logs;
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
    async fn admit(&self, ctx: &Context) -> Result<Admitted, NotAdmitted> {
        match ctx.args()["customer"].as_u64() {
            Some(customer) if customer % 2 == 0 => Ok(Admitted::now()),
            _ => Err(AdmissionBlocked::new("odd customer").drop_task().into()),
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

/// A behavior whose check always fails, the way a check fails when Redis
/// refuses it.  It counts how often it was asked.
struct Unanswerable(Arc<AtomicU32>);

impl Admission for Unanswerable {
    async fn admit(&self, _ctx: &Context) -> Result<Admitted, NotAdmitted> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(NotAdmitted::failed("the check could not run"))
    }
}

impl<T: Task> Behavior<T> for Unanswerable {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

#[tokio::test]
async fn an_admission_that_fails_fails_the_task_through_its_retry() {
    let docket = docket().await;
    let checks = Arc::new(AtomicU32::new(0));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Unanswerable(Arc::clone(&checks)))
        .with(Retry::attempts(3));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(checks.load(Ordering::SeqCst), 3);
    assert_eq!(
        (status.state, status.error.as_deref()),
        (State::Failed, Some("the check could not run"))
    );
}

/// A behavior that blocks a task once, without saying when to try again,
/// and then admits it.
struct NotYet(Arc<AtomicU32>);

impl Admission for NotYet {
    async fn admit(&self, _ctx: &Context) -> Result<Admitted, NotAdmitted> {
        match self.0.fetch_add(1, Ordering::SeqCst) {
            0 => Err(AdmissionBlocked::new("not yet").into()),
            _ => Ok(Admitted::now()),
        }
    }
}

impl<T: Task> Behavior<T> for NotYet {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

#[tokio::test]
async fn a_block_without_a_delay_tries_the_task_again_shortly() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let checks = Arc::new(AtomicU32::new(0));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(NotYet(Arc::clone(&checks)));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(checks.load(Ordering::SeqCst), 2);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
    assert!(logs.contains(&format!(
        "⏳ Task {} blocked by admission control, rescheduling",
        execution.key()
    )));
}

/// pydocket checks every admission before it acts on any, so a block after
/// a failure still wins, and the check after a failure still runs.
#[tokio::test]
async fn a_block_wins_over_a_failed_admission() {
    let docket = docket().await;
    let failures = Arc::new(AtomicU32::new(0));
    let blocks = Arc::new(AtomicU32::new(0));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Unanswerable(Arc::clone(&failures)))
        .with(NotYet(Arc::clone(&blocks)));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    // The first attempt was blocked, and the second one failed.
    assert_eq!(failures.load(Ordering::SeqCst), 2);
    assert_eq!(blocks.load(Ordering::SeqCst), 2);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Failed
    );
}

/// A failed admission counts as a run that failed, so a rate limit keeps
/// the call it gave, as in pydocket.
#[tokio::test]
async fn a_failed_admission_spends_a_rate_limit_call() {
    let docket = docket().await;
    let failures = Arc::new(AtomicU32::new(0));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(
            RateLimit::new(1)
                .per(Duration::from_secs(60))
                .drop_excess()
                .scope(docket.name()),
        )
        .with(Unanswerable(Arc::clone(&failures)))
        .with(Retry::attempts(2));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    // The retry found the window full, and the rate limit dropped it.
    assert_eq!(failures.load(Ordering::SeqCst), 2);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

/// A behavior whose check takes a while and then fails, so a cancel can
/// arrive while it runs.
struct SlowlyUnanswerable {
    checking: Arc<Notify>,
    checks: Arc<AtomicU32>,
}

impl Admission for SlowlyUnanswerable {
    async fn admit(&self, _ctx: &Context) -> Result<Admitted, NotAdmitted> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.checking.notify_one();
        tokio::time::sleep(Duration::from_millis(500)).await;
        Err(NotAdmitted::failed("the check could not run"))
    }
}

impl<T: Task> Behavior<T> for SlowlyUnanswerable {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

#[tokio::test]
async fn a_cancel_during_a_failing_admission_is_not_retried() {
    let docket = docket().await;
    let checking = Arc::new(Notify::new());
    let checks = Arc::new(AtomicU32::new(0));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(SlowlyUnanswerable {
            checking: Arc::clone(&checking),
            checks: Arc::clone(&checks),
        })
        .with(Retry::attempts(3));
    let execution = docket.add(Noop).await.unwrap();

    let run = tokio::spawn(worker(&docket).run_until_finished());
    within(10, checking.notified()).await;
    docket.cancel(execution.key()).await.unwrap();
    within(10, run).await.unwrap().unwrap();

    assert_eq!(checks.load(Ordering::SeqCst), 1);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}
