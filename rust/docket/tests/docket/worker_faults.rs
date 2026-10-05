//! What a worker does when Redis refuses one of its commands.  These tests
//! need a plain Redis behind the fault proxy, so they do nothing on the
//! in-process engine or a cluster.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use docket::behaviors::{
    AfterCompletion, AfterFailure, Behavior, BoxError, Completion, Failure, Hooks, Outcome,
};
use docket::{Context, Docket, Registration, State, Strike, Task, Worker};
use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::Semaphore;

use crate::logs::Logs;
use crate::support::proxy::Proxy;
use crate::support::{Echo, Noop, docket_through, proxy, within, worker};

/// A worker that takes over a lost delivery and reconnects quickly.
fn hasty(docket: &Docket) -> Worker {
    worker(docket)
        .redelivery_timeout(Duration::from_millis(200))
        .reconnection_delay(Duration::from_millis(50))
}

#[rstest::rstest]
#[case::creating_the_group(|proxy: &Proxy| proxy.fail("XGROUP", 1), "injected failure for XGROUP")]
#[case::reading(|proxy: &Proxy| proxy.fail("XREADGROUP", 1), "injected failure for XREADGROUP")]
#[case::taking_the_sweep_lease(|proxy: &Proxy| proxy.fail("SET", 1), "injected failure for SET")]
#[case::sweeping(|proxy: &Proxy| proxy.fail("XAUTOCLAIM", 1), "injected failure for XAUTOCLAIM")]
#[case::checking_for_work(|proxy: &Proxy| proxy.fail("XLEN", 1), "injected failure for XLEN")]
#[case::claiming(
    |proxy: &Proxy| proxy.fail_script(include_str!("../../lua/claim.lua"), 1),
    "injected failure for EVALSHA"
)]
#[case::moving_due_tasks(
    |proxy: &Proxy| proxy.fail_script(include_str!("../../lua/stream_due_tasks.lua"), 1),
    "moving due tasks failed"
)]
#[case::beating(|proxy: &Proxy| proxy.fail("MULTI", 1), "the heartbeat failed")]
#[tokio::test]
async fn a_worker_finishes_its_tasks_after_redis_refuses_a_command(
    #[case] arm: fn(&Proxy),
    #[case] logged: &str,
) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let execution = docket.add(Noop).await.unwrap();
    arm(&proxy);

    within(20, hasty(&docket).concurrency(1).run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains(logged));
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

/// Ends each run the same way.
struct Then(AfterCompletion);

impl Completion for Then {
    async fn on_complete(&self, _: &Context, _: &Outcome) -> AfterCompletion {
        self.0.clone()
    }
}

impl<T: Task> Behavior<T> for Then {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.completion(self);
    }
}

/// Retries every failure at once.
struct RetryNow;

impl Failure for RetryNow {
    async fn on_failure(&self, _: &Context, _: &BoxError) -> AfterFailure {
        AfterFailure::RetryAt(Utc::now())
    }
}

impl<T: Task> Behavior<T> for RetryNow {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.failure(self);
    }
}

fn cancel_script(proxy: &Proxy) {
    proxy.fail_script(include_str!("../../lua/cancel_task.lua"), 1);
}

fn schedule_script(proxy: &Proxy) {
    proxy.fail_script(include_str!("../../lua/schedule.lua"), 1);
}

fn fail(text: String) -> Result<String, std::io::Error> {
    Err(std::io::Error::other(text))
}

type Attach = fn(Registration<Echo>);

#[rstest::rstest]
#[case::storing_the_result(|proxy: &Proxy| proxy.fail("SETEX", 1), |_| {}, Ok)]
#[case::cancelling_after_a_success(
    cancel_script,
    |registration: Registration<Echo>| { registration.with(Then(AfterCompletion::Cancel)); },
    Ok
)]
#[case::cancelling_after_a_failure(
    cancel_script,
    |registration: Registration<Echo>| { registration.with(Then(AfterCompletion::Cancel)); },
    fail
)]
#[case::rescheduling(
    schedule_script,
    |registration: Registration<Echo>| {
        registration.with(Then(AfterCompletion::Reschedule { when: Utc::now(), args: None }));
    },
    Ok
)]
#[case::retrying(schedule_script, |registration: Registration<Echo>| { registration.with(RetryNow); }, fail)]
#[tokio::test]
async fn a_task_runs_again_after_redis_refuses_its_ending(
    #[case] arm: fn(&Proxy),
    #[case] attach: Attach,
    #[case] outcome: fn(String) -> Result<String, std::io::Error>,
) {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    attach(docket.register(move |_ctx, args: Echo| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { outcome(args.text) }
    }));
    docket.add(Echo::new("again")).await.unwrap();
    arm(&proxy);

    let ran_twice = async {
        while runs.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    within(20, hasty(&docket).run_until(ran_twice))
        .await
        .unwrap();

    assert!(logs.contains("injected failure"));
}

#[tokio::test]
async fn a_struck_task_ends_after_redis_refuses_its_strike() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let execution = docket.add(Noop).await.unwrap();
    docket.strike(Strike::task::<Noop>()).await.unwrap();
    proxy.fail("HDEL", 1);

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[tokio::test]
async fn shutdown_logs_each_running_task_whose_ending_redis_refuses() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    let started = Arc::new(Semaphore::new(0));
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        signal.add_permits(1);
        async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, std::io::Error>(())
        }
    });
    docket.add(Noop).await.unwrap();
    docket.add(Noop).await.unwrap();
    proxy.fail_script(include_str!("../../lua/terminal.lua"), 2);

    let shutdown = async move { started.acquire_many(2).await.unwrap().forget() };
    within(20, hasty(&docket).run_until(shutdown))
        .await
        .unwrap();

    assert!(logs.contains("a task ended with an error"));
}

#[tokio::test]
async fn a_worker_logs_when_redis_refuses_to_renew_its_leases() {
    let Some(proxy) = proxy().await else { return };
    let (logs, _guard) = Logs::capture();
    let docket = docket_through(&proxy).await;
    docket.register(|_ctx, _: Noop| async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<_, std::io::Error>(())
    });
    docket.add(Noop).await.unwrap();
    proxy.fail("XCLAIM", 1);

    within(20, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains("renewing task leases failed"));
}

type Report = fn(Context) -> BoxFuture<'static, docket::Result<()>>;

#[rstest::rstest]
#[case::counting(|proxy: &Proxy| proxy.fail("HINCRBY", 1), |ctx: Context| async move {
    ctx.progress().increment(1).await
}.boxed())]
#[case::announcing(|proxy: &Proxy| proxy.fail("PUBLISH", 1), |ctx: Context| async move {
    ctx.progress().increment(1).await
}.boxed())]
#[case::reading(|proxy: &Proxy| proxy.fail("HGETALL", 1), |ctx: Context| async move {
    ctx.progress().set_total(10).await
}.boxed())]
#[case::writing(
    |proxy: &Proxy| proxy.fail_script(include_str!("../../lua/progress_write.lua"), 1),
    |ctx: Context| async move { ctx.progress().set_message(Some("halfway")).await }.boxed()
)]
#[tokio::test]
async fn a_task_fails_when_redis_refuses_its_progress(
    #[case] arm: fn(&Proxy),
    #[case] report: Report,
) {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let proxy = Arc::new(proxy);
    let armed = Arc::clone(&proxy);
    docket.register(move |ctx: Context, _: Noop| {
        arm(&armed);
        report(ctx)
    });
    let execution = docket.add(Noop).await.unwrap();

    within(10, hasty(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert!(status.error.unwrap().contains("injected failure"));
}
