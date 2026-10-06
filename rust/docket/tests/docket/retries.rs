use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::behaviors::ForcedRetry;
use docket::{ExponentialRetry, Retry, State};

use crate::support::{Noop, docket, within, worker};

fn failing(
    attempts: &Arc<AtomicU32>,
    succeed_on: u32,
) -> impl Fn(docket::Context, Noop) -> std::future::Ready<Result<(), std::io::Error>> + use<> {
    let attempts = Arc::clone(attempts);
    move |ctx, _| {
        attempts.fetch_add(1, Ordering::SeqCst);
        std::future::ready(if ctx.attempt() >= succeed_on {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "attempt {} failed",
                ctx.attempt()
            )))
        })
    }
}

#[tokio::test]
async fn retry_runs_a_failing_task_again() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket
        .register(failing(&attempts, 3))
        .with(Retry::attempts(3));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_task_fails_after_its_last_attempt() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket
        .register(failing(&attempts, 99))
        .with(Retry::attempts(2));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert_eq!(status.error.as_deref(), Some("attempt 2 failed"));
    let error = execution.result().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("task {} failed: attempt 2 failed", execution.key())
    );
}

#[tokio::test]
async fn without_a_retry_a_task_fails_at_once() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket.register(failing(&attempts, 99));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exponential_retry_waits_longer_each_time() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket
        .register(failing(&attempts, 3))
        .with(ExponentialRetry::attempts(3).minimum_delay(Duration::from_millis(100)));
    docket.add(Noop).await.unwrap();

    let started = tokio::time::Instant::now();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    // 100 ms after the first attempt, then 200 ms after the second.
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_forced_retry_spends_an_attempt() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&attempts);
    docket
        .register(move |ctx, _: Noop| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                if ctx.attempt() < 2 {
                    return Err(ForcedRetry::after(Duration::from_millis(50)));
                }
                Ok(())
            }
        })
        .with(Retry::forever());
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_panicking_handler_fails_the_task() {
    let docket = docket().await;
    docket.register(|_ctx, _: Noop| async move { explode() });
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert_eq!(
        status.error.as_deref(),
        Some("the task's handler panicked: the handler blew up")
    );
}

fn explode() -> Result<(), std::io::Error> {
    panic!("the handler blew up")
}

#[tokio::test]
async fn retry_waits_its_delay_between_attempts() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket
        .register(failing(&attempts, 2))
        .with(Retry::attempts(2).delay(Duration::from_millis(200)));
    docket.add(Noop).await.unwrap();

    let started = tokio::time::Instant::now();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn exponential_retry_can_retry_forever() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    docket
        .register(failing(&attempts, 4))
        .with(ExponentialRetry::forever().minimum_delay(Duration::from_millis(10)));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
