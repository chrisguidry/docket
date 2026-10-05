use std::time::Duration;

use docket::{Retry, State, Timeout};

use crate::support::{Noop, docket, within, worker};

#[tokio::test]
async fn a_task_past_its_deadline_fails() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<_, std::io::Error>(())
        })
        .with(Timeout::after(Duration::from_millis(100)));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(status.state, State::Failed);
    assert_eq!(
        status.error,
        Some(format!(
            "Docket task {} exceeded timeout of 0.1s",
            execution.key()
        ))
    );
}

#[tokio::test]
async fn a_task_can_extend_its_deadline() {
    let docket = docket().await;
    docket
        .register(|ctx: docket::Context, _: Noop| async move {
            let timeout = ctx.timeout().expect("the task has a timeout");
            assert!(timeout.remaining() <= Duration::from_millis(200));
            tokio::time::sleep(Duration::from_millis(150)).await;
            timeout.extend(Duration::from_millis(200));
            timeout.extend_by_base();
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, std::io::Error>(())
        })
        .with(Timeout::after(Duration::from_millis(200)));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_timed_out_task_can_be_retried() {
    let docket = docket().await;
    docket
        .register(|ctx: docket::Context, _: Noop| async move {
            if ctx.attempt() == 1 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Ok::<_, std::io::Error>(())
        })
        .with(Timeout::after(Duration::from_millis(100)))
        .with(Retry::attempts(2));
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_task_without_a_timeout_has_no_deadline() {
    let docket = docket().await;
    docket.register(|ctx: docket::Context, _: Noop| async move {
        assert!(ctx.timeout().is_none());
        Ok::<_, std::io::Error>(())
    });
    let execution = docket.add(Noop).await.unwrap();
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
