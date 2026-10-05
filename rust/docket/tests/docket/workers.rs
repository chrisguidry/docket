use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::{State, Worker};
use tokio::sync::Notify;

use crate::support::{Echo, Noop, docket, within, worker};

#[tokio::test]
async fn cancel_stops_a_running_task() {
    let docket = docket().await;
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        let signal = Arc::clone(&signal);
        async move {
            signal.notify_one();
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<_, std::io::Error>(())
        }
    });
    let execution = docket.add(Noop).await.unwrap();

    let run = tokio::spawn(worker(&docket).run_until_finished());
    within(10, started.notified()).await;
    docket.cancel(execution.key()).await.unwrap();
    within(10, run).await.unwrap().unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[tokio::test]
async fn shutdown_lets_running_tasks_finish() {
    let docket = docket().await;
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        let signal = Arc::clone(&signal);
        async move {
            signal.notify_one();
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, std::io::Error>(())
        }
    });
    let execution = docket.add(Noop).await.unwrap();

    let shutdown = Arc::clone(&started);
    within(
        10,
        worker(&docket).run_until(async move { shutdown.notified().await }),
    )
    .await
    .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn another_worker_takes_over_a_task_whose_worker_died() {
    let docket = docket().await;
    let attempts = Arc::new(AtomicU32::new(0));
    let started = Arc::new(Notify::new());
    let (counted, signal) = (Arc::clone(&attempts), Arc::clone(&started));
    docket.register(move |ctx: docket::Context, _: Noop| {
        let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
        let signal = Arc::clone(&signal);
        async move {
            assert_eq!(ctx.attempt(), 1, "a redelivery is not a new attempt");
            if first {
                signal.notify_one();
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Ok::<_, std::io::Error>(())
        }
    });
    let execution = docket.add(Noop).await.unwrap();

    let doomed = tokio::spawn(
        worker(&docket)
            .name("doomed")
            .redelivery_timeout(Duration::from_millis(200))
            .run_forever(),
    );
    within(10, started.notified()).await;
    doomed.abort();

    within(
        10,
        worker(&docket)
            .name("survivor")
            .redelivery_timeout(Duration::from_millis(200))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let status = execution.status().await.unwrap().unwrap();
    assert_eq!(
        (status.state, status.worker.as_deref()),
        (State::Completed, Some("survivor"))
    );
}

#[tokio::test]
async fn a_task_with_no_handler_completes_with_a_warning() {
    let docket = docket().await;
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
async fn workers_reject_settings_they_cannot_use() {
    let docket = docket().await;
    let none = Worker::new(docket.clone())
        .concurrency(0)
        .run_until_finished()
        .await;
    let empty = Worker::new(docket)
        .message_batch(0)
        .run_until_finished()
        .await;
    assert_eq!(
        none.unwrap_err().to_string(),
        "a worker's concurrency must be at least 1"
    );
    assert_eq!(
        empty.unwrap_err().to_string(),
        "a worker's message batch must be at least 1"
    );
}

#[tokio::test]
async fn a_running_worker_reports_its_heartbeat() {
    let docket = docket().await;
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        let signal = Arc::clone(&signal);
        async move {
            signal.notify_one();
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<_, std::io::Error>(())
        }
    });
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket.add(Noop).await.unwrap();

    let run = tokio::spawn(worker(&docket).name("beating").run_until_finished());
    within(10, started.notified()).await;
    let workers = within(10, async {
        loop {
            let workers = docket.workers().await.unwrap();
            if !workers.is_empty() {
                return workers;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(workers[0].name, "beating");
    assert_eq!(workers[0].tasks, ["echo", "noop"]);
    assert_eq!(
        docket.task_workers("echo").await.unwrap()[0].name,
        "beating"
    );

    let snapshot = docket.snapshot().await.unwrap();
    assert_eq!(snapshot.running.len(), 1);
    assert_eq!(snapshot.running[0].worker.as_deref(), Some("beating"));

    within(10, run).await.unwrap().unwrap();
    assert_eq!(docket.workers().await.unwrap(), []);
}
