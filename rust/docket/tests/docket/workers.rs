use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use docket::behaviors::{AfterCompletion, Behavior, Completion, Hooks, Outcome};
use docket::{Context, Docket, State, Task, Worker};
use redis::AsyncCommands;
use tokio::sync::Notify;

use crate::logs::Logs;
use crate::snapshots::raw;
use crate::support::{Echo, Noop, docket, url, within, worker};

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

#[tokio::test]
async fn a_worker_told_to_stop_before_it_starts_stops() {
    let docket = docket().await;
    let execution = docket.add(Noop).await.unwrap();
    within(10, worker(&docket).run_until(async {}))
        .await
        .unwrap();
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Queued
    );
}

#[tokio::test]
async fn every_way_to_run_reports_an_unusable_setting() {
    let docket = docket().await;
    let until = Worker::new(docket.clone())
        .concurrency(0)
        .run_until(std::future::pending())
        .await;
    let forever = Worker::new(docket).concurrency(0).run_forever().await;
    assert!(matches!(until, Err(docket::Error::Invalid(_))));
    assert!(matches!(forever, Err(docket::Error::Invalid(_))));
}

#[tokio::test]
async fn a_worker_can_leave_automatic_tasks_to_others() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(docket::Perpetual::every(Duration::from_millis(10)).automatic());
    within(
        10,
        worker(&docket)
            .schedule_automatic_tasks(false)
            .run_until_finished(),
    )
    .await
    .unwrap();
    assert_eq!(docket.snapshot().await.unwrap().total_tasks, 0);
}

#[tokio::test]
async fn shutdown_reaches_a_worker_whose_every_slot_is_busy() {
    let docket = docket().await;
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    docket.register(move |_ctx, _: Noop| {
        signal.notify_one();
        async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, std::io::Error>(())
        }
    });
    let execution = docket.add(Noop).await.unwrap();
    let waiting = docket.add(Noop).await.unwrap();

    let shutdown = Arc::clone(&started);
    within(
        10,
        worker(&docket)
            .concurrency(1)
            .run_until(async move { shutdown.notified().await }),
    )
    .await
    .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
    assert_eq!(
        waiting.status().await.unwrap().unwrap().state,
        State::Queued
    );
}

#[tokio::test]
async fn a_worker_that_stops_beating_drops_off_the_list() {
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url())
        .heartbeat_interval(Duration::from_millis(20))
        .missed_heartbeats(2)
        .connect()
        .await
        .unwrap();
    let run = tokio::spawn(worker(&docket).run_forever());
    within(10, async {
        while docket.workers().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    run.abort();

    within(10, async {
        while !docket.workers().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// Panics when the task is done, outside the handler.
struct PanicAfter;

impl Completion for PanicAfter {
    async fn on_complete(&self, _: &Context, _: &Outcome) -> AfterCompletion {
        panic!("the completion hook gave up")
    }
}

impl<T: Task> Behavior<T> for PanicAfter {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.completion(self);
    }
}

#[tokio::test]
async fn a_worker_stops_when_a_task_runner_panics() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(PanicAfter);
    docket.add(Noop).await.unwrap();

    let error = within(10, worker(&docket).run_until_finished())
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains("a task's runner stopped"),
        "{error}"
    );
}

/// Lets each run end normally.
struct Finish;

impl Completion for Finish {
    async fn on_complete(&self, _: &Context, _: &Outcome) -> AfterCompletion {
        AfterCompletion::Finish
    }
}

impl<T: Task> Behavior<T> for Finish {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.completion(self);
    }
}

#[tokio::test]
async fn a_completion_hook_can_let_a_task_end_normally() {
    let docket = docket().await;
    docket
        .register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) })
        .with(Finish);
    let execution = docket.add(Echo::new("done")).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(execution.result().await.unwrap(), "done");
}

#[tokio::test]
async fn a_worker_drops_a_stream_entry_that_is_not_a_task() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    let _: String = raw
        .xadd(format!("{}:stream", docket.name()), "*", &[("junk", "1")])
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains("dropping a stream entry that is not a task"));
    assert_eq!(docket.snapshot().await.unwrap().total_tasks, 0);
}

#[tokio::test]
async fn a_worker_creates_its_group_again_after_someone_deletes_it() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let first = docket.add(Echo::new("first")).await.unwrap();
    let run = tokio::spawn(worker(&docket).run_forever());
    within(10, first.result()).await.unwrap();

    let _: i64 = redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg(format!("{}:stream", docket.name()))
        .arg("docket-workers")
        .query_async(&mut raw)
        .await
        .unwrap();
    let second = docket.add(Echo::new("second")).await.unwrap();

    assert_eq!(within(10, second.result()).await.unwrap(), "second");
    run.abort();
}

#[tokio::test]
async fn a_delivery_replaced_while_its_worker_was_dead_does_not_run() {
    let docket = docket().await;
    let doomed = Docket::connect(docket.name(), crate::support::shared_url(&docket))
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    doomed.register(move |_ctx, _: Echo| {
        signal.notify_one();
        std::future::pending::<Result<String, std::io::Error>>()
    });
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket.register(move |_ctx, args: Echo| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { Ok::<_, std::io::Error>(args.text) }
    });
    docket.add(Echo::new("old")).key("k").await.unwrap();
    let run = tokio::spawn(
        worker(&doomed)
            .name("doomed")
            .redelivery_timeout(Duration::from_millis(200))
            .run_forever(),
    );
    within(10, started.notified()).await;
    run.abort();

    let replaced = docket
        .replace(Echo::new("new"), "k", chrono::Utc::now())
        .await
        .unwrap();
    within(
        20,
        worker(&docket)
            .redelivery_timeout(Duration::from_millis(200))
            .run_until_finished(),
    )
    .await
    .unwrap();

    assert_eq!(replaced.result().await.unwrap(), "new");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
