use std::time::Duration;

use chrono::Utc;
use docket::{Disposition, Event, State};
use futures::StreamExt;

use crate::support::{Echo, Noop, docket, within, worker};

#[tokio::test]
async fn add_many_schedules_every_call_in_one_round_trip() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket
        .strike(docket::Strike::task::<Echo>().field("text").eq("struck"))
        .await
        .unwrap();
    let executions = docket
        .add_many([
            docket.call(Echo::new("a")),
            docket.call(Echo::new("b")).key("b"),
            docket
                .call(Echo::new("c"))
                .at(Utc::now() + chrono::Duration::milliseconds(100)),
            docket.call(Echo::new("struck")),
        ])
        .await
        .unwrap();
    let dispositions: Vec<&Disposition> = executions
        .iter()
        .map(docket::Execution::disposition)
        .collect();
    assert_eq!(
        dispositions,
        [
            &Disposition::Scheduled,
            &Disposition::Scheduled,
            &Disposition::Scheduled,
            &Disposition::Struck
        ]
    );

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    for (execution, text) in executions.iter().zip(["a", "b", "c"]) {
        assert_eq!(execution.result().await.unwrap(), serde_json::json!(text));
    }
}

#[tokio::test]
async fn add_many_reports_keys_that_already_exist() {
    let docket = docket().await;
    docket
        .add(Echo::new("first"))
        .key("taken")
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let executions = docket
        .add_many([docket.call(Echo::new("second")).key("taken")])
        .await
        .unwrap();
    assert_eq!(executions[0].disposition(), &Disposition::AlreadyScheduled);
}

#[tokio::test]
async fn an_empty_batch_does_nothing() {
    let docket = docket().await;
    assert!(docket.add_many([]).await.unwrap().is_empty());
}

#[tokio::test]
async fn replace_many_swaps_each_task() {
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    docket
        .add(Echo::new("old"))
        .key("k")
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let executions = docket
        .replace_many([docket.call(Echo::new("new")).key("k")])
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        executions[0].result().await.unwrap(),
        serde_json::json!("new")
    );
}

#[tokio::test]
async fn replace_many_needs_every_key() {
    let docket = docket().await;
    let error = docket
        .replace_many([docket.call(Echo::new("x"))])
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "replacing a echo task needs its key");
}

#[tokio::test]
async fn a_docket_that_keeps_nothing_forgets_finished_tasks() {
    let url = crate::support::url();
    let docket = docket::Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url)
        .execution_ttl(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("gone")).await.unwrap();

    within(10, crate::support::worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(execution.status().await.unwrap(), None);
}

#[tokio::test]
async fn a_task_reports_progress_to_its_followers() {
    let docket = docket().await;
    docket.register(|ctx: docket::Context, _: Noop| async move {
        let progress = ctx.progress();
        progress.set_total(3).await?;
        progress.increment(2).await?;
        progress.set_message(Some("almost")).await?;
        progress.set_message(None).await?;
        Ok::<_, docket::Error>(())
    });
    let execution = docket
        .add(Noop)
        .after(Duration::from_millis(200))
        .await
        .unwrap();
    let mut events = execution.subscribe().await.unwrap();

    let run = tokio::spawn(worker(&docket).run_until_finished());
    let mut seen = Vec::new();
    within(10, async {
        while let Some(event) = events.next().await {
            let event = event.unwrap();
            let done = matches!(&event, Event::State(state) if state.state.is_terminal());
            seen.push(event);
            if done {
                break;
            }
        }
    })
    .await;
    within(10, run).await.unwrap().unwrap();

    let states: Vec<State> = seen
        .iter()
        .filter_map(|event| match event {
            Event::State(state) => Some(state.state),
            _ => None,
        })
        .collect();
    assert_eq!(states.first(), Some(&State::Scheduled));
    assert!(states.contains(&State::Running));
    assert_eq!(states.last(), Some(&State::Completed));
    let progress: Vec<(Option<i64>, i64, Option<String>)> = seen
        .iter()
        .filter_map(|event| match event {
            Event::Progress(progress) => {
                Some((progress.current, progress.total, progress.message.clone()))
            }
            _ => None,
        })
        .collect();
    assert!(progress.contains(&(Some(2), 3, None)), "{progress:?}");
    assert!(
        progress.contains(&(Some(2), 3, Some("almost".to_owned()))),
        "{progress:?}"
    );
}

#[tokio::test]
async fn progress_reads_as_unstarted_before_a_task_runs() {
    let docket = docket().await;
    let execution = docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    let progress = execution.progress().await.unwrap();
    assert_eq!(
        (progress.current, progress.total, progress.message),
        (None, 100, None)
    );
}

#[tokio::test]
async fn clear_removes_tasks_that_have_not_started() {
    let docket = docket().await;
    docket.add(Noop).await.unwrap();
    docket
        .add(Noop)
        .after(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(docket.snapshot().await.unwrap().total_tasks, 2);
    assert_eq!(docket.clear().await.unwrap(), 2);
    let snapshot = docket.snapshot().await.unwrap();
    assert_eq!((snapshot.total_tasks, snapshot.future.len()), (0, 0));
}
