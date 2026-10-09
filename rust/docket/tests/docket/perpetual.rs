use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{Event, Perpetual, State, Task};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::support::telemetry::value;
use crate::support::{Noop, docket, within, worker};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "count")]
struct Count {
    n: u32,
}

#[tokio::test]
async fn a_perpetual_task_runs_again_after_each_run() {
    let docket = docket().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    docket
        .register(move |ctx: docket::Context, args: Count| {
            recorded.lock().unwrap().push(args.n);
            async move {
                ctx.perpetual()
                    .unwrap()
                    .perpetuate(&Count { n: args.n + 1 });
                Ok::<_, std::io::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    docket.add(Count { n: 1 }).key("counter").await.unwrap();

    let limits = HashMap::from([("counter".to_owned(), 3)]);
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
}

/// The limit refuses the task's own next run, as a strike would, so
/// `run_at_most` returns after the last allowed run instead of waiting for
/// the next one to come due.
#[tokio::test]
async fn run_at_most_returns_after_the_last_allowed_run_of_a_slow_perpetual_task() {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            *counted.lock().unwrap() += 1;
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    docket.add(Noop).key("hourly").await.unwrap();

    let limits = HashMap::from([("hourly".to_owned(), 1)]);
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), 1);
    assert_eq!(docket.snapshot().await.unwrap().future, []);
}

#[tokio::test]
async fn a_perpetual_task_can_stop_itself() {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |ctx: docket::Context, _: Noop| {
            *counted.lock().unwrap() += 1;
            async move {
                ctx.perpetual().unwrap().cancel();
                Ok::<_, std::io::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    let execution = docket.add(Noop).key("once").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), 1);
    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}

#[tokio::test]
async fn a_perpetual_task_can_move_its_next_run() {
    let docket = docket().await;
    docket
        .register(|ctx: docket::Context, _: Noop| async move {
            ctx.perpetual().unwrap().after(Duration::from_secs(3600));
            Ok::<_, std::io::Error>(())
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    let first = docket.add(Noop).key("hourly").await.unwrap();

    // The next run is an hour out, so the worker would wait for it.
    let ran = async {
        while first.status().await.unwrap().unwrap().state != State::Scheduled {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    within(10, worker(&docket).run_until(ran)).await.unwrap();

    let snapshot = docket.snapshot().await.unwrap();
    let next = &snapshot.future[0];
    assert_eq!(next.key, "hourly");
    assert!(next.when > chrono::Utc::now() + chrono::Duration::minutes(59));
}

#[tokio::test]
async fn an_automatic_task_starts_with_the_worker() {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&runs);
    docket
        .register(move |ctx: docket::Context, args: Count| {
            recorded
                .lock()
                .unwrap()
                .push((ctx.key().to_owned(), args.n));
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(Perpetual::every(Duration::from_millis(10)).automatic());

    let limits = HashMap::from([("count".to_owned(), 2)]);
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    assert_eq!(
        *runs.lock().unwrap(),
        [("count".to_owned(), 0), ("count".to_owned(), 0)]
    );
}

#[tokio::test]
async fn a_perpetual_task_runs_again_after_it_fails() {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            *counted.lock().unwrap() += 1;
            async { Err::<(), _>(std::io::Error::other("nope")) }
        })
        .with(Perpetual::every(Duration::from_millis(10)));
    docket.add(Noop).key("stubborn").await.unwrap();

    let limits = HashMap::from([("stubborn".to_owned(), 2)]);
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), 2);
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "stopping")]
struct Stopping {
    round: String,
}

impl Stopping {
    fn new(round: &str) -> Self {
        Self {
            round: round.to_owned(),
        }
    }
}

#[tokio::test]
async fn a_perpetual_task_that_stops_itself_keeps_a_replacement() {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&runs);
    docket
        .register(move |ctx: docket::Context, args: Stopping| {
            recorded.lock().unwrap().push(args.round.clone());
            async move {
                if args.round == "first" {
                    let soon = chrono::Utc::now() + chrono::Duration::milliseconds(200);
                    let replacement = Stopping::new("replacement");
                    ctx.docket().replace(replacement, ctx.key(), soon).await?;
                }
                ctx.perpetual().unwrap().cancel();
                Ok::<_, docket::Error>(())
            }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    docket
        .add(Stopping::new("first"))
        .key("stopping")
        .await
        .unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), ["first", "replacement"]);
    let labels = ["docket.task=stopping", "docket.where=on_complete"];
    assert_eq!(
        value(&docket, "docket_tasks_superseded", &labels),
        Some(1.0)
    );
}

/// A run that a replace superseded ends without a state event, so a caller
/// waiting on the key waits for the replacement, not the old run.
#[rstest::rstest]
#[case::stops_and_succeeds(true, false)]
#[case::stops_and_fails(true, true)]
#[case::reschedules_and_succeeds(false, false)]
#[case::reschedules_and_fails(false, true)]
#[tokio::test]
async fn a_perpetual_task_that_gives_way_publishes_no_ending(
    #[case] stops: bool,
    #[case] fails: bool,
) {
    let docket = docket().await;
    let runs = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&runs);
    docket
        .register(move |ctx: docket::Context, args: Stopping| {
            recorded.lock().unwrap().push(args.round.clone());
            async move {
                if args.round == "replacement" {
                    ctx.perpetual().unwrap().cancel();
                    return Ok(());
                }
                let soon = chrono::Utc::now() + chrono::Duration::milliseconds(200);
                let replacement = Stopping::new("replacement");
                ctx.docket().replace(replacement, ctx.key(), soon).await?;
                if stops {
                    ctx.perpetual().unwrap().cancel();
                }
                if fails {
                    return Err("the first run fails".into());
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
        })
        .with(Perpetual::every(Duration::from_secs(3600)));
    let execution = docket
        .add(Stopping::new("first"))
        .key("stopping")
        .await
        .unwrap();
    let mut events = execution.subscribe().await.unwrap();

    let first_ending = async {
        loop {
            if let Some(Ok(Event::State(event))) = events.next().await
                && event.state.is_terminal()
            {
                return runs.lock().unwrap().clone();
            }
        }
    };
    let (finished, seen) = within(10, async {
        tokio::join!(worker(&docket).run_until_finished(), first_ending)
    })
    .await;
    finished.unwrap();

    assert_eq!(seen, ["first", "replacement"]);
}
