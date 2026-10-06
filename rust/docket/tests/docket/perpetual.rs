use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{Perpetual, State, Task};
use serde::{Deserialize, Serialize};

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
