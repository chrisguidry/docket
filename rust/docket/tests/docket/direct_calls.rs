//! Calling a task's handler straight from a test, with a context that
//! `ContextBuilder` makes, as pydocket's tests call a task function with
//! the dependencies they pass it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use docket::testing::ContextBuilder;
use docket::{Context, Task};
use serde::{Deserialize, Serialize};

use crate::support::docket;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "sync")]
struct Sync {
    cursor: u32,
}

/// Stops at cursor 0, moves the next run an hour out at cursor 1, and
/// otherwise runs again with the next cursor.
async fn sync(ctx: Context, args: Sync) -> Result<(), std::io::Error> {
    let next = ctx.perpetual().expect("sync is perpetual");
    match args.cursor {
        0 => next.cancel(),
        1 => next.after(Duration::from_secs(3600)),
        cursor => next.perpetuate(&Sync { cursor: cursor + 1 }),
    }
    Ok(())
}

#[tokio::test]
async fn a_handler_can_cancel_the_perpetual_it_is_given() {
    let docket = docket().await;
    let args = Sync { cursor: 0 };
    let ctx = ContextBuilder::new(&docket, &args).perpetual().build();

    sync(ctx.clone(), args).await.unwrap();

    assert!(ctx.perpetual().unwrap().is_cancelled());
}

#[tokio::test]
async fn a_test_reads_back_the_next_run_a_handler_chose() {
    let docket = docket().await;
    let args = Sync { cursor: 1 };
    let before = Utc::now();
    let ctx = ContextBuilder::new(&docket, &args).perpetual().build();

    sync(ctx.clone(), args).await.unwrap();

    let next = ctx.perpetual().unwrap();
    assert!(!next.is_cancelled());
    let when = next.next_when().unwrap();
    assert!(when >= before + Duration::from_secs(3600), "{when}");
    assert_eq!(next.next_args::<Sync>(), None);
}

#[tokio::test]
async fn a_test_reads_back_the_arguments_a_handler_perpetuated() {
    let docket = docket().await;
    let args = Sync { cursor: 5 };
    let ctx = ContextBuilder::new(&docket, &args).perpetual().build();

    sync(ctx.clone(), args).await.unwrap();

    let next = ctx.perpetual().unwrap();
    assert_eq!(next.next_args::<Sync>(), Some(Sync { cursor: 6 }));
    assert_eq!(next.next_when(), None);
}

#[tokio::test]
async fn a_context_carries_what_the_builder_sets() {
    let docket = docket().await;
    let when: DateTime<Utc> = "2026-01-02T03:04:05Z".parse().unwrap();
    let ctx = ContextBuilder::new(&docket, &Sync { cursor: 3 })
        .key("sync-1")
        .attempt(2)
        .when(when)
        .worker("worker-a")
        .behavior(7_u8)
        .build();

    assert_eq!(ctx.docket().name(), docket.name());
    assert_eq!(ctx.function(), "sync");
    assert_eq!(ctx.args(), &serde_json::json!({ "cursor": 3 }));
    assert_eq!(ctx.key(), "sync-1");
    assert_eq!(ctx.attempt(), 2);
    assert_eq!(ctx.when(), when);
    assert_eq!(ctx.worker(), "worker-a");
    assert_eq!(ctx.behavior::<u8>(), Some(&7));
    assert!(ctx.perpetual().is_none());
}

#[tokio::test]
async fn a_context_has_a_first_attempt_due_now_under_a_key_of_its_own() {
    let docket = docket().await;
    let before = Utc::now();
    let first = ContextBuilder::new(&docket, &Sync { cursor: 0 }).build();
    let second = ContextBuilder::new(&docket, &Sync { cursor: 0 }).build();

    assert_eq!(first.attempt(), 1);
    assert!(first.when() >= before);
    assert_ne!(first.key(), second.key());
}

#[tokio::test]
async fn a_handler_reports_progress_through_a_built_context() {
    let docket = docket().await;
    let args = Sync { cursor: 0 };
    let ctx = ContextBuilder::new(&docket, &args).key("progress").build();

    ctx.progress().set_total(4).await.unwrap();
    ctx.progress().increment(1).await.unwrap();
}
