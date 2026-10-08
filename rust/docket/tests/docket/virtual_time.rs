//! A `memory://` docket's clock, which a test moves forward so that
//! hour-long intervals and retry delays take no real time.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Datelike, TimeZone, Utc};
use docket::testing::{advance_time, assert_task_count, skip_idle_time};
use docket::{Context, Cron, Docket, Perpetual, Retry};

use crate::support::{Noop, within, worker};

const HOUR: Duration = Duration::from_hours(1);

/// When each run of a task was due.
type Due = Arc<Mutex<Vec<DateTime<Utc>>>>;

async fn memory(url: &str) -> Docket {
    Docket::connect("virtual-time", url).await.unwrap()
}

fn unique_url() -> String {
    format!("memory://{}", uuid::Uuid::now_v7())
}

/// Registers a noop that records when each run was due, and fails each
/// run when `fail` is set.
fn record(docket: &Docket, fail: bool) -> (Due, docket::Registration<Noop>) {
    let due = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&due);
    let registration = docket.register(move |ctx: Context, _: Noop| {
        recorded.lock().unwrap().push(ctx.when());
        async move {
            if fail {
                return Err(std::io::Error::other("again"));
            }
            Ok(())
        }
    });
    (due, registration)
}

/// Each gap is at least `every`, and short of it by less than the moment a
/// worker takes to pick a due task up.
fn assert_apart(due: &[DateTime<Utc>], every: Duration) {
    for pair in due.windows(2) {
        let gap = (pair[1] - pair[0]).to_std().unwrap();
        assert!(
            gap >= every && gap < every + Duration::from_secs(10),
            "{due:?}"
        );
    }
}

#[tokio::test]
async fn skipped_time_runs_an_hourly_perpetual_task_at_once() {
    let docket = memory(&unique_url()).await;
    skip_idle_time(&docket);
    let (due, registration) = record(&docket, false);
    registration.with(Perpetual::every(HOUR));
    docket.add(Noop).key("hourly").await.unwrap();

    let limits = [("hourly".to_owned(), 3)].into();
    within(10, worker(&docket).run_at_most(limits))
        .await
        .unwrap();

    let due = due.lock().unwrap();
    assert_eq!(due.len(), 3);
    assert_apart(&due, HOUR);
}

#[tokio::test]
async fn skipped_time_runs_a_whole_retry_series_at_once() {
    let docket = memory(&unique_url()).await;
    skip_idle_time(&docket);
    let (due, registration) = record(&docket, true);
    registration.with(Retry::attempts(4).delay(HOUR));
    docket.add(Noop).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    let due = due.lock().unwrap();
    assert_eq!(due.len(), 4);
    assert_apart(&due, HOUR);
}

#[tokio::test]
async fn advancing_the_clock_of_one_docket_brings_a_task_due_on_every_docket_on_the_url() {
    let url = unique_url();
    let producer = memory(&url).await;
    let consumer = memory(&url).await;
    let (due, _) = record(&consumer, false);
    producer.add(Noop).after(HOUR).await.unwrap();

    advance_time(&producer, HOUR);
    within(10, worker(&consumer).run_until_finished())
        .await
        .unwrap();

    assert_eq!(due.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_memory_docket_keeps_real_time_unless_a_test_moves_it() {
    let docket = memory(&unique_url()).await;
    let (due, _) = record(&docket, false);
    docket.add(Noop).after(HOUR).await.unwrap();

    let run = tokio::time::timeout(
        Duration::from_millis(300),
        worker(&docket).run_until_finished(),
    );

    assert!(run.await.is_err());
    assert!(due.lock().unwrap().is_empty());
    assert_task_count(&docket, "noop", 1).await;
}

#[tokio::test]
async fn an_automatic_cron_task_starts_at_its_next_match_on_the_dockets_clock() {
    let docket = memory(&unique_url()).await;
    advance_time(&docket, Duration::from_hours(400 * 24));
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Cron::new("@yearly").unwrap());

    let seeded = async {
        while docket.snapshot().await.unwrap().future.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    within(10, worker(&docket).run_until(seeded)).await.unwrap();

    let snapshot = docket.snapshot().await.unwrap();
    let next_year = snapshot.taken.year() + 1;
    assert!(snapshot.taken.year() > Utc::now().year());
    assert_eq!(
        snapshot.future[0].when,
        Utc.with_ymd_and_hms(next_year, 1, 1, 0, 0, 0).unwrap()
    );
}

#[tokio::test]
#[should_panic(expected = "only a memory:// docket's clock can move")]
async fn only_a_memory_dockets_clock_can_move() {
    let docket = memory("redis://127.0.0.1:1/0").await;
    skip_idle_time(&docket);
}
