use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chrono::{Datelike, TimeZone, Timelike, Utc};
use docket::{Cron, Docket, Execution, State};

use crate::support::{Noop, docket, within, worker};

/// Completes once the docket has a task scheduled under `key`.
async fn scheduled(docket: &Docket, key: &str) {
    loop {
        let snapshot = docket.snapshot().await.unwrap();
        if snapshot.future.iter().any(|task| task.key == key) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn an_automatic_cron_task_starts_at_its_next_match() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Cron::new("@yearly").unwrap());

    within(10, worker(&docket).run_until(scheduled(&docket, "noop")))
        .await
        .unwrap();

    let snapshot = docket.snapshot().await.unwrap();
    let next_year = Utc::now().year() + 1;
    assert_eq!(
        snapshot.future[0].when,
        Utc.with_ymd_and_hms(next_year, 1, 1, 0, 0, 0).unwrap()
    );
}

/// Completes once `execution` has run `runs` times and waits again.
async fn ran_and_rescheduled(execution: &Execution<()>, runs: &AtomicU32) {
    loop {
        let state = execution.status().await.unwrap().unwrap().state;
        if runs.load(Ordering::SeqCst) == 1 && state == State::Scheduled {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_manual_cron_task_runs_again_at_its_next_match() {
    let docket = docket().await;
    let runs = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&runs);
    docket
        .register(move |_ctx, _: Noop| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::io::Error>(()) }
        })
        .with(
            Cron::new("@hourly")
                .unwrap()
                .manual()
                .timezone(chrono_tz::Asia::Kolkata),
        );
    let execution = docket.add(Noop).key("hourly").await.unwrap();

    within(
        10,
        worker(&docket).run_until(ran_and_rescheduled(&execution, &runs)),
    )
    .await
    .unwrap();

    // Kolkata is 5:30 ahead of UTC, so its hours start at half past in UTC.
    let snapshot = docket.snapshot().await.unwrap();
    let next = snapshot.future[0].when;
    assert_eq!((next.minute(), next.second()), (30, 0));
    assert!(next > Utc::now());
}

#[tokio::test]
async fn a_cron_task_with_no_next_match_ends_after_its_run() {
    let docket = docket().await;
    docket
        .register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) })
        .with(Cron::new("0 0 30 2 *").unwrap().manual());
    let execution = docket.add(Noop).key("never-again").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Completed
    );
}
