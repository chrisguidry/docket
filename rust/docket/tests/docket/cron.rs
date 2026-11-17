use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{Cron, Docket, Execution, State};
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};

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
    let next_year = Timestamp::now().to_zoned(TimeZone::UTC).year() + 1;
    assert_eq!(
        snapshot.future[0].when,
        jiff::civil::date(next_year, 1, 1)
            .to_zoned(TimeZone::UTC)
            .unwrap()
            .timestamp()
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
                .timezone(TimeZone::get("Asia/Kolkata").unwrap()),
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
    let utc = next.to_zoned(TimeZone::UTC);
    assert_eq!((utc.minute(), utc.second()), (30, 0));
    assert!(next > Timestamp::now());
}

/// The next slot counts from when a run started, as in pydocket, so a run
/// still going at its next slot is followed by that slot's run, not the one
/// after it.
#[tokio::test]
async fn a_run_that_overlaps_its_next_slot_is_followed_by_that_slot() {
    let docket = docket().await;
    let whens = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&whens);
    docket
        .register(move |ctx, _: Noop| {
            recorded.lock().unwrap().push(ctx.when());
            async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                Ok::<_, std::io::Error>(())
            }
        })
        .with(Cron::new("* * * * * *").unwrap());

    let two_runs = async {
        while whens.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    within(10, worker(&docket).run_until(two_runs))
        .await
        .unwrap();

    let whens = whens.lock().unwrap().clone();
    assert_eq!(
        whens[1].duration_since(whens[0]),
        SignedDuration::from_secs(1)
    );
}
