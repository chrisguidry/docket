use std::collections::HashMap;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{Agenda, times};
use crate::{Disposition, Docket, Task};

#[derive(Serialize, Deserialize, docket_rs_macros::Task)]
#[task(name = "item", crate = crate)]
struct Item {
    n: u32,
}

/// Its map's keys are tuples, which JSON cannot hold.
#[derive(Serialize, Deserialize, docket_rs_macros::Task)]
#[task(name = "unencodable", crate = crate)]
struct Unencodable {
    pairs: HashMap<(u8, u8), u8>,
}

fn start() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 5, 12, 0, 0).unwrap()
}

#[test]
fn spreads_tasks_from_the_start_to_the_end() {
    let spread = times(3, start(), Duration::from_secs(60), Duration::ZERO);
    let offsets: Vec<i64> = spread
        .iter()
        .map(|when| (*when - start()).num_seconds())
        .collect();
    assert_eq!(offsets, [0, 30, 60]);
}

#[test]
fn puts_a_single_task_halfway() {
    let spread = times(1, start(), Duration::from_secs(60), Duration::ZERO);
    assert_eq!(spread, [start() + chrono::Duration::seconds(30)]);
}

#[test]
fn jitter_never_moves_a_task_before_the_start() {
    let spread = times(50, start(), Duration::from_secs(1), Duration::from_secs(10));
    assert!(spread.iter().all(|when| *when >= start()));
}

async fn docket() -> Docket {
    Docket::connect("agenda", format!("memory://{}", uuid::Uuid::now_v7()))
        .await
        .unwrap()
}

#[tokio::test]
async fn scatter_schedules_every_task() {
    let docket = docket().await;
    let mut agenda = Agenda::new();
    agenda.add(&Item { n: 1 }).add(&Item { n: 2 });
    assert_eq!((agenda.len(), agenda.is_empty()), (2, false));

    let executions = agenda
        .scatter(&docket, Duration::from_secs(60))
        .await
        .unwrap();

    assert!(
        executions
            .iter()
            .all(|e| e.disposition() == &Disposition::Scheduled)
    );
    assert_eq!(docket.snapshot().await.unwrap().future.len(), 2);
}

#[tokio::test]
async fn scattering_keyed_tasks_again_keeps_the_first_schedule() {
    let docket = docket().await;
    let mut agenda = Agenda::new();
    agenda.add_keyed(&Item { n: 1 }, "one");
    agenda
        .scatter(&docket, Duration::from_secs(60))
        .await
        .unwrap();

    let again = agenda
        .scatter(&docket, Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(again[0].disposition(), &Disposition::AlreadyScheduled);
}

#[tokio::test]
async fn scatter_needs_a_period() {
    let docket = docket().await;
    let error = Agenda::new()
        .scatter(&docket, Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "an agenda scatters over a period longer than zero"
    );
}

#[tokio::test]
async fn scatter_reports_arguments_that_are_not_json() {
    let docket = docket().await;
    let mut agenda = Agenda::new();
    agenda.add(&Unencodable {
        pairs: HashMap::from([((1, 2), 3)]),
    });
    let error = agenda
        .scatter(&docket, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "key must be a string");
}

#[test]
fn clear_empties_the_agenda() {
    let mut agenda = Agenda::new();
    agenda.add(&Item { n: 1 });
    agenda.clear();
    assert!(agenda.is_empty());
    assert_eq!(Item::NAME, "item");
}
