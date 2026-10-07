use chrono::{DateTime, TimeZone, Utc};
use rstest::rstest;

use super::Cron;

#[rstest]
#[case::weekday_morning("0 9 * * 1-5", "2026-10-05T09:00:00Z")]
#[case::midnight("@midnight", "2026-10-06T00:00:00Z")]
#[case::hourly("@hourly", "2026-10-05T08:00:00Z")]
fn finds_the_next_match(#[case] expression: &str, #[case] expected: &str) {
    let now = Utc.with_ymd_and_hms(2026, 10, 5, 7, 30, 0).unwrap();
    assert_eq!(
        Cron::new(expression)
            .unwrap()
            .next_after(now)
            .unwrap()
            .to_rfc3339(),
        expected.replace('Z', "+00:00")
    );
}

#[test]
fn reads_the_schedule_in_its_time_zone() {
    let now = Utc.with_ymd_and_hms(2026, 10, 5, 7, 30, 0).unwrap();
    let cron = Cron::new("0 9 * * *")
        .unwrap()
        .timezone(chrono_tz::America::Los_Angeles);
    // 9:00 in Los Angeles is 16:00 UTC during daylight saving time.
    assert_eq!(
        cron.next_after(now).unwrap().to_rfc3339(),
        "2026-10-05T16:00:00+00:00"
    );
}

#[test]
fn rejects_an_expression_that_is_not_cron() {
    let error = Cron::new("every tuesday").unwrap_err().to_string();
    assert!(
        error.starts_with("every tuesday is not a cron expression"),
        "{error}"
    );
}

#[test]
fn a_manual_cron_keeps_its_schedule() {
    let now = Utc.with_ymd_and_hms(2026, 10, 5, 7, 30, 0).unwrap();
    let cron = Cron::new("@hourly").unwrap().manual();
    assert_eq!(
        cron.next_after(now).unwrap().to_rfc3339(),
        "2026-10-05T08:00:00+00:00"
    );
}

/// The expressions pydocket's `Cron` accepts and refuses, each with the first
/// match after one instant.  pydocket's tests read the same table.
#[test]
fn reads_every_expression_the_way_pydocket_does() {
    let table: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/cron-expressions.json"
    ))
    .unwrap();
    let after = DateTime::parse_from_rfc3339(table["after"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    let mut differences = Vec::new();
    for row in table["expressions"].as_array().unwrap() {
        let expression = row["expression"].as_str().unwrap();
        // An expression pydocket refuses has no next match: Cron::new fails.
        let expected = row["next"].as_str().map(|next| {
            Some(
                DateTime::parse_from_rfc3339(next)
                    .unwrap()
                    .with_timezone(&Utc),
            )
        });
        let next = Cron::new(expression)
            .ok()
            .map(|cron| cron.next_after(after));
        if next != expected {
            differences.push(format!("{expression}: expected {expected:?}, got {next:?}"));
        }
    }
    assert!(differences.is_empty(), "{differences:#?}");
}
