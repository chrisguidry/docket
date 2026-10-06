use std::collections::HashMap;

use redis::Value;
use rstest::rstest;

use super::{Disposition, State, Status};

#[rstest]
#[case::simple_exists(Value::SimpleString("EXISTS".into()), Disposition::AlreadyScheduled)]
#[case::bulk_superseded(Value::BulkString(b"SUPERSEDED".to_vec()), Disposition::Superseded)]
#[case::bulk_ok(Value::BulkString(b"OK".to_vec()), Disposition::Scheduled)]
#[case::other_shape(Value::Int(1), Disposition::Scheduled)]
fn a_schedule_reply_names_its_disposition(#[case] reply: Value, #[case] expected: Disposition) {
    assert_eq!(Disposition::from_value(&reply), expected);
}

#[rstest]
#[case(Disposition::Scheduled, "scheduled")]
#[case(Disposition::AlreadyScheduled, "already_scheduled")]
#[case(Disposition::Struck, "struck")]
#[case(Disposition::Superseded, "superseded")]
#[case(Disposition::Failed("refused".into()), "failed")]
fn a_disposition_has_pydockets_name(#[case] disposition: Disposition, #[case] name: &str) {
    assert_eq!(disposition.as_str(), name);
}

#[rstest]
#[case(State::Scheduled)]
#[case(State::Queued)]
#[case(State::Running)]
#[case(State::Completed)]
#[case(State::Failed)]
#[case(State::Cancelled)]
fn a_state_reads_back_from_its_name(#[case] state: State) {
    assert_eq!(State::parse(state.as_str()), Some(state));
}

fn hash(fields: &[(&str, &str)]) -> HashMap<String, String> {
    fields
        .iter()
        .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
        .collect()
}

#[rstest]
#[case::no_state(&[("function", "echo")])]
#[case::unknown_state(&[("state", "napping")])]
fn a_run_hash_without_a_known_state_has_no_status(#[case] fields: &[(&str, &str)]) {
    assert_eq!(Status::from_hash(&hash(fields)), None);
}

#[test]
fn a_run_hash_gives_every_part_of_the_status() {
    let status = Status::from_hash(&hash(&[
        ("state", "failed"),
        ("when", "1700000000.5"),
        ("worker", "w1"),
        ("started_at", "2023-11-14T22:13:21+00:00"),
        ("completed_at", "2023-11-14T22:13:22+00:00"),
        ("error", "boom"),
    ]))
    .unwrap();
    assert_eq!(status.state, State::Failed);
    assert_eq!(status.when.unwrap().timestamp_millis(), 1_700_000_000_500);
    assert_eq!(status.worker.as_deref(), Some("w1"));
    assert!(status.started_at.unwrap() < status.completed_at.unwrap());
    assert_eq!(status.error.as_deref(), Some("boom"));
}

#[rstest]
#[case::state_not_text(r#"{"state": 5}"#)]
#[case::unknown_state(r#"{"state": "napping"}"#)]
#[case::time_not_text(r#"{"state": "queued", "when": 5}"#)]
fn a_state_event_it_cannot_read_is_an_error(#[case] payload: &str) {
    assert!(serde_json::from_str::<super::StateEvent>(payload).is_err());
}

#[test]
fn a_state_event_reads_every_field() {
    let event: super::StateEvent = serde_json::from_str(
        r#"{"state": "running", "when": "2023-11-14T22:13:20+00:00", "worker": "w1",
            "started_at": "2023-11-14T22:13:21+00:00", "error": null}"#,
    )
    .unwrap();
    assert_eq!(event.state, State::Running);
    assert_eq!(event.when.unwrap().timestamp(), 1_700_000_000);
    assert_eq!(event.worker.as_deref(), Some("w1"));
    assert_eq!(event.started_at.unwrap().timestamp(), 1_700_000_001);
}
