use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use rstest::rstest;

use super::Message;

fn message() -> Message {
    Message {
        key: "order-9".into(),
        when: Utc.timestamp_micros(1_791_228_363_529_728).unwrap(),
        function: "charge".into(),
        args: r#"{"customer":7}"#.into(),
        attempt: 2,
        generation: 5,
        trace: HashMap::new(),
    }
}

fn as_map(fields: Vec<(String, Vec<u8>)>) -> HashMap<String, Vec<u8>> {
    fields.into_iter().collect()
}

#[test]
fn writes_the_fields_the_scripts_copy() {
    let names: Vec<String> = message()
        .fields()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        names,
        [
            "key",
            "when",
            "function",
            "args",
            "kwargs",
            "attempt",
            "generation"
        ]
    );
}

#[test]
fn round_trips_through_fields() {
    assert_eq!(
        Message::from_fields(&as_map(message().fields())).unwrap(),
        message()
    );
}

#[test]
fn a_missing_generation_is_zero() {
    let mut fields = as_map(message().fields());
    fields.remove("generation");
    assert_eq!(Message::from_fields(&fields).unwrap().generation, 0);
}

#[rstest]
#[case::no_key("key", None, "a task message has no key field")]
#[case::no_when("when", None, "a task message has no when field")]
#[case::no_function("function", None, "a task message has no function field")]
#[case::no_args("args", None, "a task message has no args field")]
#[case::no_attempt("attempt", None, "a task message has no attempt field")]
#[case::bad_when("when", Some("soon"), "a task message's when is soon")]
#[case::bad_attempt("attempt", Some("x"), "a task message's attempt is not a number")]
#[case::negative_attempt("attempt", Some("-1"), "a task message's attempt is out of range")]
#[case::bad_generation("generation", Some("x"), "a task message's generation is not a number")]
fn rejects_bad_fields(#[case] field: &str, #[case] value: Option<&str>, #[case] error: &str) {
    let mut fields = as_map(message().fields());
    match value {
        Some(value) => fields.insert(field.to_owned(), value.as_bytes().to_vec()),
        None => fields.remove(field),
    };
    assert_eq!(
        Message::from_fields(&fields).unwrap_err().to_string(),
        error
    );
}

#[test]
fn reads_a_stream_entry() {
    let entry = redis::streams::StreamId {
        id: "1-0".into(),
        map: message()
            .fields()
            .into_iter()
            .map(|(field, value)| (field, redis::Value::BulkString(value)))
            .collect(),
        ..Default::default()
    };
    assert_eq!(Message::from_entry(&entry).unwrap(), message());
}
