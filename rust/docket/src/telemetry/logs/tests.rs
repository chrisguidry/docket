use rstest::rstest;
use serde_json::json;

use super::{call_repr, format_duration};
use crate::task::{Logged, TaskField};

#[rstest]
#[case::milliseconds(0.012_3, "    12ms")]
#[case::almost_100_seconds(99.9, " 99900ms")]
#[case::seconds(250.0, "   250s ")]
fn writes_durations_as_pydocket_does(#[case] seconds: f64, #[case] written: &str) {
    assert_eq!(format_duration(seconds), written);
}

const FIELDS: &[TaskField] = &[
    TaskField {
        name: "customer",
        logged: Logged::Value,
    },
    TaskField {
        name: "card",
        logged: Logged::Hidden,
    },
    TaskField {
        name: "items",
        logged: Logged::Length,
    },
];

#[test]
fn shows_only_the_logged_fields() {
    let args = json!({"customer": 7, "card": "4242", "items": [1, 2, 3]});
    assert_eq!(
        call_repr("charge", FIELDS, &args, "order-9"),
        "charge(customer=7, card=..., items=[len 3]){order-9}"
    );
}

#[rstest]
#[case::list(json!([1, 2]), "[len 2]")]
#[case::map(json!({"a": 1}), "{len 1}")]
#[case::text(json!("héllo"), "[len 5]")]
#[case::number(json!(12), "12")]
fn shows_the_length_of_a_collection(#[case] items: serde_json::Value, #[case] shown: &str) {
    let fields = [TaskField {
        name: "items",
        logged: Logged::Length,
    }];
    let call = call_repr("t", &fields, &json!({ "items": items }), "k");
    assert_eq!(call, format!("t(items={shown}){{k}}"));
}

#[rstest]
#[case::text(json!("hello"), "'hello'")]
#[case::single_quote(json!("it's"), "\"it's\"")]
#[case::both_quotes(json!("it's \"so\""), "'it\\'s \"so\"'")]
#[case::escapes(json!("a\\b\n\r\t\u{1}"), "'a\\\\b\\n\\r\\t\\x01'")]
#[case::truth(json!(true), "True")]
#[case::falsehood(json!(false), "False")]
#[case::nothing(json!(null), "None")]
#[case::number(json!(1.5), "1.5")]
#[case::list(json!([1, "a"]), "[1, 'a']")]
#[case::map(json!({"a": null}), "{'a': None}")]
fn shows_a_value_as_python_repr_does(#[case] value: serde_json::Value, #[case] shown: &str) {
    let fields = [TaskField {
        name: "value",
        logged: Logged::Value,
    }];
    let call = call_repr("t", &fields, &json!({ "value": value }), "k");
    assert_eq!(call, format!("t(value={shown}){{k}}"));
}

#[test]
fn hides_a_logged_field_the_arguments_lack() {
    let args = json!({"card": "4242"});
    assert_eq!(
        call_repr("charge", &FIELDS[..1], &args, "k"),
        "charge(customer=...){k}"
    );
}

#[test]
fn shows_no_fields_for_a_task_that_lists_none() {
    assert_eq!(call_repr("cleanup", &[], &json!(null), "k"), "cleanup(){k}");
}
