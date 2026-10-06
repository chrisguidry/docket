use rstest::rstest;
use serde_json::{Value, json};

use super::{Operator, Strike, Strikes};

fn struck(strikes: &[Strike], function: &str, args: &Value) -> bool {
    let state = Strikes::default();
    for strike in strikes {
        state.apply(strike, false);
    }
    state.is_struck(function, args)
}

#[rstest]
#[case::whole_task(Strike::task_named("charge"), "charge", json!({"customer": 7}), true)]
#[case::other_task(Strike::task_named("refund"), "charge", json!({}), false)]
#[case::equal(Strike::task_named("charge").field("customer").eq(7), "charge", json!({"customer": 7}), true)]
#[case::not_equal(Strike::task_named("charge").field("customer").ne(7), "charge", json!({"customer": 8}), true)]
#[case::greater(Strike::any_task().field("cents").gt(100), "charge", json!({"cents": 101}), true)]
#[case::not_greater(Strike::any_task().field("cents").gt(100), "charge", json!({"cents": 100}), false)]
#[case::at_least(Strike::any_task().field("cents").ge(100), "charge", json!({"cents": 100}), true)]
#[case::less(Strike::any_task().field("cents").lt(100), "charge", json!({"cents": 99.5}), true)]
#[case::at_most(Strike::any_task().field("cents").le(100), "charge", json!({"cents": 100}), true)]
#[case::between(Strike::any_task().field("cents").between(10, 20), "charge", json!({"cents": 15}), true)]
#[case::outside(Strike::any_task().field("cents").between(10, 20), "charge", json!({"cents": 21}), false)]
#[case::strings(Strike::any_task().field("region").ge("m"), "charge", json!({"region": "us"}), true)]
#[case::booleans(Strike::any_task().field("test").gt(false), "charge", json!({"test": true}), true)]
#[case::mixed_kinds(Strike::any_task().field("cents").gt(1), "charge", json!({"cents": "2"}), false)]
#[case::missing_field(Strike::any_task().field("cents").eq(1), "charge", json!({"other": 1}), false)]
#[case::not_an_object(Strike::any_task().field("cents").eq(1), "charge", json!([1]), false)]
#[case::other_tasks_condition(Strike::task_named("refund").field("customer").eq(7), "charge", json!({"customer": 7}), false)]
fn matches_calls(
    #[case] strike: Strike,
    #[case] function: &str,
    #[case] args: Value,
    #[case] expected: bool,
) {
    assert_eq!(struck(&[strike], function, &args), expected);
}

#[test]
fn a_between_without_two_bounds_matches_nothing() {
    let strike = Strike::any_task()
        .field("cents")
        .when(Operator::Between, vec![1]);
    assert!(!struck(&[strike], "charge", &json!({"cents": 1})));
}

#[test]
fn a_field_strike_leaves_the_whole_task_strike_in_force() {
    let strikes = [
        Strike::task_named("charge"),
        Strike::task_named("charge").field("customer").eq(7),
    ];
    assert!(struck(&strikes, "charge", &json!({"customer": 8})));
}

#[test]
fn restoring_removes_only_the_matching_strike() {
    let state = Strikes::default();
    state.apply(&Strike::task_named("charge"), false);
    state.apply(&Strike::any_task().field("customer").eq(7), false);
    state.apply(&Strike::any_task().field("customer").eq(8), false);
    state.apply(&Strike::task_named("charge"), true);
    state.apply(&Strike::any_task().field("customer").eq(7), true);
    assert!(!state.is_struck("charge", &json!({"customer": 7})));
    assert!(state.is_struck("charge", &json!({"customer": 8})));
    state.apply(&Strike::any_task().field("customer").eq(8), true);
    assert!(!state.is_struck("charge", &json!({"customer": 8})));
}

#[test]
fn a_strike_with_neither_task_nor_field_does_nothing() {
    let state = Strikes::default();
    state.apply(&Strike::any_task(), false);
    assert!(!state.is_struck("charge", &json!({})));
}

#[rstest]
#[case(Operator::Equal, "==")]
#[case(Operator::NotEqual, "!=")]
#[case(Operator::GreaterThan, ">")]
#[case(Operator::GreaterOrEqual, ">=")]
#[case(Operator::LessThan, "<")]
#[case(Operator::LessOrEqual, "<=")]
#[case(Operator::Between, "between")]
fn operators_round_trip_through_text(#[case] operator: Operator, #[case] text: &str) {
    assert_eq!(operator.as_str(), text);
    assert_eq!(Operator::parse(text), Some(operator));
}

#[test]
fn unknown_operators_do_not_parse() {
    assert_eq!(Operator::parse("~="), None);
}

#[test]
fn instructions_round_trip_through_fields() {
    let strikes = [
        (Strike::task_named("charge"), false),
        (Strike::task_named("charge").field("customer").eq(7), true),
        (Strike::any_task().field("cents").between(1, 2), false),
    ];
    for (strike, restore) in strikes {
        let fields = super::instruction(&strike, restore)
            .into_iter()
            .map(|(name, value)| {
                (
                    name.to_owned(),
                    redis::Value::BulkString(value.into_bytes()),
                )
            })
            .collect();
        assert_eq!(super::monitor::decode(&fields), Some((strike, restore)));
    }
}

#[rstest]
#[case::no_direction(&[("function", "charge")])]
#[case::bad_operator(&[("direction", "strike"), ("parameter", "x"), ("operator", "~"), ("value", "1")])]
#[case::bad_value(&[("direction", "strike"), ("parameter", "x"), ("operator", "=="), ("value", "{")])]
#[case::no_operator(&[("direction", "strike"), ("parameter", "x"), ("value", "1")])]
#[case::no_value(&[("direction", "strike"), ("parameter", "x"), ("operator", "==")])]
#[case::not_bulk(&[])]
fn ignores_instructions_it_cannot_read(#[case] fields: &[(&str, &str)]) {
    let mut fields: std::collections::HashMap<String, redis::Value> = fields
        .iter()
        .map(|(name, value)| {
            (
                (*name).to_owned(),
                redis::Value::BulkString(value.as_bytes().to_vec()),
            )
        })
        .collect();
    if fields.is_empty() {
        fields.insert("direction".to_owned(), redis::Value::Int(1));
    }
    assert_eq!(super::monitor::decode(&fields), None);
}
