use rstest::rstest;
use serde_json::{Value, json};

use super::{parse_operator, parse_value};

#[rstest]
#[case::text("seven", json!("seven"))]
#[case::str("str:a:b", json!("a:b"))]
#[case::int("int:7", json!(7))]
#[case::negative_int("int:-7", json!(-7))]
#[case::big_int("int:18446744073709551615", json!(18_446_744_073_709_551_615_u64))]
#[case::builtins_int("builtins.int:7", json!(7))]
#[case::float("float:2.5", json!(2.5))]
#[case::bool_true("bool:True", json!(true))]
#[case::bool_other("bool:yes", json!(false))]
#[case::json("json:[1,10]", json!([1, 10]))]
fn values_read_as_pydocket_reads_them(#[case] text: &str, #[case] expected: Value) {
    assert_eq!(parse_value(text), Ok(expected));
}

#[rstest]
#[case::bad_int("int:x", "x is not an int")]
#[case::bad_float("float:x", "x is not a finite float")]
#[case::infinite_float("float:inf", "inf is not a finite float")]
#[case::bad_json("json:[", "[ is not JSON")]
#[case::python_type("datetime.timedelta:60", "datetime.timedelta is not a type")]
#[case::colon_in_text("user:42", "user is not a type")]
fn values_that_are_not_json_are_refused(#[case] text: &str, #[case] expected: &str) {
    let error = parse_value(text).unwrap_err();
    assert!(error.starts_with(expected), "{error}");
}

#[test]
fn operators_are_pydockets() {
    assert!(parse_operator("between").is_ok());
    assert!(
        parse_operator("=~")
            .unwrap_err()
            .starts_with("=~ is not an operator")
    );
}
