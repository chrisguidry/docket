//! The expected spellings come from pydocket's vendored exporter and from
//! `prometheus_client`'s `floatToGoString`, run on the same inputs.

use opentelemetry::Value;
use rstest::rstest;

use super::super::names::{label_name, metric_name, unit_suffix};
use super::super::values::{go_float, label_value, python_repr};

#[rstest]
#[case::plain("docket_tasks_added", "docket_tasks_added")]
#[case::dots("http.server.duration", "http_server_duration")]
#[case::colons_stay("a:b", "a:b")]
#[case::runs_collapse("a.-_b", "a_b")]
#[case::leading_digit("1st.place", "_st_place")]
fn sanitizes_metric_names(#[case] name: &str, #[case] expected: &str) {
    assert_eq!(metric_name(name), expected);
}

#[rstest]
#[case::dots("docket.name", "docket_name")]
#[case::colons_go("a:b", "a_b")]
#[case::underscores_collapse("a__b", "a_b")]
#[case::leading_digit("9.x", "_x")]
#[case::non_ascii("café", "caf_")]
fn sanitizes_label_names(#[case] key: &str, #[case] expected: &str) {
    assert_eq!(label_name(key), expected);
}

#[rstest]
#[case::seconds("s", "seconds")]
#[case::count("1", "")]
#[case::bytes("By", "bytes")]
#[case::annotation_only("{packets}", "")]
#[case::annotated("{packets}By", "bytes")]
#[case::rate("By/s", "bytes_per_second")]
#[case::annotated_rate("{request}/s", "per_second")]
#[case::unknown_rate("km/h", "km_per_hour")]
#[case::unknown_per("By/fortnight", "bytes_per_fortnight")]
#[case::empty_per("x/", "x")]
#[case::unknown("km", "km")]
#[case::stripped(" foo bar ", "foo_bar")]
#[case::unclosed("{x", "x")]
fn maps_units(#[case] unit: &str, #[case] expected: &str) {
    assert_eq!(unit_suffix(unit), expected);
}

#[rstest]
#[case::integral(2.0, "2.0")]
#[case::fraction(0.3, "0.3")]
#[case::shortest(0.1 + 0.2, "0.30000000000000004")]
#[case::zero(0.0, "0.0")]
#[case::negative_zero(-0.0, "-0.0")]
#[case::small_plain(0.0001, "0.0001")]
#[case::small_exponent(1e-5, "1e-05")]
#[case::large_plain(1e15, "1000000000000000.0")]
#[case::large_exponent(1e16, "1e+16")]
#[case::exponent_with_digits(-1.5e100, "-1.5e+100")]
#[case::nan(f64::NAN, "nan")]
#[case::infinity(f64::INFINITY, "inf")]
#[case::negative_infinity(f64::NEG_INFINITY, "-inf")]
fn writes_floats_as_python_repr_does(#[case] value: f64, #[case] expected: &str) {
    assert_eq!(python_repr(value), expected);
}

#[rstest]
#[case::one(1.0, "1.0")]
#[case::six_digits(123_456.5, "123456.5")]
#[case::seven_digits(1_234_567.0, "1.234567e+06")]
#[case::round_million(1e6, "1e+06")]
#[case::fraction_after_seven(12_345_678.9, "1.23456789e+07")]
#[case::large_integral(9_007_199_254_740_992.0, "9.007199254740992e+15")]
#[case::already_exponent(1e16, "1e+16")]
#[case::negative_large(-1_234_567.0, "-1234567.0")]
#[case::nan(f64::NAN, "NaN")]
#[case::infinity(f64::INFINITY, "+Inf")]
#[case::negative_infinity(f64::NEG_INFINITY, "-Inf")]
fn writes_samples_as_float_to_go_string_does(#[case] value: f64, #[case] expected: &str) {
    assert_eq!(go_float(value), expected);
}

#[rstest]
#[case::string(Value::from("a b"), "a b")]
#[case::boolean(Value::Bool(false), "false")]
#[case::integer(Value::I64(-7), "-7")]
#[case::float(Value::F64(2.0), "2.0")]
fn writes_label_values(#[case] value: Value, #[case] expected: &str) {
    assert_eq!(label_value(&value), expected);
}
