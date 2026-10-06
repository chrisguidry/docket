//! Sample values and label values, spelled the way Python spells them, so
//! that docket-rs and pydocket serve the same text for the same metrics.

use std::fmt::Write;

use opentelemetry::{Array, Value};

/// A float as Python's `repr` writes it: the shortest digits that read back
/// as the same float, in plain form from `1e-4` up to below `1e16` and with
/// an exponent of at least two digits outside that range.
pub(super) fn python_repr(value: f64) -> String {
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        // NaN and the infinities, which Python writes as nan, inf, and -inf.
        return value.to_string().to_lowercase();
    };
    let exponent: i32 = exponent.parse().expect("an exponent is an integer");
    if (-4..16).contains(&exponent) {
        let plain = value.to_string();
        if plain.contains('.') {
            plain
        } else {
            format!("{plain}.0")
        }
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

/// A sample value as `prometheus_client`'s `floatToGoString` writes it.  Go
/// writes an exponent from seven digits before the point, sooner than
/// Python, so `1234567.0` is `1.234567e+06`.
pub(super) fn go_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned();
    }
    let repr = python_repr(value);
    match repr.find('.') {
        Some(point) if value > 0.0 && point > 6 => {
            let digits = format!("{}.{}{}", &repr[..1], &repr[1..point], &repr[point + 1..]);
            let mantissa = digits.trim_end_matches(['0', '.']);
            format!("{mantissa}e+{:02}", point - 1)
        }
        _ => repr,
    }
}

/// A label value: a string as it is, and anything else as Python's
/// `json.dumps` writes it.
pub(super) fn label_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.to_string(),
        Value::F64(number) => json_float(*number),
        Value::Array(Array::F64(numbers)) => json_list(numbers.iter().map(|n| json_float(*n))),
        Value::Array(Array::String(texts)) => {
            json_list(texts.iter().map(|text| json_string(text.as_str())))
        }
        // Booleans and integers, alone or in a list, display as JSON does,
        // apart from the space that json.dumps puts after each comma.
        other => other.to_string().replace(',', ", "),
    }
}

fn json_list(items: impl Iterator<Item = String>) -> String {
    format!("[{}]", items.collect::<Vec<_>>().join(", "))
}

/// A float as `json.dumps` writes it, which differs from `repr` only for
/// NaN and the infinities.
fn json_float(number: f64) -> String {
    match python_repr(number).as_str() {
        "nan" => "NaN".to_owned(),
        "inf" => "Infinity".to_owned(),
        "-inf" => "-Infinity".to_owned(),
        repr => repr.to_owned(),
    }
}

/// A string as `json.dumps` writes it: in ASCII, with every other character
/// as a `\u` escape of its UTF-16 code units.
fn json_string(text: &str) -> String {
    let mut json = String::from('"');
    for unit in text.encode_utf16() {
        match unit {
            0x22 => json.push_str("\\\""),
            0x5C => json.push_str("\\\\"),
            0x0A => json.push_str("\\n"),
            0x0D => json.push_str("\\r"),
            0x09 => json.push_str("\\t"),
            0x08 => json.push_str("\\b"),
            0x0C => json.push_str("\\f"),
            0x20..=0x7E => json.push(char::from(u8::try_from(unit).expect("ASCII fits a byte"))),
            _ => write!(json, "\\u{unit:04x}").expect("a String takes every write"),
        }
    }
    json.push('"');
    json
}
