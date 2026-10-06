//! The text of docket's log lines, which reads the same as pydocket's.

use std::fmt::Write as _;

use serde_json::Value;

use crate::task::{Logged, TaskField};

/// A duration the way pydocket's `format_duration` writes it: milliseconds
/// in six columns, or whole seconds once it reaches 100 seconds.
pub(crate) fn format_duration(seconds: f64) -> String {
    if seconds < 100.0 {
        format!("{:6.0}ms", seconds * 1000.0)
    } else {
        format!("{seconds:6.0}s ")
    }
}

/// A run's call, such as `charge(customer=7, card=...){key}`.  Only the
/// fields marked to be logged show their values, so a log line never leaks
/// an argument nobody chose to show.
pub(crate) fn call_repr(function: &str, fields: &[TaskField], args: &Value, key: &str) -> String {
    let shown: Vec<String> = fields
        .iter()
        .map(|field| {
            let value = args.get(field.name);
            let text = match (field.logged, value) {
                (Logged::Value, Some(value)) => repr(value),
                (Logged::Length, Some(value)) => length(value),
                _ => "...".to_owned(),
            };
            format!("{}={text}", field.name)
        })
        .collect();
    format!("{function}({}){{{key}}}", shown.join(", "))
}

/// The length of a collection, in the brackets pydocket's `Logged` uses for
/// a list or a mapping.  A value that has no length shows in full.
fn length(value: &Value) -> String {
    match value {
        Value::Array(items) => format!("[len {}]", items.len()),
        Value::Object(entries) => format!("{{len {}}}", entries.len()),
        Value::String(text) => format!("[len {}]", text.chars().count()),
        other => repr(other),
    }
}

/// A value the way Python's `repr` writes it, which pydocket's `Logged`
/// uses: `'text'`, `True`, `None`, `[1, 2]`, and `{'a': 1}`.
fn repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => repr_str(text),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(repr).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(entries) => {
            let entries: Vec<String> = entries
                .iter()
                .map(|(key, value)| format!("{}: {}", repr_str(key), repr(value)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
    }
}

/// A string the way Python's `repr` quotes it: in single quotes, or in
/// double quotes when it holds a single quote and no double quote.
fn repr_str(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut written = String::from(quote);
    for character in text.chars() {
        match character {
            '\\' => written.push_str("\\\\"),
            '\n' => written.push_str("\\n"),
            '\r' => written.push_str("\\r"),
            '\t' => written.push_str("\\t"),
            c if c == quote => {
                written.push('\\');
                written.push(c);
            }
            // Writing to a String cannot fail.
            c if c.is_control() => {
                let _ = write!(written, "\\x{:02x}", u32::from(c));
            }
            c => written.push(c),
        }
    }
    written.push(quote);
    written
}

#[cfg(test)]
mod tests;
