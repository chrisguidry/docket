//! The text of docket's log lines, which reads the same as pydocket's.

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
                (Logged::Value, Some(value)) => value.to_string(),
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
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
