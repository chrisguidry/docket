//! Text output in the shape pydocket's CLI prints with rich: its tables,
//! its relative times, and Python's `repr` of a value.

use std::fmt::Write as _;

use chrono::{DateTime, Local, TimeDelta, Utc};
use serde_json::Value;

/// How a column lines up its cells.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Justify {
    Left,
    Right,
}

/// A table drawn as rich draws its default one, with a heavy head, but no
/// colors and no wrapping to the terminal's width.
pub(super) struct Table {
    title: String,
    columns: Vec<(String, Justify)>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn column(mut self, header: &str, justify: Justify) -> Self {
        self.columns.push((header.to_owned(), justify));
        self
    }

    /// Adds a row.  A cell can hold several lines.
    pub fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    pub fn render(&self) -> String {
        let widths: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, (header, _))| {
                self.rows
                    .iter()
                    .flat_map(|row| row[index].lines())
                    .map(width)
                    .chain([width(header)])
                    .max()
                    .unwrap_or_default()
            })
            .collect();
        let total = widths.iter().map(|width| width + 3).sum::<usize>() + 1;

        let mut out = String::new();
        for line in wrap(&self.title, total) {
            let left = (total - width(&line)) / 2;
            let right = total - width(&line) - left;
            let _ = writeln!(out, "{}{line}{}", " ".repeat(left), " ".repeat(right));
        }
        let rule = |left: &str, fill: &str, middle: &str, right: &str| {
            let segments: Vec<String> = widths.iter().map(|width| fill.repeat(width + 2)).collect();
            format!("{left}{}{right}\n", segments.join(middle))
        };
        out.push_str(&rule("┏", "━", "┳", "┓"));
        let headers: Vec<String> = self
            .columns
            .iter()
            .map(|(header, _)| header.clone())
            .collect();
        out.push_str(&self.line(&headers, &widths, "┃"));
        out.push_str(&rule("┡", "━", "╇", "┩"));
        for row in &self.rows {
            let height = row
                .iter()
                .map(|cell| cell.lines().count())
                .max()
                .unwrap_or(1);
            for index in 0..height.max(1) {
                let cells: Vec<String> = row
                    .iter()
                    .map(|cell| cell.lines().nth(index).unwrap_or_default().to_owned())
                    .collect();
                out.push_str(&self.line(&cells, &widths, "│"));
            }
        }
        out.push_str(&rule("└", "─", "┴", "┘"));
        out
    }

    fn line(&self, cells: &[String], widths: &[usize], edge: &str) -> String {
        let padded: Vec<String> = cells
            .iter()
            .zip(widths)
            .zip(&self.columns)
            .map(|((cell, &width), (_, justify))| match justify {
                Justify::Left => format!(" {cell:<width$} "),
                Justify::Right => format!(" {cell:>width$} "),
            })
            .collect();
        format!("{edge}{}{edge}\n", padded.join(edge))
    }
}

fn width(text: &str) -> usize {
    text.chars().count()
}

/// Splits `text` into lines at most `limit` wide, at spaces where it can,
/// as rich wraps a title that is wider than its table.
fn wrap(text: &str, limit: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for paragraph in text.lines() {
        let mut line = String::new();
        for word in paragraph.split(' ') {
            let mut word = word.to_owned();
            if !line.is_empty() && width(&line) + 1 + width(&word) > limit {
                lines.push(std::mem::take(&mut line));
            }
            while width(&word) > limit {
                let rest = word.split_off(word.char_indices().nth(limit).map_or(0, |(at, _)| at));
                lines.push(std::mem::replace(&mut word, rest));
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(&word);
        }
        lines.push(line);
    }
    lines
}

/// A time as pydocket's `local_time` prints it, in the local time zone.
pub(super) fn local_time(when: DateTime<Utc>) -> String {
    when.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S %z")
        .to_string()
}

/// `when` relative to `now`, as pydocket's `relative_time` prints it: a
/// distance within half an hour, and a local time beyond that.
pub(super) fn relative_time(now: DateTime<Utc>, when: DateTime<Utc>) -> String {
    let delta = now - when;
    let half_hour = TimeDelta::minutes(30);
    if delta < -half_hour || delta >= half_hour {
        format!("at {}", local_time(when))
    } else if delta < TimeDelta::zero() {
        format!("in {}", timedelta(-delta))
    } else {
        format!("{} ago", timedelta(delta))
    }
}

/// A duration as Python prints a `timedelta`, such as `0:00:05.250000` or
/// `-1 day, 23:59:59`.
pub(super) fn timedelta(delta: TimeDelta) -> String {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    let micros = delta.num_microseconds().unwrap_or(i64::MAX);
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rest = micros.rem_euclid(MICROS_PER_DAY);
    let (seconds, fraction) = (rest / 1_000_000, rest % 1_000_000);
    let mut text = String::new();
    if days != 0 {
        let plural = if days.abs() == 1 { "" } else { "s" };
        let _ = write!(text, "{days} day{plural}, ");
    }
    let _ = write!(
        text,
        "{}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if fraction != 0 {
        let _ = write!(text, ".{fraction:06}");
    }
    text
}

/// A string as Python's `repr` prints it.
pub(super) fn repr_str(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::from(quote);
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if character == quote => {
                out.push('\\');
                out.push(quote);
            }
            _ => out.push(character),
        }
    }
    out.push(quote);
    out
}

/// A JSON value as Python's `repr` prints the value it loads as.
pub(super) fn repr(value: &Value) -> String {
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
        Value::Object(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|(name, value)| format!("{}: {}", repr_str(name), repr(value)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

#[cfg(test)]
mod tests;
