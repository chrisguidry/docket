//! Metric and label names, built from OpenTelemetry's names and units the
//! way pydocket's vendored exporter builds them.

/// Replaces each run of characters other than ASCII letters, digits, and
/// `extra` with one `_`.  An `_` counts as part of a run, so `a__b` becomes
/// `a_b`.
fn replace_runs(text: &str, extra: &[char]) -> String {
    let mut sanitized = String::with_capacity(text.len());
    let mut in_run = false;
    for character in text.chars() {
        let kept = character.is_ascii_alphanumeric() || extra.contains(&character);
        if kept {
            sanitized.push(character);
        } else if !in_run {
            sanitized.push('_');
        }
        in_run = !kept;
    }
    sanitized
}

/// Replaces a leading digit with `_`, since a Prometheus name cannot start
/// with one.
fn without_leading_digit(text: &str) -> String {
    match text.chars().next() {
        Some(first) if first.is_ascii_digit() => format!("_{}", &text[1..]),
        _ => text.to_owned(),
    }
}

/// A metric name as Prometheus allows it: ASCII letters, digits, `_`, and
/// `:`, not starting with a digit.
pub(super) fn metric_name(name: &str) -> String {
    replace_runs(&without_leading_digit(name), &[':'])
}

/// A label name as Prometheus allows it: ASCII letters, digits, and `_`, not
/// starting with a digit.  So `docket.name` becomes `docket_name`.
pub(super) fn label_name(key: &str) -> String {
    replace_runs(&without_leading_digit(key), &[])
}

/// The UCUM units that Prometheus spells out in a metric name.  The unit
/// `1` is a plain count, so it adds nothing.
const SPELLED: &[(&str, &str)] = &[
    ("d", "days"),
    ("h", "hours"),
    ("min", "minutes"),
    ("s", "seconds"),
    ("ms", "milliseconds"),
    ("us", "microseconds"),
    ("ns", "nanoseconds"),
    ("By", "bytes"),
    ("KiBy", "kibibytes"),
    ("MiBy", "mebibytes"),
    ("GiBy", "gibibytes"),
    ("TiBy", "tibibytes"),
    ("KBy", "kilobytes"),
    ("MBy", "megabytes"),
    ("GBy", "gigabytes"),
    ("TBy", "terabytes"),
    ("m", "meters"),
    ("V", "volts"),
    ("A", "amperes"),
    ("J", "joules"),
    ("W", "watts"),
    ("g", "grams"),
    ("Cel", "celsius"),
    ("Hz", "hertz"),
    ("1", ""),
    ("%", "percent"),
];

/// The time units that follow `per_` in a rate, such as `By/s`.
const PER: &[(&str, &str)] = &[
    ("s", "second"),
    ("m", "minute"),
    ("h", "hour"),
    ("d", "day"),
    ("w", "week"),
    ("mo", "month"),
    ("y", "year"),
];

fn lookup(table: &[(&str, &'static str)], unit: &str) -> Option<&'static str> {
    table
        .iter()
        .find(|(from, _)| *from == unit)
        .map(|(_, to)| *to)
}

/// Removes an annotation in braces, such as `{packets}`.  It spans from
/// the first `{` to the last `}`, as a greedy match of `{.*}` does.
fn without_annotation(unit: &str) -> String {
    match (unit.find('{'), unit.rfind('}')) {
        (Some(start), Some(end)) if start < end => {
            format!("{}{}", &unit[..start], &unit[end + 1..])
        }
        _ => unit.to_owned(),
    }
}

/// The suffix that a unit adds to a metric name: `s` is `seconds`, `By/s`
/// is `bytes_per_second`, and `1` adds nothing.
pub(super) fn unit_suffix(unit: &str) -> String {
    let unit = without_annotation(unit);
    if let Some(known) = lookup(SPELLED, &unit) {
        return known.to_owned();
    }
    if let Some((top, bottom)) = unit.split_once('/') {
        let bottom = replace_runs(bottom, &[':']);
        if !bottom.is_empty() {
            let top = replace_runs(top, &[':']);
            let top = lookup(SPELLED, &top).map_or(top, str::to_owned);
            let bottom = lookup(PER, &bottom).map_or(bottom, str::to_owned);
            return if top.is_empty() {
                format!("per_{bottom}")
            } else {
                format!("{top}_per_{bottom}")
            };
        }
    }
    replace_runs(&unit, &[':']).trim_matches('_').to_owned()
}
