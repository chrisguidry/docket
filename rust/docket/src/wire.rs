//! How times travel to Redis: ISO 8601 text in messages and events, and Unix
//! seconds where the Lua scripts compare them.

use chrono::{DateTime, SecondsFormat, Utc};

/// The text form, such as `2026-10-05T18:26:03.529728+00:00`.
pub(crate) fn iso(when: DateTime<Utc>) -> String {
    when.to_rfc3339_opts(SecondsFormat::Micros, false)
}

pub(crate) fn parse_iso(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|when| when.with_timezone(&Utc))
}

/// Seconds since the Unix epoch, with microseconds.
pub(crate) fn seconds(when: DateTime<Utc>) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "microseconds since the epoch stay well within f64's 53 bits"
    )]
    let micros = when.timestamp_micros() as f64;
    micros / 1_000_000.0
}

/// The time `seconds` since the Unix epoch, to the microsecond, as
/// [`seconds`] writes it.
pub(crate) fn from_seconds(seconds: f64) -> DateTime<Utc> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "microseconds since the epoch stay well within i64"
    )]
    let micros = (seconds * 1_000_000.0).round() as i64;
    DateTime::from_timestamp_micros(micros).unwrap_or(DateTime::<Utc>::MAX_UTC)
}

/// Milliseconds since the Unix epoch.
pub(crate) fn millis(when: DateTime<Utc>) -> i64 {
    when.timestamp_millis()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{iso, millis, parse_iso, seconds};

    #[test]
    fn round_trips_through_text() {
        let when = Utc.timestamp_micros(1_791_228_363_529_728).unwrap();
        assert_eq!(iso(when), "2026-10-05T19:26:03.529728+00:00");
        assert_eq!(parse_iso(&iso(when)), Some(when));
    }

    #[test]
    fn reads_other_offsets_as_utc() {
        let when = parse_iso("2026-10-05T15:26:03-04:00").unwrap();
        assert_eq!(iso(when), "2026-10-05T19:26:03.000000+00:00");
    }

    #[test]
    fn rejects_text_that_is_not_a_time() {
        assert_eq!(parse_iso("soon"), None);
    }

    #[test]
    fn converts_to_epoch_numbers() {
        let when = Utc.timestamp_micros(1_791_228_363_529_728).unwrap();
        assert!((seconds(when) - 1_791_228_363.529_728).abs() < 1e-6);
        assert_eq!(millis(when), 1_791_228_363_529);
    }
}
