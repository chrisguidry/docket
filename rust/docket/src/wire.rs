//! How times travel to Redis: ISO 8601 text in messages and events, and Unix
//! seconds where the Lua scripts compare them.

use jiff::Timestamp;
use jiff::fmt::temporal::DateTimePrinter;
use jiff::tz::Offset;

/// The text form, such as `2026-10-05T18:26:03.529728+00:00`.
pub(crate) fn iso(when: Timestamp) -> String {
    DateTimePrinter::new()
        .precision(Some(6))
        .timestamp_with_offset_to_string(&when, Offset::UTC)
}

pub(crate) fn parse_iso(text: &str) -> Option<Timestamp> {
    text.parse().ok()
}

/// Seconds since the Unix epoch, with microseconds.
pub(crate) fn seconds(when: Timestamp) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "microseconds since the epoch stay well within f64's 53 bits"
    )]
    let micros = when.as_microsecond() as f64;
    micros / 1_000_000.0
}

/// The time `seconds` since the Unix epoch, to the microsecond, as
/// [`seconds`] writes it.
pub(crate) fn from_seconds(seconds: f64) -> Timestamp {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "microseconds since the epoch stay well within i64"
    )]
    let micros = (seconds * 1_000_000.0).round() as i64;
    Timestamp::from_microsecond(micros).unwrap_or(Timestamp::MAX)
}

/// Milliseconds since the Unix epoch.
pub(crate) fn millis(when: Timestamp) -> i64 {
    when.as_millisecond()
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;

    use super::{iso, millis, parse_iso, seconds};

    #[test]
    fn round_trips_through_text() {
        let when = Timestamp::from_microsecond(1_791_228_363_529_728).unwrap();
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
        let when = Timestamp::from_microsecond(1_791_228_363_529_728).unwrap();
        assert!((seconds(when) - 1_791_228_363.529_728).abs() < 1e-6);
        assert_eq!(millis(when), 1_791_228_363_529);
    }
}
