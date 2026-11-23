use std::time::{Duration, SystemTime};

use super::{TimeZone, Timestamp};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_micros(seconds * 1_000_000).unwrap()
}

const TEN_THOUSAND_YEARS: Duration = Duration::from_hours(10_000 * 365 * 24);

#[test]
fn adds_and_subtracts_durations() {
    assert_eq!(
        at(1_800_000_000) + Duration::from_secs(90),
        at(1_800_000_090)
    );
    assert_eq!(
        at(1_800_000_000) - Duration::from_secs(90),
        at(1_799_999_910)
    );
}

#[test]
fn saturates_at_the_ends_of_the_range() {
    let when = at(1_800_000_000);
    assert_eq!(when.checked_add(TEN_THOUSAND_YEARS), None);
    assert_eq!(when.saturating_add(TEN_THOUSAND_YEARS), Timestamp::MAX);
    assert_eq!(when.saturating_add(Duration::MAX), Timestamp::MAX);
    assert_eq!(when.checked_sub(TEN_THOUSAND_YEARS * 2), None);
    assert_eq!(when.saturating_sub(TEN_THOUSAND_YEARS * 2), Timestamp::MIN);
    assert_eq!(when.saturating_sub(Duration::MAX), Timestamp::MIN);
}

#[test]
#[should_panic(expected = "the sum is later than Timestamp::MAX")]
fn adding_past_the_end_panics() {
    let _ = Timestamp::MAX + Duration::from_secs(1);
}

#[test]
#[should_panic(expected = "the difference is earlier than Timestamp::MIN")]
fn subtracting_past_the_start_panics() {
    let _ = Timestamp::MIN - Duration::from_secs(1);
}

#[test]
fn measures_the_time_between_two_timestamps() {
    let (earlier, later) = (at(1_800_000_000), at(1_800_000_005));
    assert_eq!(
        later.checked_duration_since(earlier),
        Some(Duration::from_secs(5))
    );
    assert_eq!(earlier.checked_duration_since(later), None);
    assert_eq!(earlier.saturating_duration_since(later), Duration::ZERO);
}

#[test]
fn counts_microseconds_from_the_epoch() {
    let when = Timestamp::from_unix_micros(1_791_228_363_529_728).unwrap();
    assert_eq!(when.as_unix_micros(), 1_791_228_363_529_728);
    assert_eq!(Timestamp::from_unix_micros(i64::MAX), None);
}

#[test]
fn prints_and_parses_rfc_3339() {
    let when = Timestamp::from_unix_micros(1_791_228_363_529_728).unwrap();
    assert_eq!(when.to_string(), "2026-10-05T19:26:03.529728Z");
    assert_eq!(format!("{when:?}"), "2026-10-05T19:26:03.529728Z");
    assert_eq!(
        "2026-10-05T15:26:03.529728-04:00"
            .parse::<Timestamp>()
            .unwrap(),
        when
    );
    let error = "soon".parse::<Timestamp>().unwrap_err().to_string();
    assert!(error.contains("soon is not a timestamp"), "{error}");
}

#[test]
fn converts_to_and_from_system_time() {
    let when = at(1_800_000_000);
    let system = SystemTime::UNIX_EPOCH + Duration::from_hours(500_000);
    assert_eq!(Timestamp::from(system), when);
    assert_eq!(SystemTime::from(when), system);
}

#[test]
fn system_times_outside_the_range_saturate() {
    let far = SystemTime::UNIX_EPOCH + TEN_THOUSAND_YEARS;
    assert_eq!(Timestamp::from(far), Timestamp::MAX);
    let before = SystemTime::UNIX_EPOCH - TEN_THOUSAND_YEARS * 2;
    assert_eq!(Timestamp::from(before), Timestamp::MIN);
}

#[test]
fn finds_time_zones_by_name() {
    assert!(TimeZone::get("America/Los_Angeles").is_ok());
    let error = TimeZone::get("Mars/Olympus_Mons").unwrap_err().to_string();
    assert!(
        error.contains("Mars/Olympus_Mons is not a time zone"),
        "{error}"
    );
}

#[cfg(feature = "tzdb-bundle")]
#[test]
fn reads_the_bundle_when_the_system_database_cannot_resolve_the_name() {
    let empty = jiff::tz::TimeZoneDatabase::none();
    let zone = super::zone_named(&empty, "America/Los_Angeles").unwrap();
    assert_eq!(zone.0.iana_name(), Some("America/Los_Angeles"));
}

/// A `TZif` file, version 1, for a zone fixed at five hours east of UTC.
fn fixed_five_hours_east_tzif() -> Vec<u8> {
    let mut tzif = b"TZif".to_vec();
    tzif.extend([0; 16]);
    for count in [0_u32, 0, 0, 0, 1, 4] {
        tzif.extend(count.to_be_bytes());
    }
    tzif.extend(18_000_i32.to_be_bytes());
    tzif.extend([0, 0]);
    tzif.extend(b"FIV\0");
    tzif
}

#[test]
fn prefers_the_system_database_to_the_bundle() {
    let tzdir = std::env::temp_dir().join(format!("docket-tzdir-{}", std::process::id()));
    std::fs::create_dir_all(tzdir.join("America")).unwrap();
    std::fs::write(
        tzdir.join("America/Los_Angeles"),
        fixed_five_hours_east_tzif(),
    )
    .unwrap();
    let system = jiff::tz::TimeZoneDatabase::from_dir(&tzdir).unwrap();
    let zone = super::zone_named(&system, "America/Los_Angeles").unwrap();
    std::fs::remove_dir_all(&tzdir).unwrap();

    let offset = zone.0.to_offset(jiff::Timestamp::UNIX_EPOCH);
    assert_eq!(offset, jiff::tz::Offset::constant(5));
}

#[cfg(not(feature = "tzdb-bundle"))]
#[test]
fn without_the_bundle_a_missing_system_database_fails() {
    let empty = jiff::tz::TimeZoneDatabase::none();
    let error = super::zone_named(&empty, "America/Los_Angeles")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("America/Los_Angeles is not a time zone"),
        "{error}"
    );
    assert!(
        error.contains("no time zone database configured"),
        "{error}"
    );
}

#[cfg(feature = "chrono")]
mod chrono {
    use chrono::{DateTime, FixedOffset, TimeZone as _, Utc};

    use super::{Timestamp, at};

    #[test]
    fn converts_to_and_from_chrono() {
        let chrono = Utc.timestamp_opt(1_800_000_000, 5).unwrap();
        let when = Timestamp::from(chrono);
        assert_eq!(when, at(1_800_000_000) + std::time::Duration::from_nanos(5));
        assert_eq!(DateTime::<Utc>::from(when), chrono);
    }

    #[test]
    fn reads_any_offset() {
        let offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let chrono = offset.timestamp_opt(1_800_000_000, 0).unwrap();
        assert_eq!(Timestamp::from(chrono), at(1_800_000_000));
    }

    #[test]
    fn converts_times_before_the_epoch() {
        let chrono = Utc.timestamp_opt(-1, 250_000_000).unwrap();
        assert_eq!(DateTime::<Utc>::from(Timestamp::from(chrono)), chrono);
    }

    #[test]
    fn times_outside_the_range_saturate() {
        assert_eq!(Timestamp::from(DateTime::<Utc>::MAX_UTC), Timestamp::MAX);
        assert_eq!(Timestamp::from(DateTime::<Utc>::MIN_UTC), Timestamp::MIN);
    }

    #[test]
    fn a_leap_second_reads_as_the_nanosecond_before_it() {
        let leap = Utc.timestamp_opt(1_483_228_799, 1_500_000_000).unwrap();
        assert_eq!(
            Timestamp::from(leap),
            at(1_483_228_799) + std::time::Duration::from_nanos(999_999_999)
        );
    }
}

#[cfg(feature = "chrono-tz")]
#[test]
fn converts_chrono_tz_zones() {
    let zone = TimeZone::try_from(chrono_tz::Asia::Kolkata).unwrap();
    assert_eq!(zone, TimeZone::get("Asia/Kolkata").unwrap());
}

#[cfg(feature = "jiff-02")]
#[test]
fn converts_to_and_from_jiff() {
    let jiff = jiff::Timestamp::from_second(1_800_000_000).unwrap();
    assert_eq!(Timestamp::from(jiff), at(1_800_000_000));
    assert_eq!(jiff::Timestamp::from(at(1_800_000_000)), jiff);
    let zone = jiff::tz::TimeZone::get("Asia/Kolkata").unwrap();
    let docket = TimeZone::from(zone.clone());
    assert_eq!(jiff::tz::TimeZone::from(docket), zone);
}
