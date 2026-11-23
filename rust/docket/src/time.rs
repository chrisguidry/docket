//! The time types in docket's API.  docket-rs computes times with jiff 0.2,
//! and keeps jiff out of its signatures, so that moving to a later jiff does
//! not change them.  The `chrono`, `chrono-tz`, and `jiff-02` features
//! convert these types to and from those crates' types.

use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use jiff::SignedDuration;

use crate::error::{Error, Result};

/// An instant, to the nanosecond, from the year -9999 to the year 9999.
///
/// It prints and parses as RFC 3339 in UTC, such as
/// `2026-10-05T19:26:03.529728Z`.  Parsing accepts any offset.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub(crate) jiff::Timestamp);

impl Timestamp {
    /// 1970-01-01 at midnight, UTC.
    pub const UNIX_EPOCH: Self = Self(jiff::Timestamp::UNIX_EPOCH);
    /// The earliest timestamp, in the year -9999.
    pub const MIN: Self = Self(jiff::Timestamp::MIN);
    /// The latest timestamp, at the end of the year 9999.
    pub const MAX: Self = Self(jiff::Timestamp::MAX);

    /// The system clock's time.
    #[must_use]
    pub fn now() -> Self {
        Self(jiff::Timestamp::now())
    }

    /// The time `micros` microseconds after the Unix epoch, or `None` when it
    /// is outside the range from [`Timestamp::MIN`] to [`Timestamp::MAX`].
    #[must_use]
    pub fn from_unix_micros(micros: i64) -> Option<Self> {
        jiff::Timestamp::from_microsecond(micros).ok().map(Self)
    }

    /// Microseconds since the Unix epoch, rounded toward zero.
    #[must_use]
    pub fn as_unix_micros(self) -> i64 {
        self.0.as_microsecond()
    }

    /// `self` plus `duration`, or `None` when the sum is later than
    /// [`Timestamp::MAX`].
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let duration = SignedDuration::try_from(duration).ok()?;
        self.0.checked_add(duration).ok().map(Self)
    }

    /// `self` plus `duration`, or [`Timestamp::MAX`] when the sum is later.
    #[must_use]
    pub fn saturating_add(self, duration: Duration) -> Self {
        self.checked_add(duration).unwrap_or(Self::MAX)
    }

    /// `self` minus `duration`, or `None` when the difference is earlier
    /// than [`Timestamp::MIN`].
    #[must_use]
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        let duration = SignedDuration::try_from(duration).ok()?;
        self.0.checked_sub(duration).ok().map(Self)
    }

    /// `self` minus `duration`, or [`Timestamp::MIN`] when the difference is
    /// earlier.
    #[must_use]
    pub fn saturating_sub(self, duration: Duration) -> Self {
        self.checked_sub(duration).unwrap_or(Self::MIN)
    }

    /// How long after `earlier` `self` is, or `None` when `earlier` is later
    /// than `self`.
    #[must_use]
    pub fn checked_duration_since(self, earlier: Self) -> Option<Duration> {
        Duration::try_from(self.0.duration_since(earlier.0)).ok()
    }

    /// How long after `earlier` `self` is, or zero when `earlier` is later
    /// than `self`.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.checked_duration_since(earlier).unwrap_or_default()
    }
}

/// # Panics
///
/// When the sum is later than [`Timestamp::MAX`].
impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, duration: Duration) -> Self {
        self.checked_add(duration)
            .expect("the sum is later than Timestamp::MAX")
    }
}

/// # Panics
///
/// When the difference is earlier than [`Timestamp::MIN`].
impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, duration: Duration) -> Self {
        self.checked_sub(duration)
            .expect("the difference is earlier than Timestamp::MIN")
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        text.parse()
            .map(Self)
            .map_err(|error| Error::Invalid(format!("{text} is not a timestamp: {error}")))
    }
}

/// Saturates at [`Timestamp::MIN`] or [`Timestamp::MAX`] when the time is
/// outside their range.
impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        jiff::Timestamp::try_from(time).map_or_else(
            |_| {
                if time > SystemTime::UNIX_EPOCH {
                    Self::MAX
                } else {
                    Self::MIN
                }
            },
            Self,
        )
    }
}

impl From<Timestamp> for SystemTime {
    fn from(when: Timestamp) -> Self {
        Self::from(when.0)
    }
}

/// Saturates at [`Timestamp::MIN`] or [`Timestamp::MAX`] when the time is
/// outside their range.  A leap second reads as the last nanosecond before
/// it.
#[cfg(feature = "chrono")]
impl<Tz: chrono::TimeZone> From<chrono::DateTime<Tz>> for Timestamp {
    fn from(when: chrono::DateTime<Tz>) -> Self {
        let seconds = when.timestamp();
        let nanos = i32::try_from(when.timestamp_subsec_nanos().min(999_999_999))
            .expect("nanoseconds below one second fit in i32");
        jiff::Timestamp::new(seconds, nanos)
            .map_or_else(|_| if seconds > 0 { Self::MAX } else { Self::MIN }, Self)
    }
}

#[cfg(feature = "chrono")]
impl From<Timestamp> for chrono::DateTime<chrono::Utc> {
    fn from(when: Timestamp) -> Self {
        let nanos = when.0.as_nanosecond();
        let seconds = i64::try_from(nanos.div_euclid(1_000_000_000))
            .expect("jiff's seconds since the epoch fit in i64");
        let subsec = u32::try_from(nanos.rem_euclid(1_000_000_000))
            .expect("nanoseconds below one second fit in u32");
        Self::from_timestamp(seconds, subsec).expect("jiff's range is inside chrono's")
    }
}

#[cfg(feature = "jiff-02")]
impl From<jiff::Timestamp> for Timestamp {
    fn from(when: jiff::Timestamp) -> Self {
        Self(when)
    }
}

#[cfg(feature = "jiff-02")]
impl From<Timestamp> for jiff::Timestamp {
    fn from(when: Timestamp) -> Self {
        when.0
    }
}

/// A time zone that a [`Cron`](crate::Cron) schedule is read in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeZone(pub(crate) jiff::tz::TimeZone);

impl TimeZone {
    /// Coordinated Universal Time.
    pub const UTC: Self = Self(jiff::tz::TimeZone::UTC);

    /// The time zone with this IANA name, such as `America/Los_Angeles`.
    ///
    /// docket-rs reads the names from the system's time zone database, such
    /// as `/usr/share/zoneinfo`.  When that database is missing or has no
    /// zone by this name, it reads the copy that the `tzdb-bundle` feature,
    /// on by default, builds into the binary.  On Windows, jiff builds a
    /// copy in without the feature.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] when neither database has a zone with this name.
    pub fn get(name: &str) -> Result<Self> {
        zone_named(jiff::tz::db(), name)
    }
}

fn zone_named(system: &jiff::tz::TimeZoneDatabase, name: &str) -> Result<TimeZone> {
    system
        .get(name)
        .or_else(|error| {
            jiff::tz::TimeZoneDatabase::bundled()
                .get(name)
                .map_err(|_| error)
        })
        .map(TimeZone)
        .map_err(|error| Error::Invalid(format!("{name} is not a time zone: {error}")))
}

/// Looks the zone up by its IANA name, as [`TimeZone::get`] does.
#[cfg(feature = "chrono-tz")]
impl TryFrom<chrono_tz::Tz> for TimeZone {
    type Error = Error;

    fn try_from(zone: chrono_tz::Tz) -> Result<Self> {
        Self::get(zone.name())
    }
}

#[cfg(feature = "jiff-02")]
impl From<jiff::tz::TimeZone> for TimeZone {
    fn from(zone: jiff::tz::TimeZone) -> Self {
        Self(zone)
    }
}

#[cfg(feature = "jiff-02")]
impl From<TimeZone> for jiff::tz::TimeZone {
    fn from(zone: TimeZone) -> Self {
        zone.0
    }
}

#[cfg(test)]
mod tests;
