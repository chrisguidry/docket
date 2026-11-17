//! The time a docket goes by.  A docket on Redis reads the system clock.  A
//! `memory://` docket reads a clock that a test can move forward, which
//! every docket on its URL shares; see [`crate::testing::advance_time`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use jiff::{SignedDuration, Timestamp};

/// `when` plus `delay`, or [`Timestamp::MAX`] when the sum is past the end
/// of jiff's range, in the year 9999.
pub(crate) fn later(when: Timestamp, delay: std::time::Duration) -> Timestamp {
    SignedDuration::try_from(delay)
        .ok()
        .and_then(|delay| when.checked_add(delay).ok())
        .unwrap_or(Timestamp::MAX)
}

/// A docket's clock.
#[derive(Clone, Debug, Default)]
pub(crate) struct Clock(Option<Arc<Moved>>);

/// How far a test has moved a `memory://` clock.
#[derive(Debug, Default)]
pub(crate) struct Moved {
    ahead_micros: AtomicI64,
    skip_idle: AtomicBool,
}

impl Clock {
    /// A clock that starts at the system's time and that a test can move.
    #[cfg(feature = "memory")]
    pub fn movable() -> Self {
        Self(Some(Arc::default()))
    }

    pub fn now(&self) -> Timestamp {
        let now = Timestamp::now();
        match &self.0 {
            Some(moved) => now
                .saturating_add(SignedDuration::from_micros(
                    moved.ahead_micros.load(Ordering::Relaxed),
                ))
                .expect("a SignedDuration has no calendar units"),
            None => now,
        }
    }

    /// The clock, when a worker with nothing to run moves it to the next
    /// scheduled task.
    pub fn skipping_idle_time(&self) -> Option<&Moved> {
        self.0
            .as_deref()
            .filter(|moved| moved.skip_idle.load(Ordering::Relaxed))
    }

    #[cfg(feature = "memory")]
    pub fn moved(&self) -> &Moved {
        self.0
            .as_deref()
            .expect("only a memory:// docket's clock can move")
    }
}

impl Moved {
    /// Moves the clock to `when`, unless it is there already.  Two workers
    /// that move it at once leave it at the later time.
    pub fn advance_to(&self, when: Timestamp) {
        let ahead = when.duration_since(Timestamp::now()).as_micros();
        let ahead = i64::try_from(ahead).unwrap_or(i64::MAX);
        self.ahead_micros.fetch_max(ahead, Ordering::Relaxed);
    }

    #[cfg(feature = "memory")]
    pub fn advance(&self, by: std::time::Duration) {
        let by = i64::try_from(by.as_micros()).unwrap_or(i64::MAX);
        self.ahead_micros.fetch_add(by, Ordering::Relaxed);
    }

    #[cfg(feature = "memory")]
    pub fn skip_idle_time(&self) {
        self.skip_idle.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use jiff::Timestamp;

    use super::later;

    #[test]
    fn adds_a_delay() {
        let when = Timestamp::from_second(1_800_000_000).unwrap();
        assert_eq!(
            later(when, Duration::from_secs(90)),
            Timestamp::from_second(1_800_000_090).unwrap()
        );
    }

    #[test]
    fn stops_at_the_end_of_the_range() {
        let when = Timestamp::from_second(1_800_000_000).unwrap();
        let ten_thousand_years = Duration::from_hours(10_000 * 365 * 24);
        assert_eq!(later(when, ten_thousand_years), Timestamp::MAX);
        assert_eq!(later(when, Duration::MAX), Timestamp::MAX);
    }
}
