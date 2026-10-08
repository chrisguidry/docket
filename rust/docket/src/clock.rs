//! The time a docket goes by.  A docket on Redis reads the system clock.  A
//! `memory://` docket reads a clock that a test can move forward, which
//! every docket on its URL shares; see [`crate::testing::advance_time`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use chrono::{DateTime, TimeDelta, Utc};

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

    pub fn now(&self) -> DateTime<Utc> {
        let now = Utc::now();
        match &self.0 {
            Some(moved) => {
                now + TimeDelta::microseconds(moved.ahead_micros.load(Ordering::Relaxed))
            }
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
    pub fn advance_to(&self, when: DateTime<Utc>) {
        let ahead = (when - Utc::now()).num_microseconds().unwrap_or(i64::MAX);
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
