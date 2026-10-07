//! The limits that [`Worker::run_at_most`](crate::Worker::run_at_most) puts on
//! a docket's keys while it runs.  pydocket keeps them as a condition in its
//! strike list, so a key that has used up its runs is struck everywhere in
//! the process: a worker refuses its deliveries, and scheduling refuses its
//! next run, such as a perpetual task's.

use std::collections::HashMap;

use super::Docket;

/// How many runs a key may have, and how many it has had.
#[derive(Debug, Default)]
pub(crate) struct RunLimit {
    allowed: u32,
    ran: u32,
}

/// Takes a docket's limits off again when it is dropped, even when the run
/// that set them is cancelled.
pub(crate) struct Limited {
    docket: Docket,
    keys: Vec<String>,
}

impl Drop for Limited {
    fn drop(&mut self) {
        let mut limits = self.docket.run_limits();
        for key in &self.keys {
            limits.remove(key);
        }
    }
}

impl Docket {
    /// Allows each key in `limits` that many runs, until the guard drops.
    pub(crate) fn limit_runs(&self, limits: &HashMap<String, u32>) -> Limited {
        let mut current = self.run_limits();
        for (key, &allowed) in limits {
            current.insert(key.clone(), RunLimit { allowed, ran: 0 });
        }
        Limited {
            docket: self.clone(),
            keys: limits.keys().cloned().collect(),
        }
    }

    /// Counts a run that a worker claimed.
    pub(crate) fn count_run(&self, key: &str) {
        if let Some(limit) = self.run_limits().get_mut(key) {
            limit.ran += 1;
        }
    }

    /// Whether `key` has had every run its limit allows.
    pub(crate) fn has_run_out(&self, key: &str) -> bool {
        self.run_limits()
            .get(key)
            .is_some_and(|limit| limit.ran >= limit.allowed)
    }

    fn run_limits(&self) -> std::sync::MutexGuard<'_, HashMap<String, RunLimit>> {
        self.inner
            .run_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
