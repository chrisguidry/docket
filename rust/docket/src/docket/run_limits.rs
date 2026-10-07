//! The limits that [`Worker::run_at_most`](crate::Worker::run_at_most) puts on
//! a docket's keys while it runs.  pydocket keeps each call's limit as its
//! own condition in the docket's strike list, so a key that one call has run
//! enough times is struck everywhere in the process: a worker refuses its
//! deliveries, and scheduling refuses its next run, such as a perpetual
//! task's.  Each call counts only its own worker's runs.

use std::collections::HashMap;

use super::Docket;

/// How many runs a key may have under one call, and how many it has had.
#[derive(Debug)]
struct RunLimit {
    allowed: u32,
    ran: u32,
}

/// The limits of every `run_at_most` call on a docket, by call.
#[derive(Debug, Default)]
pub(crate) struct RunLimits {
    next: u64,
    calls: HashMap<u64, HashMap<String, RunLimit>>,
}

/// One call's limits, which it counts its runs against.  They come off the
/// docket when this is dropped, even when the call is cancelled.
pub(crate) struct Limited {
    docket: Docket,
    call: u64,
}

impl Limited {
    /// Counts a run of `key` that this call's worker claimed.
    pub(crate) fn count_run(&self, key: &str) {
        let mut limits = self.docket.run_limits();
        if let Some(limit) = limits
            .calls
            .get_mut(&self.call)
            .and_then(|call| call.get_mut(key))
        {
            limit.ran += 1;
        }
    }
}

impl Drop for Limited {
    fn drop(&mut self) {
        self.docket.run_limits().calls.remove(&self.call);
    }
}

impl Docket {
    /// Allows each key in `limits` that many runs, until the guard drops.
    pub(crate) fn limit_runs(&self, limits: &HashMap<String, u32>) -> Limited {
        let mut current = self.run_limits();
        let call = current.next;
        current.next += 1;
        current.calls.insert(
            call,
            limits
                .iter()
                .map(|(key, &allowed)| (key.clone(), RunLimit { allowed, ran: 0 }))
                .collect(),
        );
        Limited {
            docket: self.clone(),
            call,
        }
    }

    /// Whether any call has run `key` as many times as it allows.
    pub(crate) fn has_run_out(&self, key: &str) -> bool {
        self.run_limits()
            .calls
            .values()
            .filter_map(|call| call.get(key))
            .any(|limit| limit.ran >= limit.allowed)
    }

    fn run_limits(&self) -> std::sync::MutexGuard<'_, RunLimits> {
        self.inner
            .run_limits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
