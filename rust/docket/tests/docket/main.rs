//! docket-rs against a real Redis, or the in-process one.
//!
//! The tests run against `DOCKET_TEST_URL`, or `memory://` when it is not
//! set.  Each test gets a docket of its own, so they run in parallel.

mod support;

mod admission;
mod automatic;
mod behavior_faults;
mod cancellation;
mod concurrency;
mod context;
mod credentials;
mod cron;
mod events;
mod faults;
mod logs;
mod perpetual;
mod results;
mod retries;
mod scheduling;
mod snapshots;
mod strikes;
mod sweeping;
mod testing_helpers;
mod timeouts;
mod unreachable;
mod worker_faults;
mod workers;
