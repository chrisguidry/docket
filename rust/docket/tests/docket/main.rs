//! docket-rs against a real Redis, or the in-process one.
//!
//! The tests run against `DOCKET_TEST_URL`, or `memory://` when it is not
//! set.  Each test gets a docket of its own, so they run in parallel.

mod support;

mod admission;
mod faults;
mod perpetual;
mod results;
mod retries;
mod scheduling;
mod strikes;
mod timeouts;
mod workers;
