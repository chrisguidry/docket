//! docket-rs runs functions on other machines, now or later, with Redis
//! keeping the queue.
//!
//! It is the Rust implementation of [docket](https://github.com/chrisguidry/docket).

pub mod behaviors;
mod connection;
mod context;
mod docket;
mod error;
mod execution;
mod keys;
#[cfg(feature = "memory")]
mod memory;
mod scripts;
mod strikes;
mod task;
mod wire;
mod worker;

pub use behaviors::{
    ConcurrencyLimit, Cooldown, Cron, Debounce, ExponentialRetry, ForcedRetry, Perpetual,
    RateLimit, Retry, Timeout,
};
pub use context::Context;
pub use docket::{
    Add, Call, Docket, DocketBuilder, Registration, Snapshot, TaskSummary, WorkerSummary,
};
pub use error::{Error, Result};
pub use execution::{
    Disposition, Event, Events, Execution, Progress, ProgressEvent, ProgressSnapshot, State,
    StateEvent, Status,
};
pub use strikes::{Operator, Strike, StrikeField};
pub use task::Task;
pub use worker::Worker;
