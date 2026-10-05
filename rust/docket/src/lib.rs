#![doc = include_str!("../README.md")]

mod agenda;
pub mod behaviors;
#[cfg(feature = "cli")]
pub mod cli;
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
pub mod testing;
mod wire;
mod worker;

pub use agenda::Agenda;
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
/// The types of a credentials provider, from redis-rs; see
/// [`DocketBuilder::credentials_provider`].
pub use redis::{BasicAuth, StreamingCredentialsProvider};
pub use strikes::{Operator, Strike, StrikeField};
pub use task::Task;
pub use worker::Worker;
