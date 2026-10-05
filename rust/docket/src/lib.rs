//! docket-rs runs functions on other machines, now or later, with Redis
//! keeping the queue.
//!
//! It is the Rust implementation of [docket](https://github.com/chrisguidry/docket).

#[cfg(feature = "memory")]
mod memory;
mod scripts;
mod task;

pub use task::Task;
