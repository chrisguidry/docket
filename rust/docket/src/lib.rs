//! docket-rs runs functions on other machines, now or later, with Redis
//! keeping the queue.
//!
//! It is the Rust implementation of [docket](https://github.com/chrisguidry/docket).

mod task;

pub use task::Task;
