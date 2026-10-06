//! The in-process Redis engine behind `memory://`.
//!
//! These files are copied from burner-redis
//! (<https://github.com/prefectlabs/burner-redis>) at e506d0d, version 0.1.7
//! plus one commit, with its Python bindings and persistence left out.  The
//! MIT license in `LICENSE` here covers them.  pydocket's `memory://` runs
//! the same engine, so both languages see the same in-memory behavior.  Fix
//! bugs upstream first, then copy the files again.

#![allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    dead_code,
    unused_imports,
    unused_must_use
)]

pub mod lists;
pub mod pubsub;
pub mod scripting;
pub mod store;
pub mod streams;
