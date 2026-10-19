//! The in-process Redis engine behind `memory://`.
//!
//! These files are copied from burner-redis
//! (<https://github.com/prefectlabs/burner-redis>) at e506d0d, version 0.1.7
//! plus one commit, with its Python bindings and persistence left out.  They
//! also carry the fixes from burner-redis PRs #9 to #13.  The MIT license in
//! `LICENSE` here covers them.  pydocket's `memory://` runs the same engine
//! from PyPI, so the two languages differ until a burner-redis release has
//! those fixes.  Fix bugs upstream first, then copy the files again.

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
