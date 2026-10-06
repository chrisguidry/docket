//! `memory://`: a docket backed by an in-process Redis, for tests.

mod commands;
// Kept as upstream formats it, so that a fresh copy is a clean diff.
#[rustfmt::skip]
mod engine;
mod pubsub;
mod resp;
mod server;
mod session;

#[cfg(test)]
mod tests;

pub(crate) use server::MemoryServer;
