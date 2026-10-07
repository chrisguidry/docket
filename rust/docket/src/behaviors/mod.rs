//! Behaviors change how a task runs: retries, timeouts, perpetual and cron
//! schedules, and admission controls.  Users write their own against the same
//! hooks.

mod concurrency;
mod cooldown;
mod cron;
mod debounce;
mod hooks;
mod perpetual;
mod ratelimit;
mod retry;
mod subject;
mod timeout;

pub(crate) use concurrency::{SAFEGUARD_PREFIX, SafeguardWake, safeguard_wake};
pub use hooks::{
    Admission, AdmissionBlocked, Admitted, AfterCompletion, AfterFailure, Behavior, BoxError,
    Completion, Failure, Hooks, NotAdmitted, Outcome, Released, Runtime, TaskFuture,
};
pub(crate) use hooks::{Automatic, ErasedHooks, Release};

pub use concurrency::ConcurrencyLimit;
pub use cooldown::Cooldown;
pub use cron::Cron;
pub use debounce::Debounce;
pub use perpetual::{Automatic as AutomaticMode, Manual, Perpetual, PerpetualControl};
pub use ratelimit::RateLimit;
pub use retry::{ExponentialRetry, ForcedRetry, Retry};
pub use timeout::{TimedOut, Timeout, TimeoutControl};

/// Runs one of docket's scripts on the docket's connection.
async fn run_script<T: redis::FromRedisValue>(
    docket: &crate::Docket,
    call: &crate::scripts::Call,
) -> crate::Result<T> {
    let mut connection = docket.connection().await?;
    call.run(&mut connection).await.map_err(Into::into)
}

#[cfg(all(test, feature = "memory"))]
mod tests;
