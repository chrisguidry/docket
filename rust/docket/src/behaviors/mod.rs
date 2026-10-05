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
    Completion, Failure, Hooks, Outcome, Released, Runtime, TaskFuture,
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

/// A docket error as the Redis error that admission hooks report.
fn redis_error(error: crate::Error) -> redis::RedisError {
    match error {
        crate::Error::Redis(error) => error,
        other => redis::RedisError::from((redis::ErrorKind::Client, "docket", other.to_string())),
    }
}
