//! Running a failed task again.

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::{AfterFailure, Behavior, BoxError, Failure, Hooks};
use crate::context::Context;
use crate::task::Task;

/// Retries a failed task after a fixed delay.  `attempts` counts every run,
/// the first included, so `Retry::attempts(1)` never retries.
///
/// A handler can ask for a retry at a time it chooses by returning
/// [`ForcedRetry`]; that spends an attempt like any failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retry {
    attempts: Option<u32>,
    delay: Duration,
}

impl Retry {
    /// Up to `attempts` runs in all, with no delay between them.
    #[must_use]
    pub fn attempts(attempts: u32) -> Self {
        Self {
            attempts: Some(attempts),
            delay: Duration::ZERO,
        }
    }

    /// Retries for as long as the task fails.
    #[must_use]
    pub fn forever() -> Self {
        Self {
            attempts: None,
            delay: Duration::ZERO,
        }
    }

    /// Waits `delay` before each retry.
    #[must_use]
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

impl<T: Task> Behavior<T> for Retry {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.failure(self);
    }
}

impl Failure for Retry {
    async fn on_failure(&self, ctx: &Context, error: &BoxError) -> AfterFailure {
        decide(
            self.attempts,
            ctx.attempt(),
            error,
            self.delay,
            ctx.docket().now(),
        )
    }
}

/// Retries a failed task with a delay that doubles each time:
/// `minimum_delay`, then twice that, up to `maximum_delay`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExponentialRetry {
    attempts: Option<u32>,
    minimum_delay: Duration,
    maximum_delay: Duration,
}

impl ExponentialRetry {
    /// Up to `attempts` runs in all, waiting 1 s, then 2 s, and so on up to
    /// 64 s between them.
    #[must_use]
    pub fn attempts(attempts: u32) -> Self {
        Self {
            attempts: Some(attempts),
            minimum_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(64),
        }
    }

    /// Retries for as long as the task fails.
    #[must_use]
    pub fn forever() -> Self {
        Self {
            attempts: None,
            ..Self::attempts(1)
        }
    }

    /// The delay before the first retry.
    #[must_use]
    pub fn minimum_delay(mut self, delay: Duration) -> Self {
        self.minimum_delay = delay;
        self
    }

    /// The longest delay between two attempts.
    #[must_use]
    pub fn maximum_delay(mut self, delay: Duration) -> Self {
        self.maximum_delay = delay;
        self
    }

    /// The delay after attempt `attempt` fails.
    fn delay_after(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1);
        2u32.checked_pow(exponent)
            .and_then(|factor| self.minimum_delay.checked_mul(factor))
            .map_or(self.maximum_delay, |delay| delay.min(self.maximum_delay))
    }
}

impl<T: Task> Behavior<T> for ExponentialRetry {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.failure(self);
    }
}

impl Failure for ExponentialRetry {
    async fn on_failure(&self, ctx: &Context, error: &BoxError) -> AfterFailure {
        decide(
            self.attempts,
            ctx.attempt(),
            error,
            self.delay_after(ctx.attempt()),
            ctx.docket().now(),
        )
    }
}

fn decide(
    attempts: Option<u32>,
    attempt: u32,
    error: &BoxError,
    delay: Duration,
    now: DateTime<Utc>,
) -> AfterFailure {
    if attempts.is_some_and(|attempts| attempt >= attempts) {
        return AfterFailure::Fail;
    }
    let delay = error
        .downcast_ref::<ForcedRetry>()
        .map_or(delay, |forced| forced.delay);
    AfterFailure::RetryAt(now + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX))
}

/// An error that asks for a retry after a delay the handler chooses.
///
/// ```
/// # use docket::behaviors::ForcedRetry;
/// # use std::time::Duration;
/// async fn charge() -> Result<(), ForcedRetry> {
///     Err(ForcedRetry::after(Duration::from_secs(5 * 60)))
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the task asked to run again in {delay:?}")]
pub struct ForcedRetry {
    delay: Duration,
}

impl ForcedRetry {
    /// Retries after `delay`.
    #[must_use]
    pub fn after(delay: Duration) -> Self {
        Self { delay }
    }

    /// Retries at `when`, or now when `when` has passed.
    #[must_use]
    pub fn at(when: DateTime<Utc>) -> Self {
        Self {
            delay: (when - Utc::now()).to_std().unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests;
