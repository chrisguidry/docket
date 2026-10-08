//! Capping how many times a task runs in a period.

use std::time::Duration;

use redis::AsyncCommands;

use super::subject::subject;
use super::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks, NotAdmitted, Released};
use crate::context::Context;
use crate::scripts;
use crate::task::Task;
use crate::wire::millis;

/// Lets a task run at most `limit` times in any sliding window of `per`.  A
/// call over the limit waits for room, or is dropped with
/// [`RateLimit::drop_excess`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RateLimit {
    limit: u32,
    per: Duration,
    drop: bool,
    field: Option<String>,
    scope: Option<String>,
}

impl RateLimit {
    /// At most `limit` runs per minute, until [`RateLimit::per`] says
    /// otherwise.
    #[must_use]
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            per: Duration::from_secs(60),
            drop: false,
            field: None,
            scope: None,
        }
    }

    /// Limits each value of `field` on its own.
    pub fn per_field(field: impl Into<String>, limit: u32) -> Self {
        Self {
            field: Some(field.into()),
            ..Self::new(limit)
        }
    }

    /// The length of the window.
    #[must_use]
    pub fn per(mut self, per: Duration) -> Self {
        self.per = per;
        self
    }

    /// Drops calls over the limit instead of waiting for room.
    #[must_use]
    pub fn drop_excess(mut self) -> Self {
        self.drop = true;
        self
    }

    /// Keeps the limit's key under `scope` instead of the docket's name.
    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }
}

impl<T: Task> Behavior<T> for RateLimit {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

const ADMITTED: i64 = 1;

impl Admission for RateLimit {
    async fn admit(&self, ctx: &Context) -> Result<Admitted, NotAdmitted> {
        let base = self.scope.as_deref().unwrap_or(ctx.docket().name());
        let key = format!("{base}:ratelimit:{}", subject(ctx, self.field.as_deref())?);
        let now_ms = millis(ctx.docket().now());
        let window_ms = i64::try_from(self.per.as_millis()).unwrap_or(i64::MAX);
        let member = format!("{}:{now_ms}", ctx.key());
        let call = scripts::RateLimit {
            ratelimit_key: key.clone(),
            member: member.clone(),
            now_ms,
            window_ms,
            limit: i64::from(self.limit),
            ttl_ms: window_ms.saturating_mul(2),
        }
        .call();
        let docket = ctx.docket().clone();
        match super::run_script::<(i64, i64)>(&docket, &call).await {
            // A run that a later admission hook blocks gives its place back.
            Ok((ADMITTED, _)) => Ok(Admitted::with_release(move |released| async move {
                if released == Released::Blocked
                    && let Ok(mut connection) = docket.connection().await
                {
                    let _: redis::RedisResult<i64> = connection.zrem(&key, &member).await;
                }
            })),
            Ok(_) if self.drop => Err(AdmissionBlocked::new("the rate limit is reached")
                .drop_task()
                .into()),
            Ok((_, retry_after)) => Err(AdmissionBlocked::new("the rate limit is reached")
                .retry_delay(Duration::from_millis(retry_after.try_into().unwrap_or(1)))
                .into()),
            Err(error) => Err(NotAdmitted::failed(format!(
                "checking the rate limit failed: {error}"
            ))),
        }
    }
}
