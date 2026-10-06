//! Running a task once after its calls settle.

use std::time::Duration;

use chrono::Utc;

use super::subject::subject;
use super::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks};
use crate::context::Context;
use crate::scripts;
use crate::task::Task;
use crate::wire::millis;

/// Runs a task once its calls stop coming for `settle`.  The first call
/// waits; later calls inside the window are dropped, and each one restarts
/// the wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Debounce {
    settle: Duration,
    field: Option<String>,
    scope: Option<String>,
}

impl Debounce {
    /// Debounces the whole task.
    #[must_use]
    pub fn new(settle: Duration) -> Self {
        Self {
            settle,
            field: None,
            scope: None,
        }
    }

    /// Debounces each value of `field` on its own.
    pub fn per_field(field: impl Into<String>, settle: Duration) -> Self {
        Self {
            field: Some(field.into()),
            ..Self::new(settle)
        }
    }

    /// Keeps the debounce's keys under `scope` instead of the docket's name.
    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }
}

impl<T: Task> Behavior<T> for Debounce {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

const PROCEED: i64 = 1;
const RESCHEDULE: i64 = 2;

impl Admission for Debounce {
    async fn admit(&self, ctx: &Context) -> Result<Admitted, AdmissionBlocked> {
        let base = self.scope.as_deref().unwrap_or(ctx.docket().name());
        let tag = subject(ctx, self.field.as_deref())?;
        // The braces make the two keys one cluster slot, so that the script
        // can touch both.
        let prefix = format!("{base}:debounce:{tag}:{{{tag}}}");
        let settle_ms = i64::try_from(self.settle.as_millis()).unwrap_or(i64::MAX);
        let call = scripts::Debounce {
            winner_key: format!("{prefix}:winner"),
            seen_key: format!("{prefix}:last_seen"),
            execution_key: ctx.key().to_owned(),
            settle_ms,
            now_ms: millis(Utc::now()),
            ttl_ms: settle_ms.saturating_mul(10),
        }
        .call();
        match super::run_script::<(i64, i64)>(ctx.docket(), &call).await {
            Ok((PROCEED, _)) => Ok(Admitted::now()),
            Ok((RESCHEDULE, remaining)) => {
                Err(AdmissionBlocked::new("waiting for calls to settle")
                    .retry_delay(Duration::from_millis(remaining.try_into().unwrap_or(0))))
            }
            Ok(_) => Err(AdmissionBlocked::new("a newer call is settling").drop_task()),
            Err(error) => Err(AdmissionBlocked::new(format!("debouncing failed: {error}"))),
        }
    }
}
