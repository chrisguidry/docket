//! Dropping calls that come too soon after the last run.

use std::time::Duration;

use super::subject::subject;
use super::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks};
use crate::context::Context;
use crate::task::Task;

/// Lets a task run at most once per `window`; calls inside the window are
/// dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cooldown {
    window: Duration,
    field: Option<String>,
    scope: Option<String>,
}

impl Cooldown {
    /// Cools down the whole task.
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            field: None,
            scope: None,
        }
    }

    /// Cools down each value of `field` on its own.
    pub fn per_field(field: impl Into<String>, window: Duration) -> Self {
        Self {
            field: Some(field.into()),
            ..Self::new(window)
        }
    }

    /// Keeps the cooldown's key under `scope` instead of the docket's name.
    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }
}

impl<T: Task> Behavior<T> for Cooldown {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.admission(self);
    }
}

impl Admission for Cooldown {
    async fn admit(&self, ctx: &Context) -> Result<Admitted, AdmissionBlocked> {
        let base = self.scope.as_deref().unwrap_or(ctx.docket().name());
        let key = format!("{base}:cooldown:{}", subject(ctx, self.field.as_deref())?);
        let window = u64::try_from(self.window.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let set: crate::Result<Option<String>> = async {
            let mut connection = ctx.docket().connection().await?;
            redis::cmd("SET")
                .arg(&key)
                .arg(1)
                .arg("NX")
                .arg("PX")
                .arg(window)
                .query_async(&mut connection)
                .await
                .map_err(Into::into)
        }
        .await;
        match set {
            Ok(Some(_)) => Ok(Admitted::now()),
            Ok(None) => Err(AdmissionBlocked::new("the task is cooling down").drop_task()),
            Err(error) => Err(AdmissionBlocked::new(format!(
                "checking the cooldown failed: {error}"
            ))),
        }
    }
}
