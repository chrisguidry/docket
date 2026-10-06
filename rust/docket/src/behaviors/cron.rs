//! Tasks on a cron schedule.

use std::marker::PhantomData;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;

use super::perpetual::{Automatic, Manual, PerpetualControl};
use super::{AfterCompletion, Behavior, Completion, Hooks, Outcome};
use crate::context::Context;
use crate::error::{Error, Result};
use crate::task::Task;

/// Runs a task on a cron schedule, in a time zone.  A cron task is
/// automatic, like an automatic [`Perpetual`](super::Perpetual): every worker
/// adds it when it starts, so its arguments need a `Default`.
/// [`Cron::manual`] runs it only after something adds it.
///
/// Expressions have five fields, or one of `@yearly`, `@annually`,
/// `@monthly`, `@weekly`, `@daily`, `@midnight`, and `@hourly`.
#[derive(Clone, Debug)]
pub struct Cron<Mode = Automatic> {
    schedule: croner::Cron,
    timezone: Tz,
    mode: PhantomData<Mode>,
}

impl Cron {
    /// Parses a cron expression, scheduled in UTC.
    pub fn new(expression: &str) -> Result<Self> {
        let expression = if expression.trim().eq_ignore_ascii_case("@midnight") {
            "@daily"
        } else {
            expression
        };
        let schedule = croner::Cron::from_str(expression).map_err(|error| {
            Error::Invalid(format!("{expression} is not a cron expression: {error}"))
        })?;
        Ok(Self {
            schedule,
            timezone: Tz::UTC,
            mode: PhantomData,
        })
    }

    /// Runs the task only after something adds it.
    #[must_use]
    pub fn manual(self) -> Cron<Manual> {
        Cron {
            schedule: self.schedule,
            timezone: self.timezone,
            mode: PhantomData,
        }
    }
}

impl<Mode> Cron<Mode> {
    /// Reads the schedule in `timezone`, so that it follows daylight saving
    /// time there.
    #[must_use]
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.timezone = timezone;
        self
    }

    /// The first time after `after` that the schedule matches.
    fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule
            .find_next_occurrence(&after.with_timezone(&self.timezone), false)
            .ok()
            .map(|next| next.with_timezone(&Utc))
    }

    fn attach_cron<T: Task>(&self, hooks: &mut Hooks<'_, T>) {
        hooks.context(PerpetualControl::new);
        hooks.completion(Schedule {
            cron: Cron {
                schedule: self.schedule.clone(),
                timezone: self.timezone,
                mode: PhantomData::<Manual>,
            },
        });
    }
}

impl<T: Task + Default> Behavior<T> for Cron<Automatic> {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        self.attach_cron(hooks);
        hooks.automatic(move || self.next_after(Utc::now()));
    }
}

impl<T: Task> Behavior<T> for Cron<Manual> {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        self.attach_cron(hooks);
    }
}

struct Schedule {
    cron: Cron<Manual>,
}

impl Completion for Schedule {
    async fn on_complete(&self, ctx: &Context, _outcome: &Outcome) -> AfterCompletion {
        let control = ctx
            .perpetual()
            .expect("a cron task gives each run its control");
        match self.cron.next_after(Utc::now()) {
            Some(next) => control.decide(next),
            None => AfterCompletion::Finish,
        }
    }
}

#[cfg(test)]
mod tests;
