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
/// Expressions have five fields, or six with seconds first, or are one of
/// `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, and
/// `@hourly`.  They mean what they mean in pydocket, which refuses a field
/// for the year, `?`, a `W` other than `LW`, and an expression that never
/// matches, so this does too.
///
/// Each run's next match counts from when the run started, so a run still
/// going at its next match is followed by that match's run at once.
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
        if let Some(reason) = refused_by_pydocket(expression, &schedule) {
            return Err(Error::Invalid(format!(
                "{expression} is not a cron expression: {reason}"
            )));
        }
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

/// Why pydocket's parser refuses an expression that croner accepts, if it
/// does.  The day of the month is the third field from the end, both with
/// and without a field for the seconds.
fn refused_by_pydocket(expression: &str, schedule: &croner::Cron) -> Option<&'static str> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    let day_of_month = fields.len().checked_sub(3).map(|index| fields[index]);
    if fields.len() == 7 {
        Some("it has a field for the year")
    } else if expression.contains('?') {
        Some("it uses ?")
    } else if day_of_month.is_some_and(|day| day.contains('W') && day != "LW") {
        Some("its day of the month uses W, other than LW")
    } else if schedule.find_next_occurrence(&Utc::now(), false).is_err() {
        Some("it never matches")
    } else {
        None
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
        hooks.context_at(PerpetualControl::new);
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
        hooks.automatic_at(move |now| self.next_after(now));
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
        self.cron
            .next_after(control.started_at)
            .map_or(AfterCompletion::Finish, |next| control.decide(next))
    }
}

#[cfg(test)]
mod tests;
