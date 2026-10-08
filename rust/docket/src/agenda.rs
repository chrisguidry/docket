//! Scheduling a set of tasks spread over a period, so that a large batch
//! does not land on the workers all at once.

use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::RngExt;

use crate::docket::{Call, Docket};
use crate::error::{Error, Result};
use crate::execution::Execution;
use crate::task::Task;

/// A set of tasks to schedule together, spread evenly over a period.
///
/// ```no_run
/// # use docket::{Agenda, Docket, Task};
/// # use std::time::Duration;
/// # #[derive(serde::Serialize, serde::Deserialize, Task)]
/// # #[task(name = "process")]
/// # struct Process { item: u64 }
/// # async fn example(docket: Docket) -> docket::Result<()> {
/// let mut agenda = Agenda::new();
/// for item in 0..100 {
///     agenda.add_keyed(&Process { item }, format!("item-{item}"));
/// }
/// agenda.scatter(&docket, Duration::from_secs(50 * 60)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Agenda {
    calls: Vec<Call>,
}

impl Agenda {
    /// An empty agenda.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a task, which gets a new key when it is scattered.
    pub fn add<T: Task>(&mut self, args: &T) -> &mut Self {
        self.calls.push(Call::new(args));
        self
    }

    /// Adds a task under a key.  A stable key makes scattering the agenda
    /// again keep the first schedule instead of adding the task twice.
    pub fn add_keyed<T: Task>(&mut self, args: &T, key: impl Into<String>) -> &mut Self {
        self.calls.push(Call::new(args).key(key));
        self
    }

    /// How many tasks the agenda holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.calls.len()
    }

    /// Whether the agenda holds no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// Removes every task.
    pub fn clear(&mut self) {
        self.calls.clear();
    }

    /// Schedules the tasks spread evenly over `over`, starting now.  The
    /// first task is due at the start and the last at the end; a single task
    /// is due halfway.
    pub async fn scatter(&self, docket: &Docket, over: Duration) -> Result<Vec<Execution>> {
        self.scatter_from(docket, docket.now(), over, Duration::ZERO)
            .await
    }

    /// Like [`Agenda::scatter`], from `start`, with each time moved by a
    /// random offset of up to `jitter` either way, but never before `start`.
    pub async fn scatter_from(
        &self,
        docket: &Docket,
        start: DateTime<Utc>,
        over: Duration,
        jitter: Duration,
    ) -> Result<Vec<Execution>> {
        if over.is_zero() {
            return Err(Error::Invalid(
                "an agenda scatters over a period longer than zero".into(),
            ));
        }
        let calls = self
            .calls
            .iter()
            .zip(times(self.calls.len(), start, over, jitter))
            .map(|(call, when)| call.clone().at(when));
        docket.add_many(calls).await
    }
}

fn times(
    count: usize,
    start: DateTime<Utc>,
    over: Duration,
    jitter: Duration,
) -> Vec<DateTime<Utc>> {
    let over = chrono::Duration::from_std(over).unwrap_or(chrono::Duration::MAX);
    let jitter = i64::try_from(jitter.as_micros()).unwrap_or(i64::MAX);
    let offsets: Vec<chrono::Duration> = if count == 1 {
        vec![over / 2]
    } else {
        let steps = i32::try_from(count.saturating_sub(1)).unwrap_or(i32::MAX);
        (0..steps.saturating_add(1))
            .map(|step| over * step / steps)
            .collect()
    };
    offsets
        .into_iter()
        .map(|offset| {
            let shift = chrono::Duration::microseconds(rand::rng().random_range(-jitter..=jitter));
            (start + offset + shift).max(start)
        })
        .collect()
}

#[cfg(all(test, feature = "memory"))]
mod tests;
