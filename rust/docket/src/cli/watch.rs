//! `watch`: follows one run's state and progress until it ends.
//!
//! pydocket redraws a live panel with rich.  This prints a line for each
//! change instead, which reads the same through `kubectl exec` with no
//! terminal.

use std::fmt::Write as _;
use std::io::Write;

use futures::StreamExt;

use super::commands::emit;
use super::display::repr_str;
use crate::docket::Docket;
use crate::error::{Error, Result};
use crate::execution::{Event, State};

pub(super) async fn watch(docket: &Docket, key: &str, out: &mut (dyn Write + Send)) -> Result<()> {
    let Some(execution) = docket.execution(key).await? else {
        return Err(Error::Invalid(format!(
            "Task with key {} not found",
            repr_str(key)
        )));
    };
    let mut display = Display::new(key, docket.name());
    // A subscription starts with the state and progress as they are, so
    // when Redis drops one, a new one misses nothing, and the display skips
    // what it already printed.
    loop {
        let mut events = execution.subscribe().await?;
        while let Some(event) = events.next().await {
            let ended = display.update(&event?);
            emit(out, &std::mem::take(&mut display.pending))?;
            if ended {
                return Ok(());
            }
        }
    }
}

/// What `watch` has printed, and what it has yet to print.
struct Display {
    pending: String,
    state: Option<(State, Option<String>)>,
    worker: Option<String>,
    progress: Option<(i64, i64, Option<String>)>,
}

impl Display {
    fn new(key: &str, docket: &str) -> Self {
        Self {
            pending: format!("Task: {key}\nDocket: {docket}\n"),
            state: None,
            worker: None,
            progress: None,
        }
    }

    /// Adds the lines for what changed, and returns whether the run ended.
    fn update(&mut self, event: &Event) -> bool {
        match event {
            Event::State(event) => {
                let state = Some((event.state, event.error.clone()));
                if self.state != state {
                    let _ = writeln!(
                        self.pending,
                        "State: {}",
                        event.state.as_str().to_uppercase()
                    );
                    self.state = state;
                }
                if event.worker.is_some() && event.worker != self.worker {
                    let worker = event.worker.clone().unwrap_or_default();
                    let _ = writeln!(self.pending, "Worker: {worker}");
                    self.worker.clone_from(&event.worker);
                }
                let ending = match event.state {
                    State::Completed => "✓ Task completed successfully",
                    State::Failed => "✗ Task failed",
                    State::Cancelled => "✗ Task cancelled",
                    _ => return false,
                };
                if let Some(error) = &event.error {
                    let _ = writeln!(self.pending, "Error: {error}");
                }
                let _ = writeln!(self.pending, "{ending}");
                true
            }
            Event::Progress(event) => {
                let Some(current) = event.current.filter(|_| event.total > 0) else {
                    return false;
                };
                let progress = Some((current, event.total, event.message.clone()));
                if self.progress != progress {
                    let message = event.message.as_deref().unwrap_or("Processing...");
                    let percent = current.saturating_mul(100) / event.total;
                    let _ = writeln!(
                        self.pending,
                        "Progress: {current}/{} ({percent}%) {message}",
                        event.total
                    );
                    self.progress = progress;
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
