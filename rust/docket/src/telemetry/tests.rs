//! Helpers for tests that read the events docket logs.

use std::fmt::{Debug, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Metadata, Subscriber};

/// Every event logged on this thread while its guard lives, as one line of
/// `name=value` fields each.  Logging must be on for the fields of a
/// warning to be evaluated at all.
#[derive(Clone, Default)]
pub(crate) struct Logs(Arc<Mutex<Vec<String>>>);

impl Logs {
    pub(crate) fn capture() -> (Self, DefaultGuard) {
        let logs = Self::default();
        let guard = tracing::subscriber::set_default(logs.clone());
        (logs, guard)
    }

    pub(crate) fn contains(&self, text: &str) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains(text))
    }

    /// Waits until a line contains `text`.
    pub(crate) async fn wait_for(&self, text: &str) {
        let waiting = async {
            while !self.contains(text) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("the line is logged in time");
    }
}

#[derive(Default)]
struct Line(String);

impl Visit for Line {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}

impl Subscriber for Logs {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut line = Line::default();
        event.record(&mut line);
        self.0.lock().unwrap().push(line.0);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}
