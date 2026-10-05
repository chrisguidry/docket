//! What docket logs while it works.  `Logs` captures the events of the test's
//! own thread, which on the test's single-threaded runtime includes every
//! task the test spawns.

use std::fmt::{Debug, Write};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::DefaultGuard;
use tracing::{Dispatch, Event, Metadata, Subscriber};

use crate::support::{Echo, Noop, docket, within, worker};

/// Every event logged on this thread while its guard lives, as one line of
/// text each.
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<String>>>);

impl Logs {
    pub fn capture() -> (Self, DefaultGuard) {
        // With one subscriber registered, tracing decides whether an event is
        // of interest from the default of whichever thread logs it first, and
        // another test's thread has none.  A second subscriber that lives for
        // the whole run keeps tracing asking every registered one.
        static SECOND: LazyLock<Dispatch> = LazyLock::new(|| Dispatch::new(Logs::default()));
        LazyLock::force(&SECOND);
        let logs = Self::default();
        let guard = tracing::subscriber::set_default(logs.clone());
        // Events that another thread found of no interest before this capture
        // began are judged again.
        tracing::callsite::rebuild_interest_cache();
        (logs, guard)
    }

    pub fn contains(&self, text: &str) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains(text))
    }

    /// Waits until a line contains `text`.
    pub async fn wait_for(&self, text: &str) {
        within(20, async {
            while !self.contains(text) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }
}

/// One event's fields, written as `name=value`.
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

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut line = Line::default();
        span.record(&mut line);
        self.0.lock().unwrap().push(format!("span{}", line.0));
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

#[tokio::test]
async fn a_task_logs_under_its_name_key_and_attempt() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("hi")).key("greeting").await.unwrap();

    within(10, worker(&docket).name("logger").run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains("task=echo key=greeting attempt=1 worker=logger"));
    assert!(logs.contains("message=task finished"));
    assert_eq!(execution.result().await.unwrap(), "hi");
}

#[tokio::test]
async fn a_task_with_no_handler_logs_a_warning() {
    let (logs, _guard) = Logs::capture();
    let docket = docket().await;
    docket.add(Noop).key("orphan").await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert!(logs.contains("no handler is registered for this task"));
    assert!(logs.contains("function=noop key=orphan"));
}
