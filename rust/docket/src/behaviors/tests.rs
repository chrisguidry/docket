use std::fmt::{Debug, Write};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rstest::rstest;
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Metadata, Subscriber};

use super::{
    AdmissionBlocked, Admitted, Behavior, ConcurrencyLimit, Cooldown, Debounce, ErasedHooks, Hooks,
    RateLimit,
};
use crate::context::Context;
use crate::docket::Docket;

#[derive(Serialize, Deserialize, docket_rs_macros::Task)]
#[task(name = "noop", crate = crate)]
pub(super) struct Noop;

/// A docket whose Redis refuses every connection.
pub(super) async fn unreachable() -> Docket {
    Docket::connect("unreachable", "redis://127.0.0.1:1/0")
        .await
        .unwrap()
}

/// Every event logged on this thread while its guard lives, as one line of
/// `name=value` fields each.  Logging must be on for the fields of a
/// warning to be evaluated at all.
#[derive(Clone, Default)]
pub(super) struct Logs(Arc<Mutex<Vec<String>>>);

impl Logs {
    pub(super) fn capture() -> (Self, DefaultGuard) {
        let logs = Self::default();
        let guard = tracing::subscriber::set_default(logs.clone());
        (logs, guard)
    }

    pub(super) fn contains(&self, text: &str) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains(text))
    }

    /// Waits until a line contains `text`.
    pub(super) async fn wait_for(&self, text: &str) {
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

/// A docket on an in-process Redis of its own.
pub(super) async fn memory() -> Docket {
    Docket::connect("memory", format!("memory://{}", uuid::Uuid::now_v7()))
        .await
        .unwrap()
}

/// The context of a first delivery of the noop task under `key`.  The short
/// redelivery timeout makes a concurrency slot renew every 10 ms.
pub(super) fn context(docket: &Docket, key: &str) -> Context {
    Context::for_tests(docket, key, "noop")
}

pub(super) type Attach = fn(&mut Hooks<'_, Noop>);

/// Runs the one admission hook that `attach` gives a task.
pub(super) async fn admit(attach: Attach, ctx: Context) -> Result<Admitted, AdmissionBlocked> {
    let mut erased = ErasedHooks::default();
    attach(&mut Hooks {
        erased: &mut erased,
        task: PhantomData,
    });
    erased.admissions[0](ctx).await
}

#[rstest]
#[case::concurrency(|hooks: &mut Hooks<'_, Noop>| ConcurrencyLimit::new(1).attach(hooks), "taking a concurrency slot failed")]
#[case::cooldown(|hooks: &mut Hooks<'_, Noop>| Cooldown::new(Duration::from_secs(1)).attach(hooks), "checking the cooldown failed")]
#[case::debounce(|hooks: &mut Hooks<'_, Noop>| Debounce::new(Duration::from_secs(1)).attach(hooks), "debouncing failed")]
#[case::rate_limit(|hooks: &mut Hooks<'_, Noop>| RateLimit::new(1).attach(hooks), "checking the rate limit failed")]
#[tokio::test]
async fn an_unreachable_redis_blocks_the_task_for_a_retry(
    #[case] attach: Attach,
    #[case] reason: &str,
) {
    let docket = unreachable().await;
    let blocked = admit(attach, context(&docket, "a")).await.err().unwrap();
    assert!(blocked.reason().starts_with(reason), "{}", blocked.reason());
    assert!(blocked.reschedule);
}

#[rstest]
#[case::concurrency(|hooks: &mut Hooks<'_, Noop>| ConcurrencyLimit::per_field("customer", 1).attach(hooks))]
#[case::cooldown(|hooks: &mut Hooks<'_, Noop>| Cooldown::per_field("customer", Duration::from_secs(1)).attach(hooks))]
#[case::debounce(|hooks: &mut Hooks<'_, Noop>| Debounce::per_field("customer", Duration::from_secs(1)).scope("tests").attach(hooks))]
#[case::rate_limit(|hooks: &mut Hooks<'_, Noop>| RateLimit::per_field("customer", 1).attach(hooks))]
#[tokio::test]
async fn a_limit_on_a_missing_field_drops_the_task(#[case] attach: Attach) {
    let docket = unreachable().await;
    let blocked = admit(attach, context(&docket, "a")).await.err().unwrap();
    assert_eq!(
        blocked.reason(),
        "the noop task's arguments have no customer field to limit by"
    );
    assert!(!blocked.reschedule);
}

#[tokio::test]
async fn a_cooldown_admits_one_call_and_drops_the_next() {
    let docket = memory().await;
    let attach: Attach = |hooks| Cooldown::new(Duration::from_secs(60)).attach(hooks);

    let first = admit(attach, context(&docket, "a")).await;
    let second = admit(attach, context(&docket, "b")).await.err().unwrap();

    assert!(first.is_ok());
    assert_eq!(second.reason(), "the task is cooling down");
    assert!(!second.reschedule);
}

#[tokio::test]
async fn debounce_holds_the_first_call_drops_rivals_and_then_runs_it() {
    let docket = memory().await;
    let attach: Attach = |hooks| Debounce::new(Duration::from_millis(100)).attach(hooks);

    let waiting = admit(attach, context(&docket, "a")).await.err().unwrap();
    let rival = admit(attach, context(&docket, "b")).await.err().unwrap();
    // The first call runs once no call has come for the settle time.
    tokio::time::sleep(Duration::from_millis(110)).await;
    let settled = admit(attach, context(&docket, "a")).await;

    assert_eq!(waiting.reason(), "waiting for calls to settle");
    assert_eq!(waiting.retry_delay, Some(Duration::from_millis(100)));
    assert_eq!(rival.reason(), "a newer call is settling");
    assert!(!rival.reschedule);
    assert!(settled.is_ok());
}

#[rstest]
#[case::waits(|hooks: &mut Hooks<'_, Noop>| RateLimit::new(1).attach(hooks), true)]
#[case::drops(|hooks: &mut Hooks<'_, Noop>| RateLimit::new(1).drop_excess().attach(hooks), false)]
#[tokio::test]
async fn a_rate_limit_admits_up_to_its_limit(#[case] attach: Attach, #[case] reschedule: bool) {
    let docket = memory().await;

    let first = admit(attach, context(&docket, "a")).await;
    let second = admit(attach, context(&docket, "b")).await.err().unwrap();

    assert!(first.is_ok());
    assert_eq!(second.reason(), "the rate limit is reached");
    assert_eq!(second.reschedule, reschedule);
}
