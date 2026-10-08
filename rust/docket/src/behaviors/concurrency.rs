//! Capping how many copies of a task run at once, across every worker.

use std::time::Duration;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use super::subject::required_subject;
use super::{Admission, AdmissionBlocked, Admitted, Behavior, Hooks, NotAdmitted, Released};
use crate::connection::Handle;
use crate::context::Context;
use crate::docket::Docket;
use crate::keys::WORKER_GROUP;
use crate::scripts;
use crate::task::Task;
use crate::wire::seconds;

/// The key prefix of the task that wakes parked waiters when no release does.
pub(crate) const SAFEGUARD_PREFIX: &str = "__safeguard__:";

/// Caps how many copies of a task run at once, for the whole task or for
/// each value of one argument field.  A task over the limit waits, parked,
/// until a running copy finishes.
///
/// ```
/// # use docket::behaviors::ConcurrencyLimit;
/// ConcurrencyLimit::new(3);                     // three at a time
/// ConcurrencyLimit::per_field("customer", 1);  // one at a time per customer
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConcurrencyLimit {
    field: Option<String>,
    max: u32,
    scope: Option<String>,
}

impl ConcurrencyLimit {
    /// At most `max` copies of the task at once.
    #[must_use]
    pub fn new(max: u32) -> Self {
        Self {
            field: None,
            max,
            scope: None,
        }
    }

    /// At most `max` copies at once for each value of `field`.  A task
    /// without the field fails.
    pub fn per_field(field: impl Into<String>, max: u32) -> Self {
        Self {
            field: Some(field.into()),
            max,
            scope: None,
        }
    }

    /// Counts in a separate set of slots, so that limits with different
    /// scopes never share slots.
    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }
}

impl<T: Task> Behavior<T> for ConcurrencyLimit {
    fn attach(self, hooks: &mut Hooks<'_, T>) {
        hooks.erased.needs_safeguard = true;
        hooks.admission(self);
    }
}

impl Admission for ConcurrencyLimit {
    async fn admit(&self, ctx: &Context) -> Result<Admitted, NotAdmitted> {
        let docket = ctx.docket().clone();
        let keys = docket.keys();
        let key = ctx.key().to_owned();
        let slots = keys.concurrency(
            self.scope.as_deref(),
            &required_subject(ctx, self.field.as_deref())?,
        );
        let waiters = format!("{slots}:waiters");
        let delivery = ctx.delivery();
        let timeout = delivery.redelivery_timeout;
        let now = seconds(docket.now());
        let key_ttl = i64::try_from((timeout * 4).as_secs())
            .unwrap_or(i64::MAX)
            .max(1);
        let payload = serde_json::json!({"type": "state", "key": key, "state": "scheduled"});
        let call = scripts::AcquireOrPark {
            slots_key: slots.clone(),
            waiters_stream: waiters.clone(),
            stream_key: keys.stream(),
            runs_key: keys.runs(&key),
            max_concurrent: i64::from(self.max),
            task_key: key.clone(),
            current_time: now,
            is_redelivery: delivery.redelivered,
            stale_threshold: now - timeout.as_secs_f64(),
            key_ttl,
            message_id: delivery.message_id.clone(),
            worker_group_name: WORKER_GROUP.to_owned(),
            state_channel: keys.state(&key),
            state_payload: payload.to_string(),
            message: delivery.message.fields(),
        }
        .call();

        let acquired: crate::Result<(i64, Handle)> = async {
            let mut connection = docket.connection().await?;
            let acquired = call.run(&mut connection).await;
            acquired
                .map(|acquired| (acquired, connection))
                .map_err(Into::into)
        }
        .await;
        match acquired {
            Ok((1, _)) => Ok(hold(
                docket, slots, waiters, key, self.max, timeout, key_ttl,
            )),
            Ok((_, mut connection)) => {
                park(
                    &docket,
                    &mut connection,
                    &slots,
                    &waiters,
                    &key,
                    self.max,
                    timeout,
                )
                .await;
                Err(AdmissionBlocked::new("the concurrency limit is reached")
                    .handled()
                    .into())
            }
            Err(error) => Err(NotAdmitted::failed(format!(
                "taking a concurrency slot failed: {error}"
            ))),
        }
    }
}

/// Holds an acquired slot: renews it while the task runs, and gives it up
/// and wakes the next waiter when the task is done.
fn hold(
    docket: Docket,
    slots: String,
    waiters: String,
    key: String,
    max: u32,
    timeout: Duration,
    key_ttl: i64,
) -> Admitted {
    let renewal = Renewal(tokio::spawn({
        let docket = docket.clone();
        let slots = slots.clone();
        let key = key.clone();
        async move {
            loop {
                tokio::time::sleep(timeout / 4).await;
                if let Err(error) = renew(&docket, &slots, &key, key_ttl).await {
                    tracing::warn!(%error, "Concurrency lease renewal failed for {slots}");
                }
            }
        }
    }));
    Admitted::with_release(move |_: Released| async move {
        drop(renewal);
        let keys = docket.keys();
        let call = scripts::ReleaseAndWake {
            slots_key: slots,
            waiters_stream: waiters,
            stream_key: keys.stream(),
            queue_key: keys.queue(),
            task_key: key,
            max_concurrent: i64::from(max),
            stale_threshold: seconds(docket.now()) - timeout.as_secs_f64(),
            runs_prefix: keys.runs_prefix(),
            state_prefix: keys.state_prefix(),
            parked_prefix: keys.parked_prefix(),
        }
        .call();
        if let Err(error) = super::run_script::<redis::Value>(&docket, &call).await {
            tracing::warn!(%error, "releasing a concurrency slot failed");
        }
    })
}

/// Marks a held slot as alive, so that no waiter's safeguard frees it.
async fn renew(docket: &Docket, slots: &str, key: &str, key_ttl: i64) -> crate::Result<()> {
    let mut connection = docket.connection().await?;
    redis::pipe()
        .zadd(slots, key, seconds(docket.now()))
        .ignore()
        .expire(slots, key_ttl)
        .ignore()
        .query_async(&mut connection)
        .await
        .map_err(Into::into)
}

/// The task that renews a held slot.  Dropping it stops the renewal, so a
/// run that ends without its release, such as one whose worker is dropped
/// mid-task, lets the slot go stale for the safeguard to free.
struct Renewal(tokio::task::JoinHandle<()>);

impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// After the script parks a task, schedules the safeguard that wakes the
/// waiters if no release ever does, for example after every slot holder
/// died.
async fn park(
    docket: &Docket,
    connection: &mut Handle,
    slots: &str,
    waiters: &str,
    key: &str,
    max: u32,
    timeout: Duration,
) {
    let safeguard = format!("{SAFEGUARD_PREFIX}{key}");
    let scheduled: crate::Result<()> = async {
        docket
            .add(SafeguardWake {
                slots_key: slots.to_owned(),
                waiters_stream: waiters.to_owned(),
                max_concurrent: max,
            })
            .key(&safeguard)
            .after(timeout)
            .await?;
        let state: Option<String> = connection.hget(docket.keys().runs(key), "state").await?;
        cancel_unless_parked(docket, &safeguard, state.as_deref()).await
    }
    .await;
    if let Err(error) = scheduled {
        tracing::warn!(%error, "scheduling a concurrency safeguard failed");
    }
}

/// Cancels a task's safeguard unless the task is still parked.  A release
/// can wake the task between the park and the safeguard's scheduling; then
/// the safeguard has nothing left to do.
async fn cancel_unless_parked(
    docket: &Docket,
    safeguard: &str,
    state: Option<&str>,
) -> crate::Result<()> {
    if state == Some("scheduled") {
        return Ok(());
    }
    docket.cancel(safeguard).await
}

/// Wakes the parked waiters of a concurrency limit, freeing slots whose
/// holders stopped renewing them.
#[derive(Serialize, Deserialize, docket_rs_macros::Task)]
#[task(name = "__docket_safeguard_wake__", crate = crate)]
pub(crate) struct SafeguardWake {
    slots_key: String,
    waiters_stream: String,
    max_concurrent: u32,
}

pub(crate) async fn safeguard_wake(ctx: Context, args: SafeguardWake) -> crate::Result<()> {
    let docket = ctx.docket();
    let keys = docket.keys();
    let call = scripts::ScavengeAndWake {
        slots_key: args.slots_key,
        waiters_stream: args.waiters_stream,
        stream_key: keys.stream(),
        queue_key: keys.queue(),
        max_concurrent: i64::from(args.max_concurrent),
        stale_threshold: seconds(docket.now()) - ctx.delivery().redelivery_timeout.as_secs_f64(),
        runs_prefix: keys.runs_prefix(),
        state_prefix: keys.state_prefix(),
        parked_prefix: keys.parked_prefix(),
    }
    .call();
    super::run_script::<redis::Value>(docket, &call)
        .await
        .map(drop)
}

#[cfg(all(test, feature = "memory"))]
mod tests;
