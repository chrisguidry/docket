//! Running one task, from its claim to its terminal state.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use futures::FutureExt;
use redis::{AsyncCommands, Value};
use tracing::Instrument;

use super::session::{Active, Delivery, Shared};
use crate::behaviors::{
    AdmissionBlocked, AfterCompletion, AfterFailure, BoxError, Outcome, Release, Released,
};
use crate::context::{self, Context};
use crate::docket::{Docket, Placement, Registered};
use crate::error::Result;
use crate::execution::{Message, State};
use crate::keys::WORKER_GROUP;
use crate::scripts;
use crate::wire::iso;

/// How soon a blocked task tries again when its hook names no delay.
const ADMISSION_RETRY_DELAY: Duration = Duration::from_millis(100);

/// A handler whose future ended with a panic fails with this error.
#[derive(Debug, thiserror::Error)]
#[error("the task's handler panicked: {0}")]
struct Panicked(String);

/// A task stopped by [`Docket::cancel`](crate::Docket::cancel).
#[derive(Debug, thiserror::Error)]
#[error("the task was cancelled")]
struct Cancelled;

pub(super) async fn run(worker: Arc<Shared>, delivery: Delivery) -> Result<()> {
    let message = &delivery.message;
    // Logs inside the handler carry the task's name, key, and attempt, the
    // way pydocket's TaskLogger adds them.
    let span = tracing::info_span!(
        "docket.task",
        docket = worker.docket.name(),
        task = %message.function,
        key = %message.key,
        attempt = message.attempt,
        worker = %worker.settings.name,
    );
    let active = worker.start(&delivery.id, &message.key);
    let started = tokio::time::Instant::now();
    let result = execute(&worker, &delivery, &active)
        .instrument(span.clone())
        .await;
    span.in_scope(
        || tracing::info!(elapsed = ?started.elapsed(), ok = result.is_ok(), "task finished"),
    );
    worker.finish(&delivery.id);
    result
}

async fn execute(worker: &Shared, delivery: &Delivery, active: &Active) -> Result<()> {
    let docket = &worker.docket;
    let message = &delivery.message;
    let run = Run { docket, delivery };

    if docket.is_struck(message) || !worker.allow_run(&message.key) {
        return run.strike().await;
    }
    let Some(generation) = run.claim(&worker.settings.name).await? else {
        return Ok(());
    };
    let Some(registered) = docket.registered(&message.function) else {
        tracing::warn!(function = %message.function, key = %message.key, "no handler is registered for this task");
        return run.terminal(State::Completed, generation, Vec::new()).await;
    };

    let ctx = context(worker, delivery, &registered);
    let releases = match admit(&registered, &ctx).await {
        Ok(releases) => releases,
        Err(blocked) => return run.blocked(&blocked, generation).await,
    };

    let outcome = call(&registered, &ctx, active).await;
    let cancelled = active.cancelled_by_docket.load(Ordering::SeqCst);
    match outcome {
        _ if cancelled => {
            release(releases).await;
            run.terminal(State::Cancelled, generation, Vec::new()).await
        }
        Ok(output) => {
            let after = complete(&registered, &ctx, Ok(output.clone())).await;
            let result = run.succeed(after.as_ref(), output, generation).await;
            release(releases).await;
            result
        }
        Err(error) => {
            release(releases).await;
            let error = Arc::new(error);
            if let Some(failure) = &registered.hooks.failure
                && let AfterFailure::RetryAt(when) = failure(ctx.clone(), Arc::clone(&error)).await
            {
                return run.retry(when, generation).await;
            }
            let message = error.to_string();
            let outcome: Outcome = Err(Box::new(Failed(message.clone())));
            if let Some(after) = complete(&registered, &ctx, outcome).await {
                run.after_completion(&after, generation).await?;
            }
            run.terminal(
                State::Failed,
                generation,
                vec![("error".into(), message.into_bytes())],
            )
            .await
        }
    }
}

/// The error a completion hook sees for a failed task.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Failed(String);

fn context(worker: &Shared, delivery: &Delivery, registered: &Registered) -> Context {
    let message = &delivery.message;
    Context::new(context::Run {
        docket: worker.docket.clone(),
        worker: worker.settings.name.clone(),
        key: message.key.clone(),
        function: message.function.clone(),
        attempt: message.attempt,
        when: message.when,
        args: serde_json::from_str(&message.args).unwrap_or(serde_json::Value::Null),
        behaviors: registered
            .hooks
            .contexts
            .iter()
            .map(|make| make())
            .collect(),
        delivery: context::Delivery {
            message_id: delivery.id.clone(),
            message: message.clone(),
            redelivered: delivery.redelivered,
            redelivery_timeout: worker.settings.redelivery_timeout,
        },
    })
}

/// Runs the admission hooks in order.  When one blocks, the ones that
/// already admitted the task release what they gave it, newest first.
async fn admit(
    registered: &Registered,
    ctx: &Context,
) -> std::result::Result<Vec<Release>, AdmissionBlocked> {
    let mut releases = Vec::new();
    for admission in &registered.hooks.admissions {
        match admission(ctx.clone()).await {
            Ok(admitted) => releases.extend(admitted.release),
            Err(blocked) => {
                while let Some(release) = releases.pop() {
                    release(Released::Blocked).await;
                }
                return Err(blocked);
            }
        }
    }
    Ok(releases)
}

async fn release(mut releases: Vec<Release>) {
    while let Some(release) = releases.pop() {
        release(Released::Ran).await;
    }
}

/// Runs the handler, through the task's runtime hook, until it returns or
/// someone cancels it.
async fn call(registered: &Registered, ctx: &Context, active: &Active) -> Outcome {
    let args = ctx.delivery().message.args.clone();
    let task = (registered.handler)(ctx.clone(), &args);
    let task = AssertUnwindSafe(task).catch_unwind().map(|outcome| {
        outcome.unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Err(Box::new(Panicked(message)) as BoxError)
        })
    });
    let task = match &registered.hooks.runtime {
        Some(runtime) => runtime.run(ctx, Box::pin(task)),
        None => Box::pin(task),
    };
    tokio::select! {
        outcome = task => outcome,
        () = active.cancel.cancelled() => Err(Box::new(Cancelled)),
    }
}

/// Runs the completion hook, or returns `None` when the task has none.
async fn complete(
    registered: &Registered,
    ctx: &Context,
    outcome: Outcome,
) -> Option<AfterCompletion> {
    let completion = registered.hooks.completion.as_ref()?;
    Some(completion(ctx.clone(), Arc::new(outcome)).await)
}

/// The Redis side of one delivery.
struct Run<'a> {
    docket: &'a Docket,
    delivery: &'a Delivery,
}

impl Run<'_> {
    fn message(&self) -> &Message {
        &self.delivery.message
    }

    fn key(&self) -> &str {
        &self.delivery.message.key
    }

    /// Claims the delivery, and returns the run's generation, or `None` when
    /// the task must not run because it was superseded or cancelled.
    async fn claim(&self, worker: &str) -> Result<Option<i64>> {
        let keys = self.docket.keys();
        let key = self.key();
        let started_at = iso(Utc::now());
        let payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": "running",
            "worker": worker,
            "started_at": started_at,
        });
        let call = scripts::Claim {
            runs_key: keys.runs(key),
            progress_key: keys.progress(key),
            known_key: keys.known(key),
            stream_id_key: keys.stream_id(key),
            state_channel: keys.state(key),
            stream_key: keys.stream(),
            worker: worker.to_owned(),
            started_at,
            generation: self.message().generation,
            state_payload: payload.to_string(),
            key_json: serde_json::Value::from(key).to_string(),
            worker_group_name: WORKER_GROUP.to_owned(),
            message_id: self.delivery.id.clone(),
        }
        .call();
        let mut connection = self.docket.handle();
        // The reply is the claim's status, the runs hash, and the progress
        // hash.
        let (status, runs, _): (String, std::collections::HashMap<String, String>, Value) =
            call.run(&mut connection).await?;
        if status != "OK" {
            return Ok(None);
        }
        Ok(Some(
            runs.get("generation")
                .and_then(|generation| generation.parse().ok())
                .unwrap_or(self.message().generation),
        ))
    }

    /// Ends a struck delivery as cancelled without running it.
    async fn strike(&self) -> Result<()> {
        let keys = self.docket.keys();
        let key = self.key();
        let mut connection = self.docket.handle();
        let () = redis::pipe()
            .hdel(keys.runs(key), &["known", "stream_id"])
            .ignore()
            .del(&[keys.known(key), keys.stream_id(key)])
            .ignore()
            .query_async(&mut connection)
            .await?;
        self.terminal(State::Cancelled, self.message().generation, Vec::new())
            .await
    }

    /// Records a terminal state and acknowledges the delivery.
    async fn terminal(
        &self,
        state: State,
        generation: i64,
        extra_fields: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let keys = self.docket.keys();
        let key = self.key();
        let completed_at = iso(Utc::now());
        let mut payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": state.as_str(),
            "completed_at": completed_at,
        });
        if let Some((_, error)) = extra_fields.iter().find(|(field, _)| field == "error") {
            payload["error"] = String::from_utf8_lossy(error).into_owned().into();
        }
        let call = scripts::Terminal {
            runs_key: keys.runs(key),
            state_channel: keys.state(key),
            progress_key: keys.progress(key),
            stream_key: keys.stream(),
            generation,
            state: state.as_str().to_owned(),
            completed_at,
            ttl_seconds: self.docket.ttl_seconds(),
            state_payload: payload.to_string(),
            worker_group_name: WORKER_GROUP.to_owned(),
            message_id: self.delivery.id.clone(),
            extra_fields,
        }
        .call();
        let mut connection = self.docket.handle();
        call.run::<Value, _>(&mut connection).await?;
        Ok(())
    }

    /// Puts the delivery back in the docket at `when`, acknowledging it in
    /// the same script.
    async fn reschedule(
        &self,
        when: chrono::DateTime<Utc>,
        attempt: u32,
        generation: i64,
    ) -> Result<()> {
        let mut message = self.message().clone();
        message.when = when;
        message.attempt = attempt;
        message.generation = generation;
        self.docket
            .place(Placement {
                message,
                replace: true,
                reschedule_message_id: self.delivery.id.clone(),
                expected_generation: 0,
            })
            .await?;
        Ok(())
    }

    async fn retry(&self, when: chrono::DateTime<Utc>, generation: i64) -> Result<()> {
        self.reschedule(when, self.message().attempt + 1, generation)
            .await
    }

    async fn blocked(&self, blocked: &AdmissionBlocked, generation: i64) -> Result<()> {
        if blocked.handled {
            return Ok(());
        }
        if !blocked.reschedule {
            return self
                .terminal(State::Cancelled, generation, Vec::new())
                .await;
        }
        let delay = blocked
            .retry_delay
            .filter(|delay| !delay.is_zero())
            .unwrap_or(ADMISSION_RETRY_DELAY);
        let when = Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX);
        self.reschedule(when, self.message().attempt, generation)
            .await
    }

    /// Ends a completed task, after acting on its completion hook's decision.
    async fn succeed(
        &self,
        after: Option<&AfterCompletion>,
        output: serde_json::Value,
        generation: i64,
    ) -> Result<()> {
        let handled = match after {
            Some(after) => self.after_completion(after, generation).await?,
            None => false,
        };
        if handled {
            return self
                .terminal(State::Completed, generation, Vec::new())
                .await;
        }
        self.store(output).await?;
        self.terminal(State::Completed, generation, self.result_field())
            .await
    }

    fn result_field(&self) -> Vec<(String, Vec<u8>)> {
        vec![("result_key".to_owned(), self.key().as_bytes().to_vec())]
    }

    /// Acts on a completion hook's decision, and returns whether it took the
    /// place of the task's normal ending.
    async fn after_completion(&self, after: &AfterCompletion, generation: i64) -> Result<bool> {
        match after {
            AfterCompletion::Finish => Ok(false),
            AfterCompletion::Cancel => {
                self.docket.cancel(self.key()).await?;
                Ok(false)
            }
            AfterCompletion::Reschedule { when, args } => {
                let mut message = self.message().clone();
                message.when = *when;
                message.attempt = 1;
                if let Some(args) = args {
                    message.args = args.to_string();
                }
                self.docket
                    .place(Placement {
                        message,
                        replace: true,
                        reschedule_message_id: String::new(),
                        expected_generation: generation,
                    })
                    .await?;
                Ok(true)
            }
        }
    }

    /// Stores a completed task's output, unless the docket keeps nothing or
    /// there is no output to keep.
    async fn store(&self, output: serde_json::Value) -> Result<()> {
        let ttl = self.docket.ttl_seconds();
        if ttl == 0 || output.is_null() {
            return Ok(());
        }
        let stored = serde_json::json!({ "ok": output }).to_string();
        let mut connection = self.docket.handle();
        let ttl = u64::try_from(ttl).unwrap_or(u64::MAX);
        let () = connection
            .set_ex(self.docket.keys().result(self.key()), stored, ttl)
            .await?;
        Ok(())
    }
}
