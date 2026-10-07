//! Running one task, from its claim to its terminal state, with the
//! metrics, span, and log lines pydocket records for each run.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use chrono::Utc;
use futures::FutureExt;
use opentelemetry::KeyValue;
use opentelemetry::context::FutureExt as _;
use opentelemetry::trace::{Status, TraceContextExt};
use tracing::Instrument;

use super::run_state::{Claim, Run};
use super::session::{Active, Delivery, Shared};
use crate::behaviors::{AfterFailure, BoxError, NotAdmitted, Outcome, Release, Released};
use crate::context::{self, Context};
use crate::docket::Registered;
use crate::error::Result;
use crate::execution::State;
use crate::telemetry::{self, call_repr, format_duration};
use crate::wire::iso;

/// A handler whose future ended with a panic fails with this error.
#[derive(Debug, thiserror::Error)]
#[error("the task's handler panicked: {0}")]
struct Panicked(String);

/// A task stopped by [`Docket::cancel`](crate::Docket::cancel).
#[derive(Debug, thiserror::Error)]
#[error("the task was cancelled")]
struct Cancelled;

/// The handler for a task with no handler of its own and no fallback: it
/// warns and completes, as pydocket's `default_fallback_task` does, so the
/// run is counted and traced like any other.
static DEFAULT_FALLBACK: LazyLock<Registered> = LazyLock::new(|| {
    Registered::fallback(|ctx: Context, _args: serde_json::Value| async move {
        tracing::warn!(
            "Unknown task {:?} received - dropping. \
             Register it with docket.register before the worker starts.",
            ctx.function()
        );
        Ok::<_, std::convert::Infallible>(serde_json::Value::Null)
    })
});

pub(super) async fn run(worker: Arc<Shared>, delivery: Delivery) -> Result<()> {
    let message = &delivery.message;
    // Logs inside the handler carry the run's fields, the way pydocket's
    // TaskLogger adds them.
    let span = tracing::info_span!(
        "docket.task",
        "docket.name" = worker.docket.name(),
        "docket.worker" = %worker.settings.name,
        "docket.task" = %message.function,
        "docket.key" = %message.key,
        "docket.when" = %iso(message.when),
        "docket.attempt" = message.attempt,
    );
    let active = worker.start(&delivery.id, &message.key);
    let result = execute(&worker, &delivery, &active).instrument(span).await;
    worker.finish(&delivery.id);
    result
}

async fn execute(worker: &Shared, delivery: &Delivery, active: &Active) -> Result<()> {
    let docket = &worker.docket;
    let message = &delivery.message;
    let registered = docket
        .registered(&message.function)
        .or_else(|| worker.settings.fallback.clone())
        .unwrap_or_else(|| DEFAULT_FALLBACK.clone());
    let args = serde_json::from_str(&message.args).unwrap_or(serde_json::Value::Null);
    let run = Run {
        docket,
        delivery,
        worker: &worker.settings.name,
        call: call_repr(&message.function, registered.fields, &args, &message.key),
    };
    let metrics = &docket.telemetry().metrics;

    if docket.is_struck(message) || !worker.allow_run(&message.key) {
        tracing::warn!("🗙 {}", run.call);
        metrics.tasks_stricken.add(1, &run.labels_where("worker"));
        return run.strike().await;
    }
    let generation = match run.claim().await? {
        Claim::Claimed(generation) => generation,
        // Docket::cancel counted the cancellation already.
        Claim::Cancelled => {
            tracing::info!("✗ {} (cancelled)", run.call);
            return Ok(());
        }
        Claim::Superseded => {
            tracing::info!("↬ {} (superseded)", run.call);
            metrics.tasks_superseded.add(1, &run.labels_where("worker"));
            return Ok(());
        }
    };

    let labels = run.labels();
    let punctuality = (Utc::now() - message.when).as_seconds_f64();
    metrics.tasks_started.add(1, &labels);
    if delivery.redelivered {
        metrics.tasks_redelivered.add(1, &labels);
    }
    metrics.tasks_running.add(1, &labels);
    metrics.task_punctuality.record(punctuality, &labels);
    let arrow = if message.attempt > 1 { "↬" } else { "↪" };
    tracing::info!("{arrow} [{}] {}", format_duration(punctuality), run.call);

    let mut attributes = telemetry::worker_labels(docket.name(), &worker.settings.name);
    attributes.extend(telemetry::run_attributes(message));
    let span = docket.telemetry().consumer_span(message, attributes);
    let ctx = context(worker, delivery, &registered);
    let (result, duration) = attempt(&run, &registered, &ctx, active, generation)
        .with_context(span.clone())
        .await;
    span.span().end();

    metrics.tasks_running.add(-1, &labels);
    metrics.tasks_completed.add(1, &labels);
    metrics.task_duration.record(duration, &labels);
    result
}

/// Admits and runs the task, then records its end.  Returns the outcome and
/// how long the handler ran, which is zero for a run that admission
/// blocked, as in pydocket.
async fn attempt(
    run: &Run<'_>,
    registered: &Registered,
    ctx: &Context,
    active: &Active,
    generation: i64,
) -> (Result<()>, f64) {
    let span = opentelemetry::Context::current();
    let metrics = &run.docket.telemetry().metrics;
    let started = Instant::now();

    let releases = match admit(registered, ctx).await {
        Ok(releases) => releases,
        Err(NotAdmitted::Failed(error)) => {
            let duration = started.elapsed().as_secs_f64();
            metrics.tasks_failed.add(1, &run.labels());
            telemetry::fail(&span, &error.to_string());
            return (
                fail(run, registered, ctx, error, generation, duration).await,
                duration,
            );
        }
        Err(NotAdmitted::Blocked(blocked)) => {
            // Admission control asks for the task to come back later, which
            // is not a failure, so the span says what happened and ends ok.
            span.span().add_event(
                "exception",
                vec![KeyValue::new(
                    "exception.message",
                    blocked.reason().to_owned(),
                )],
            );
            span.span().set_status(Status::Ok);
            return (run.blocked(&blocked, generation).await, 0.0);
        }
    };

    let outcome = call(registered, ctx, active).await;
    let duration = started.elapsed().as_secs_f64();
    let cancelled = active.cancelled_by_docket.load(Ordering::SeqCst);
    let result = match outcome {
        _ if cancelled => {
            release(releases).await;
            span.span().set_status(Status::Ok);
            tracing::info!("✗ [{}] {} (cancelled)", format_duration(duration), run.call);
            run.terminal(State::Cancelled, generation, Vec::new()).await
        }
        Ok(output) => {
            metrics.tasks_succeeded.add(1, &run.labels());
            span.span().set_status(Status::Ok);
            let after = complete(registered, ctx, Ok(output.clone())).await;
            let result = run
                .succeed(after.as_ref(), output, generation, duration)
                .await;
            release(releases).await;
            result
        }
        Err(error) => {
            release(releases).await;
            metrics.tasks_failed.add(1, &run.labels());
            let message = error.to_string();
            telemetry::fail(&span, &message);
            fail(run, registered, ctx, error, generation, duration).await
        }
    };
    (result, duration)
}

/// Ends a failed run: a retry when the failure hook asks for one, otherwise
/// the completion hook's decision and a failed state.
async fn fail(
    run: &Run<'_>,
    registered: &Registered,
    ctx: &Context,
    error: BoxError,
    generation: i64,
    duration: f64,
) -> Result<()> {
    let error = Arc::new(error);
    let message = error.to_string();
    if let Some(failure) = &registered.hooks.failure
        && let AfterFailure::RetryAt(when) = failure(ctx.clone(), Arc::clone(&error)).await
    {
        return run.retry(when, generation, duration, &message).await;
    }
    let outcome: Outcome = Err(Box::new(Failed(message.clone())));
    let handled = match complete(registered, ctx, outcome).await {
        Some(after) => run.after_completion(&after, generation, duration).await?,
        None => false,
    };
    if !handled {
        tracing::error!(
            error = message,
            "↩ [{}] {}",
            format_duration(duration),
            run.call
        );
    }
    run.terminal(
        State::Failed,
        generation,
        vec![("error".into(), message.into_bytes())],
    )
    .await
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
) -> std::result::Result<Vec<Release>, NotAdmitted> {
    let mut releases = Vec::new();
    for admission in &registered.hooks.admissions {
        match admission(ctx.clone()).await {
            Ok(admitted) => releases.extend(admitted.release),
            Err(refused) => {
                while let Some(release) = releases.pop() {
                    release(Released::Blocked).await;
                }
                return Err(refused);
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
) -> Option<crate::behaviors::AfterCompletion> {
    let completion = registered.hooks.completion.as_ref()?;
    Some(completion(ctx.clone(), Arc::new(outcome)).await)
}
