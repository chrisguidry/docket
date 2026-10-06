//! The metrics, spans, and log lines docket emits, which match pydocket's so
//! that one dashboard and one trace view serve every language.
//!
//! Instruments and tracers come from the global OpenTelemetry providers when
//! a docket connects.  The `opentelemetry` crate binds them then, not when
//! they record, so an application installs its providers first.

mod logs;
mod metrics;
mod propagation;

use std::borrow::Cow;

use opentelemetry::global::{self, BoxedSpan, BoxedTracer};
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};

use crate::execution::Message;
use crate::wire::iso;

pub(crate) use logs::{call_repr, format_duration};
pub(crate) use metrics::Metrics;
pub(crate) use propagation::{carrier, inject};

/// A docket's instruments and tracers.
pub(crate) struct Telemetry {
    pub metrics: Metrics,
    /// pydocket names its tracers after the modules that hold them, so
    /// producer spans come from `docket.docket` and runs from
    /// `docket.worker`.
    producer: BoxedTracer,
    consumer: BoxedTracer,
}

impl Telemetry {
    pub fn new() -> Self {
        Self {
            metrics: Metrics::new(&global::meter("docket")),
            producer: global::tracer("docket.docket"),
            consumer: global::tracer("docket.worker"),
        }
    }

    /// Starts a producer span as a child of the current context, and returns
    /// a context that holds it, for the work the span covers.
    pub fn producer_span(&self, name: &'static str, attributes: Vec<KeyValue>) -> Context {
        let builder = self.producer.span_builder(name).with_attributes(attributes);
        let span: BoxedSpan = self
            .producer
            .build_with_context(builder, &Context::current());
        Context::current_with_span(span)
    }

    /// Starts a run's span.  It begins a trace of its own, linked to the
    /// span that put the task in the docket, the way a message consumer's
    /// span links to its producer's.
    pub fn consumer_span(&self, message: &Message, attributes: Vec<KeyValue>) -> Context {
        let builder = self
            .consumer
            .span_builder(Cow::Owned(message.function.clone()))
            .with_kind(SpanKind::Consumer)
            .with_attributes(attributes)
            .with_links(propagation::links(&message.trace));
        let span: BoxedSpan = self.consumer.build_with_context(builder, &Context::new());
        Context::new().with_span(span)
    }
}

/// Ends a producer span with the attributes its work produced, or with the
/// error it failed with, the way pydocket's spans record an exception.
pub(crate) fn end(context: &Context, outcome: std::result::Result<Vec<KeyValue>, &crate::Error>) {
    let span = context.span();
    match outcome {
        Ok(attributes) => span.set_attributes(attributes),
        Err(error) => fail(context, &error.to_string()),
    }
    span.end();
}

/// Marks a span failed, with an `exception` event that carries the message.
pub(crate) fn fail(context: &Context, message: &str) {
    let span = context.span();
    span.add_event(
        "exception",
        vec![KeyValue::new("exception.message", message.to_owned())],
    );
    span.set_status(Status::error(message.to_owned()));
}

/// `docket.name`, which every metric carries.
pub(crate) fn docket_labels(docket: &str) -> Vec<KeyValue> {
    vec![KeyValue::new("docket.name", docket.to_owned())]
}

/// The labels of a metric about one task, from a producer.
pub(crate) fn task_labels(docket: &str, task: &str) -> Vec<KeyValue> {
    let mut labels = docket_labels(docket);
    labels.push(KeyValue::new("docket.task", task.to_owned()));
    labels
}

/// The labels of a metric about a worker.
pub(crate) fn worker_labels(docket: &str, worker: &str) -> Vec<KeyValue> {
    let mut labels = docket_labels(docket);
    labels.push(KeyValue::new("docket.worker", worker.to_owned()));
    labels
}

/// The labels of a metric about one task on a worker.
pub(crate) fn run_labels(docket: &str, worker: &str, task: &str) -> Vec<KeyValue> {
    let mut labels = worker_labels(docket, worker);
    labels.push(KeyValue::new("docket.task", task.to_owned()));
    labels
}

/// The attributes that identify one run of a task, as pydocket's
/// `Execution.specific_labels` gives them, with `code.function.name`.
pub(crate) fn run_attributes(message: &Message) -> Vec<KeyValue> {
    vec![
        KeyValue::new("docket.task", message.function.clone()),
        KeyValue::new("docket.key", message.key.clone()),
        KeyValue::new("docket.when", iso(message.when)),
        KeyValue::new("docket.attempt", i64::from(message.attempt)),
        KeyValue::new("code.function.name", message.function.clone()),
    ]
}
