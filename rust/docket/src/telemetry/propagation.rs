//! The trace context a message carries from the span that put it in the
//! docket to the run that takes it out.  The global text map propagator
//! chooses the fields, such as W3C's `traceparent`, as it does in pydocket.

use std::collections::HashMap;

use opentelemetry::trace::{Link, TraceContextExt};
use opentelemetry::{Context, global};

/// The fields that carry the current trace context, sorted so a message's
/// fields come out the same way each time.
pub(crate) fn inject() -> Vec<(String, String)> {
    let mut carrier = HashMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&Context::current(), &mut carrier);
    });
    let mut fields: Vec<(String, String)> = carrier.into_iter().collect();
    fields.sort();
    fields
}

/// The trace context fields among a message's fields.
pub(crate) fn carrier(fields: &HashMap<String, Vec<u8>>) -> HashMap<String, String> {
    global::get_text_map_propagator(|propagator| {
        propagator
            .fields()
            .filter_map(|field| {
                fields.get(field).map(|value| {
                    (
                        field.to_owned(),
                        String::from_utf8_lossy(value).into_owned(),
                    )
                })
            })
            .collect()
    })
}

/// The context that `carrier` holds, which is empty when the message
/// carries no trace context.
pub(crate) fn extract(carrier: &HashMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// A link to the span in `context`, or none when it holds no valid one.
pub(crate) fn links(context: &Context) -> Vec<Link> {
    let span_context = context.span().span_context().clone();
    if span_context.is_valid() {
        vec![Link::with_context(span_context)]
    } else {
        Vec::new()
    }
}
