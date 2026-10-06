//! What an admission limit counts: a whole task, or each value of one field
//! of its arguments.

use super::AdmissionBlocked;
use crate::context::Context;

/// The part of a limit's key that names what it counts: the task's name, or
/// `{field}:{value}`.  A string value goes in as it is, and any other value
/// as its JSON text.
pub(crate) fn subject(ctx: &Context, field: Option<&str>) -> Result<String, AdmissionBlocked> {
    let Some(field) = field else {
        return Ok(ctx.function().to_owned());
    };
    match ctx.args().get(field) {
        Some(serde_json::Value::String(value)) => Ok(format!("{field}:{value}")),
        Some(value) => Ok(format!("{field}:{value}")),
        None => Err(AdmissionBlocked::new(format!(
            "the {} task's arguments have no {field} field to limit by",
            ctx.function()
        ))
        .drop_task()),
    }
}
