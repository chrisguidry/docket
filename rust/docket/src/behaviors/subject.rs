//! What an admission limit counts: a whole task, or each value of one field
//! of its arguments.

use super::NotAdmitted;
use crate::context::Context;

/// The part of a limit's key that names what it counts: the task's name, or
/// `{field}:{value}`.  A string value goes in as it is, and any other value
/// as its JSON text.  A missing field counts as `null`, as pydocket counts
/// an omitted argument under its default of `None`.
pub(crate) fn subject(ctx: &Context, field: Option<&str>) -> String {
    let Some(field) = field else {
        return ctx.function().to_owned();
    };
    match ctx.args().get(field) {
        Some(serde_json::Value::String(value)) => format!("{field}:{value}"),
        Some(value) => format!("{field}:{value}"),
        None => format!("{field}:null"),
    }
}

/// The same subject, for a limit that refuses a task without the field, as
/// pydocket's `ConcurrencyLimit` does.
pub(crate) fn required_subject(ctx: &Context, field: Option<&str>) -> Result<String, NotAdmitted> {
    if let Some(field) = field
        && ctx.args().get(field).is_none()
    {
        return Err(NotAdmitted::failed(format!(
            "the {} task's arguments have no {field} field to limit by",
            ctx.function()
        )));
    }
    Ok(subject(ctx, field))
}
