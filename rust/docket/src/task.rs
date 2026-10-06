use serde::Serialize;
use serde::de::DeserializeOwned;

/// The arguments of a task, which also name it.
///
/// A producer needs only this type to schedule the task, and a worker
/// registers a handler for it.  The arguments and the output travel as JSON.
///
/// A worker's log lines show a field's value only when the field is marked
/// `#[task(logged)]`, or its length with `#[task(logged(length_only))]`, the
/// way pydocket's `Logged` marks a parameter.  The others show as `...`.
///
/// ```
/// use docket::Task;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Task)]
/// #[task(name = "charge", output = Receipt)]
/// pub struct Charge {
///     #[task(logged)]
///     pub customer: u64,
///     pub cents: u64,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// pub struct Receipt {
///     pub id: String,
/// }
///
/// assert_eq!(Charge::NAME, "charge");
/// ```
pub trait Task: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The task's stable name.  Queued tasks refer to it, so it must not
    /// change when the type is renamed.
    const NAME: &'static str;

    /// What the task's handler returns.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The named fields of the arguments, in order, and how a log line shows
    /// each one.  `#[derive(Task)]` fills it in.
    const FIELDS: &'static [TaskField] = &[];
}

/// One field of a task's arguments, and how a log line shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskField {
    /// The field's name in the arguments' JSON.
    pub name: &'static str,
    /// How a log line shows the field.
    pub logged: Logged,
}

/// How a log line shows one field of a task's arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Logged {
    /// As `...`, the default, so that no argument reaches a log by accident.
    Hidden,
    /// As its JSON value.
    Value,
    /// As the length of a list, a map, or a string.
    Length,
}

/// Implements [`Task`] for an argument type; see the trait for an example.
pub use docket_rs_macros::Task;
