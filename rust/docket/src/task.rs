use serde::Serialize;
use serde::de::DeserializeOwned;

/// The arguments of a task, which also name it.
///
/// A producer needs only this type to schedule the task, and a worker
/// registers a handler for it.  The arguments and the output travel as JSON.
///
/// ```
/// use docket::Task;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Task)]
/// #[task(name = "charge", output = Receipt)]
/// pub struct Charge {
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
}

/// Implements [`Task`] for an argument type; see the trait for an example.
pub use docket_rs_macros::Task;
