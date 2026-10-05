//! Strikes block a task, or the calls whose arguments match a condition.
//! They apply when a task is added and again before it runs.

mod monitor;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::task::Task;

pub(crate) use monitor::{Monitor, encode as instruction};

/// A comparison in a strike condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operator {
    /// `==`
    Equal,
    /// `!=`
    NotEqual,
    /// `>`
    GreaterThan,
    /// `>=`
    GreaterOrEqual,
    /// `<`
    LessThan,
    /// `<=`
    LessOrEqual,
    /// `between`, inclusive at both ends.
    Between,
}

impl Operator {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "==",
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::GreaterOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessOrEqual => "<=",
            Self::Between => "between",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "==" => Self::Equal,
            "!=" => Self::NotEqual,
            ">" => Self::GreaterThan,
            ">=" => Self::GreaterOrEqual,
            "<" => Self::LessThan,
            "<=" => Self::LessOrEqual,
            "between" => Self::Between,
            _ => return None,
        })
    }

    /// Whether `actual` meets the condition.  Values of different kinds,
    /// such as a number and a string, never meet an ordering condition.
    fn matches(self, actual: &Value, expected: &Value) -> bool {
        match self {
            Self::Equal => actual == expected,
            Self::NotEqual => actual != expected,
            Self::GreaterThan => compare(actual, expected) == Some(Ordering::Greater),
            Self::GreaterOrEqual => compare(actual, expected).is_some_and(Ordering::is_ge),
            Self::LessThan => compare(actual, expected) == Some(Ordering::Less),
            Self::LessOrEqual => compare(actual, expected).is_some_and(Ordering::is_le),
            Self::Between => match expected.as_array().map(Vec::as_slice) {
                Some([low, high]) => {
                    compare(actual, low).is_some_and(Ordering::is_ge)
                        && compare(actual, high).is_some_and(Ordering::is_le)
                }
                _ => false,
            },
        }
    }
}

fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64()?.partial_cmp(&right.as_f64()?),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

/// A strike: a whole task, or the calls whose argument field meets a
/// condition, of one task or of every task.
///
/// ```
/// # use docket::{Strike, Task};
/// # #[derive(serde::Serialize, serde::Deserialize, Task)]
/// # #[task(name = "charge")]
/// # struct Charge { customer: u64 }
/// Strike::task::<Charge>();                          // every call
/// Strike::task::<Charge>().field("customer").eq(7);  // one customer
/// Strike::any_task().field("customer").ge(90_000);   // any task with that field
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Strike {
    pub(crate) function: Option<String>,
    pub(crate) condition: Option<Condition>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Condition {
    pub field: String,
    pub operator: Operator,
    pub value: Value,
}

impl Strike {
    /// Every call of a task.
    #[must_use]
    pub fn task<T: Task>() -> Self {
        Self::task_named(T::NAME)
    }

    /// Every call of the task with this name, for tools that do not have the
    /// task's type.
    pub fn task_named(name: impl Into<String>) -> Self {
        Self {
            function: Some(name.into()),
            condition: None,
        }
    }

    /// Every task, narrowed with [`Strike::field`].
    #[must_use]
    pub fn any_task() -> Self {
        Self {
            function: None,
            condition: None,
        }
    }

    /// Narrows the strike to calls whose argument field meets a condition.
    pub fn field(self, field: impl Into<String>) -> StrikeField {
        StrikeField {
            strike: self,
            field: field.into(),
        }
    }
}

/// A strike waiting for the condition on its field.
#[derive(Clone, Debug)]
pub struct StrikeField {
    strike: Strike,
    field: String,
}

impl StrikeField {
    /// Strikes calls whose field meets `operator` with `value`.
    pub fn when(self, operator: Operator, value: impl Into<Value>) -> Strike {
        Strike {
            condition: Some(Condition {
                field: self.field,
                operator,
                value: value.into(),
            }),
            ..self.strike
        }
    }

    /// `field == value`
    pub fn eq(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::Equal, value)
    }

    /// `field != value`
    pub fn ne(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::NotEqual, value)
    }

    /// `field > value`
    pub fn gt(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::GreaterThan, value)
    }

    /// `field >= value`
    pub fn ge(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::GreaterOrEqual, value)
    }

    /// `field < value`
    pub fn lt(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::LessThan, value)
    }

    /// `field <= value`
    pub fn le(self, value: impl Into<Value>) -> Strike {
        self.when(Operator::LessOrEqual, value)
    }

    /// `low <= field <= high`
    pub fn between(self, low: impl Into<Value>, high: impl Into<Value>) -> Strike {
        self.when(Operator::Between, vec![low.into(), high.into()])
    }
}

/// The strikes in force, kept current by the docket's [`Monitor`].
#[derive(Default)]
pub(crate) struct Strikes {
    state: RwLock<State>,
}

#[derive(Default)]
struct State {
    whole_tasks: Vec<String>,
    /// Conditions by task name; the `None` entry applies to every task.
    conditions: HashMap<Option<String>, Vec<Condition>>,
}

impl Strikes {
    pub fn apply(&self, strike: &Strike, restore: bool) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (&strike.function, &strike.condition) {
            (Some(function), None) => {
                state.whole_tasks.retain(|task| task != function);
                if !restore {
                    state.whole_tasks.push(function.clone());
                }
            }
            (function, Some(condition)) => {
                let conditions = state.conditions.entry(function.clone()).or_default();
                conditions.retain(|existing| existing != condition);
                if !restore {
                    conditions.push(condition.clone());
                }
                if conditions.is_empty() {
                    state.conditions.remove(function);
                }
            }
            (None, None) => {}
        }
    }

    /// Whether a call of `function` with `args` is struck.
    pub fn is_struck(&self, function: &str, args: &Value) -> bool {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.whole_tasks.iter().any(|task| task == function) {
            return true;
        }
        let Some(fields) = args.as_object() else {
            return false;
        };
        [Some(function.to_owned()), None]
            .iter()
            .filter_map(|task| state.conditions.get(task))
            .flatten()
            .any(|condition| {
                fields
                    .get(&condition.field)
                    .is_some_and(|actual| condition.operator.matches(actual, &condition.value))
            })
    }
}

pub(crate) type SharedStrikes = Arc<Strikes>;

#[cfg(test)]
mod tests;
