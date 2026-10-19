//! The arguments of `strike` and `restore`, as pydocket's CLI takes them.

use serde_json::Value;

use super::display::repr;
use crate::error::{Error, Result};
use crate::strikes::{Operator, Strike};

/// What `strike` and `restore` strike or restore: a task, a parameter of a
/// task or of every task, or both.
#[derive(clap::Args, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct StrikeArgs {
    /// The task, or `*` for every task.
    #[arg(default_value = "*")]
    pub function: String,

    /// The parameter, or `*` for the whole task.
    #[arg(default_value = "*")]
    pub parameter: String,

    /// The operator to compare the value against: ==, !=, >, >=, <, <=, or
    /// between.
    #[arg(default_value = "==", value_parser = parse_operator)]
    pub operator: Operator,

    /// The value to compare the parameter with.  Text is a string; a
    /// `str:`, `int:`, `float:`, `bool:`, or `json:` prefix gives the type,
    /// such as `int:7` or `json:[1,10]` for between.  Without a value, the
    /// parameter is compared with null.
    #[arg(value_parser = parse_value)]
    pub value: Option<Value>,
}

impl StrikeArgs {
    /// The strike the arguments describe.  It fails when both the task and
    /// the parameter are `*`, or when between has no `[low, high]` value.
    pub fn strike(&self) -> Result<Strike> {
        match (wildcard(&self.function), wildcard(&self.parameter)) {
            (None, None) => Err(Error::Invalid(
                "Must provide either a function and/or a parameter".to_owned(),
            )),
            (Some(function), None) => Ok(Strike::task_named(function)),
            (function, Some(parameter)) => {
                let value = self.value.clone().unwrap_or(Value::Null);
                if self.operator == Operator::Between
                    && value.as_array().is_none_or(|bounds| bounds.len() != 2)
                {
                    return Err(Error::Invalid(format!(
                        "between needs a value of two bounds, such as json:[1,10], not {value}"
                    )));
                }
                let strike = function.map_or_else(Strike::any_task, Strike::task_named);
                Ok(strike.field(parameter).when(self.operator, value))
            }
        }
    }

    /// The line pydocket prints for the strike, such as `Striking charge
    /// customer == 7`.
    pub(super) fn describe(&self, verb: &str) -> String {
        match wildcard(&self.parameter) {
            Some(parameter) => format!(
                "{verb} {} {parameter} {} {}\n",
                wildcard(&self.function).unwrap_or("(all tasks)"),
                self.operator.as_str(),
                repr(self.value.as_ref().unwrap_or(&Value::Null)),
            ),
            None => format!("{verb} {}\n", self.function),
        }
    }
}

/// pydocket's `handle_strike_wildcard`: `*` or nothing means any.
fn wildcard(text: &str) -> Option<&str> {
    match text {
        "" | "*" => None,
        text => Some(text),
    }
}

fn parse_operator(text: &str) -> std::result::Result<Operator, String> {
    Operator::parse(text)
        .ok_or_else(|| format!("{text} is not an operator: use ==, !=, >, >=, <, <=, or between"))
}

/// A strike's value from the command line, as JSON, which is what a
/// docket-rs worker reads.  Like pydocket's `interpret_python_value`, text
/// is a string, and a prefix before the first colon names its type.
/// pydocket takes any Python type there; docket-rs takes the ones that are
/// JSON.
pub(super) fn parse_value(text: &str) -> std::result::Result<Value, String> {
    let Some((hint, rest)) = text.split_once(':') else {
        return Ok(Value::String(text.to_owned()));
    };
    match hint.strip_prefix("builtins.").unwrap_or(hint) {
        "str" => Ok(Value::String(rest.to_owned())),
        "int" => rest
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| rest.parse::<u64>().map(Value::from))
            .map_err(|_| format!("{rest} is not an int")),
        "float" => rest
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| format!("{rest} is not a finite float")),
        "bool" => Ok(Value::Bool(rest.eq_ignore_ascii_case("true"))),
        "json" => {
            serde_json::from_str(rest).map_err(|error| format!("{rest} is not JSON: {error}"))
        }
        _ => Err(format!(
            "{hint} is not a type docket-rs can strike on: use str:, int:, float:, bool:, or \
             json:, and str: for text with a colon in it"
        )),
    }
}

#[cfg(test)]
mod tests;
