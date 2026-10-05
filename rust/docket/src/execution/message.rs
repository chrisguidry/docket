//! The fields of a task's message, as the shared Lua scripts move it between
//! the stream, the queue's parked hashes, and the concurrency waiter streams.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::error::{Error, Result};
use crate::wire::{iso, parse_iso};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Message {
    pub key: String,
    pub when: DateTime<Utc>,
    pub function: String,
    /// The task's arguments as JSON text.
    pub args: String,
    pub attempt: u32,
    pub generation: i64,
}

impl Message {
    /// The fields in the order the scripts expect.  `kwargs` exists because
    /// the scripts copy it into the run state, and Lua cannot write a missing
    /// value.  pydocket keeps keyword arguments there; docket-rs has none.
    pub fn fields(&self) -> Vec<(String, Vec<u8>)> {
        [
            ("key", self.key.clone()),
            ("when", iso(self.when)),
            ("function", self.function.clone()),
            ("args", self.args.clone()),
            ("kwargs", "{}".to_owned()),
            ("attempt", self.attempt.to_string()),
            ("generation", self.generation.to_string()),
        ]
        .into_iter()
        .map(|(field, value)| (field.to_owned(), value.into_bytes()))
        .collect()
    }

    pub fn from_fields(fields: &HashMap<String, Vec<u8>>) -> Result<Self> {
        let text = |field: &str| -> Result<String> {
            fields
                .get(field)
                .map(|value| String::from_utf8_lossy(value).into_owned())
                .ok_or_else(|| Error::Invalid(format!("a task message has no {field} field")))
        };
        let number = |field: &str| -> Result<i64> {
            text(field)?
                .parse()
                .map_err(|_| Error::Invalid(format!("a task message's {field} is not a number")))
        };
        let when = text("when")?;
        Ok(Self {
            key: text("key")?,
            when: parse_iso(&when)
                .ok_or_else(|| Error::Invalid(format!("a task message's when is {when}")))?,
            function: text("function")?,
            args: text("args")?,
            attempt: u32::try_from(number("attempt")?)
                .map_err(|_| Error::Invalid("a task message's attempt is out of range".into()))?,
            generation: if fields.contains_key("generation") {
                number("generation")?
            } else {
                0
            },
        })
    }
}

#[cfg(test)]
mod tests;
