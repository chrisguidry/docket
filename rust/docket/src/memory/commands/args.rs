//! Reading a command's arguments, with the error replies Redis gives for
//! missing or malformed ones.

use std::str::FromStr;

use bytes::Bytes;

use crate::memory::engine::store::StoreError;
use crate::memory::engine::streams::{self, StreamId};
use crate::memory::resp::Reply;

/// An error reply, as the whole line Redis would send.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Error(pub String);

impl Error {
    pub(crate) fn new(line: impl Into<String>) -> Self {
        Self(line.into())
    }

    pub(crate) fn syntax() -> Self {
        Self::new("ERR syntax error")
    }

    pub(crate) fn not_integer() -> Self {
        Self::new("ERR value is not an integer or out of range")
    }

    pub(crate) fn invalid_stream_id() -> Self {
        Self::new("ERR Invalid stream ID specified as stream command argument")
    }
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self(error.to_string())
    }
}

impl From<Error> for Reply {
    fn from(error: Error) -> Self {
        Reply::Error(error.0)
    }
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// The arguments after the command name, read front to back.
pub(crate) struct Args {
    command: String,
    items: std::vec::IntoIter<Bytes>,
}

impl Args {
    pub(crate) fn new(command: &str, items: Vec<Bytes>) -> Self {
        Self {
            command: command.to_ascii_lowercase(),
            items: items.into_iter(),
        }
    }

    /// The command's name, in lower case.
    pub(crate) fn name(&self) -> &str {
        &self.command
    }

    pub(crate) fn wrong_number(&self) -> Error {
        Error::new(format!(
            "ERR wrong number of arguments for '{}' command",
            self.name()
        ))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.len() == 0
    }

    pub(crate) fn peek(&self) -> Option<&Bytes> {
        self.items.as_slice().first()
    }

    pub(crate) fn next(&mut self) -> Result<Bytes> {
        self.items.next().ok_or_else(|| self.wrong_number())
    }

    pub(crate) fn next_str(&mut self) -> Result<String> {
        self.next()
            .map(|value| String::from_utf8_lossy(&value).into_owned())
    }

    /// The next argument, upper-cased, for matching option names.
    pub(crate) fn next_keyword(&mut self) -> Option<String> {
        self.items
            .next()
            .map(|value| String::from_utf8_lossy(&value).to_ascii_uppercase())
    }

    pub(crate) fn next_int<T: FromStr>(&mut self) -> Result<T> {
        parse(&self.next()?).ok_or_else(Error::not_integer)
    }

    pub(crate) fn next_stream_id(&mut self) -> Result<StreamId> {
        parse_stream_id(&self.next()?).ok_or_else(Error::invalid_stream_id)
    }

    /// Every argument left, which may be none.
    pub(crate) fn remaining(&mut self) -> Vec<Bytes> {
        self.items.by_ref().collect()
    }

    /// Every argument left, which must be at least one.
    pub(crate) fn rest(&mut self) -> Result<Vec<Bytes>> {
        if self.is_empty() {
            return Err(self.wrong_number());
        }
        Ok(self.remaining())
    }

    /// Every argument left, as field and value pairs, which must be at least one.
    pub(crate) fn pairs(&mut self) -> Result<Vec<(Bytes, Bytes)>> {
        if self.is_empty() || !self.items.len().is_multiple_of(2) {
            return Err(self.wrong_number());
        }
        let mut pairs = Vec::with_capacity(self.items.len() / 2);
        while let (Some(field), Some(value)) = (self.items.next(), self.items.next()) {
            pairs.push((field, value));
        }
        Ok(pairs)
    }

    /// Fails when arguments are left over that the command did not read.
    pub(crate) fn end(&self) -> Result<()> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(Error::syntax())
        }
    }
}

pub(crate) fn parse<T: FromStr>(value: &[u8]) -> Option<T> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Parses `ms-seq`, or a bare `ms`, which means `ms-0`.
pub(crate) fn parse_stream_id(value: &[u8]) -> Option<StreamId> {
    let text = std::str::from_utf8(value).ok()?;
    match text.split_once('-') {
        Some((ms, seq)) => Some((ms.parse().ok()?, seq.parse().ok()?)),
        None => Some((text.parse().ok()?, 0)),
    }
}

pub(crate) fn format_stream_id(id: StreamId) -> Bytes {
    Bytes::from(streams::format_stream_id(id))
}
