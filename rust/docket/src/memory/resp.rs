//! RESP2, the wire format between redis-rs and the in-process server.
//!
//! Requests are arrays of bulk strings, which is all redis-rs sends.  Replies
//! use the RESP2 types only, so the client never has to negotiate RESP3.

use bytes::{BufMut, Bytes, BytesMut};

/// Redis refuses a request with more arguments than this.
const MAX_ARGUMENTS: usize = 1024 * 1024;

/// Redis refuses a bulk string longer than this.
const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;

/// A reply to one request, in the shapes RESP2 can carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Reply {
    Status(String),
    /// The whole error line, starting with its code, such as `ERR` or `NOSCRIPT`.
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    pub(crate) fn ok() -> Self {
        Self::Status("OK".to_owned())
    }

    pub(crate) fn bulk(value: impl Into<Bytes>) -> Self {
        Self::Bulk(value.into())
    }

    pub(crate) fn bulks<T: Into<Bytes>>(values: impl IntoIterator<Item = T>) -> Self {
        Self::Array(values.into_iter().map(Self::bulk).collect())
    }

    pub(crate) fn integer(value: impl TryInto<i64>) -> Self {
        Self::Integer(value.try_into().unwrap_or(i64::MAX))
    }

    pub(crate) fn encode(&self, out: &mut BytesMut) {
        match self {
            Self::Status(text) => line(out, b'+', text.as_bytes()),
            // An error is one line, so a multi-line Lua traceback has to be flattened.
            Self::Error(text) => line(out, b'-', text.replace(['\r', '\n'], " ").as_bytes()),
            Self::Integer(value) => line(out, b':', value.to_string().as_bytes()),
            Self::Bulk(value) => {
                line(out, b'$', value.len().to_string().as_bytes());
                out.put_slice(value);
                out.put_slice(b"\r\n");
            }
            Self::Nil => out.put_slice(b"$-1\r\n"),
            Self::Array(items) => {
                line(out, b'*', items.len().to_string().as_bytes());
                for item in items {
                    item.encode(out);
                }
            }
        }
    }
}

impl From<Option<Bytes>> for Reply {
    fn from(value: Option<Bytes>) -> Self {
        value.map_or(Self::Nil, Self::Bulk)
    }
}

fn line(out: &mut BytesMut, marker: u8, body: &[u8]) {
    out.put_u8(marker);
    out.put_slice(body);
    out.put_slice(b"\r\n");
}

/// The client sent bytes that are not a RESP request.  Redis answers with
/// this error and closes the connection, because it cannot find where the
/// next request starts.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProtocolError(pub String);

impl ProtocolError {
    pub(crate) fn reply(&self) -> Reply {
        Reply::Error(format!("ERR Protocol error: {}", self.0))
    }
}

/// Takes one whole request off the front of `buf`.  Returns `None` when
/// `buf` holds only part of a request, and leaves `buf` as it was.
pub(crate) fn parse_request(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    let Some((count, mut position)) = header(buf, 0, b'*', MAX_ARGUMENTS, "multibulk")? else {
        return Ok(None);
    };
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((length, start)) = header(buf, position, b'$', MAX_BULK_LENGTH, "bulk")? else {
            return Ok(None);
        };
        let end = start + length;
        let Some(terminator) = buf.get(end..end + 2) else {
            return Ok(None);
        };
        if terminator != b"\r\n" {
            return Err(ProtocolError("expected CRLF after bulk string".to_owned()));
        }
        ranges.push(start..end);
        position = end + 2;
    }
    let frame = buf.split_to(position).freeze();
    Ok(Some(
        ranges.into_iter().map(|range| frame.slice(range)).collect(),
    ))
}

/// Reads a `<marker><length>\r\n` line at `position`.  Returns the length and
/// the position just past the line.
fn header(
    buf: &[u8],
    position: usize,
    marker: u8,
    max: usize,
    what: &str,
) -> Result<Option<(usize, usize)>, ProtocolError> {
    let Some(&first) = buf.get(position) else {
        return Ok(None);
    };
    if first != marker {
        return Err(ProtocolError(format!(
            "expected '{}', got '{}'",
            char::from(marker),
            char::from(first)
        )));
    }
    let Some(end) = buf[position..].windows(2).position(|pair| pair == b"\r\n") else {
        return Ok(None);
    };
    std::str::from_utf8(&buf[position + 1..position + end])
        .ok()
        .and_then(|digits| digits.parse::<usize>().ok())
        .filter(|length| *length <= max)
        .map(|length| Some((length, position + end + 2)))
        .ok_or_else(|| ProtocolError(format!("invalid {what} length")))
}
