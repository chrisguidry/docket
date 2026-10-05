//! Streams without consumer groups: XADD, XLEN, XRANGE, XREVRANGE, XDEL,
//! and XTRIM, plus the entry shapes every stream reply shares.

use std::collections::HashMap;

use bytes::Bytes;

use super::args::{Args, Error, Result, format_stream_id, parse, parse_stream_id};
use crate::memory::engine::store::Store;
use crate::memory::engine::streams::StreamId;
use crate::memory::resp::Reply;

pub(super) type Entry = (StreamId, HashMap<Bytes, Bytes>);

/// `[id, [field, value, ...]]`
pub(super) fn entry_reply((id, fields): Entry) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(format_stream_id(id)),
        Reply::bulks(fields.into_iter().flat_map(|(field, value)| [field, value])),
    ])
}

pub(super) fn entries_reply(entries: Vec<Entry>) -> Reply {
    Reply::Array(entries.into_iter().map(entry_reply).collect())
}

pub(super) fn ids_reply(ids: impl IntoIterator<Item = StreamId>) -> Reply {
    Reply::bulks(ids.into_iter().map(format_stream_id))
}

/// How to trim a stream: keep at most this many entries, or drop the entries
/// below this ID.
#[derive(Clone, Copy)]
enum Trim {
    MaxLen(usize),
    MinId(StreamId),
}

/// Reads the rest of `MAXLEN [=|~] n` or `MINID [=|~] id`.  Trimming is
/// always exact, so `~` means the same as `=`.
fn trim(args: &mut Args, strategy: &str) -> Result<Trim> {
    if matches!(args.peek().map(Bytes::as_ref), Some(b"=" | b"~")) {
        args.next_keyword();
    }
    if strategy == "MAXLEN" {
        args.next_int().map(Trim::MaxLen)
    } else {
        args.next_stream_id().map(Trim::MinId)
    }
}

fn apply_trim(store: &Store, key: &Bytes, trim: Trim) -> Result<usize> {
    Ok(match trim {
        Trim::MaxLen(length) => store.xtrim(key, Some(length), None)?,
        Trim::MinId(id) => store.xtrim(key, None, Some(id))?,
    })
}

pub(super) fn xadd(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let mut limit = None;
    let id = loop {
        let word = args.next_str()?;
        match word.to_ascii_uppercase().as_str() {
            strategy @ ("MAXLEN" | "MINID") => limit = Some(trim(args, strategy)?),
            "*" => break None,
            _ => {
                break Some(parse_stream_id(word.as_bytes()).ok_or_else(Error::invalid_stream_id)?);
            }
        }
    };
    let fields = args.pairs()?.into_iter().collect();
    if let Some(id) = id {
        check_explicit_id(store, &key, id)?;
    }
    let id = store.xadd(key.clone(), fields, id)?;
    if let Some(limit) = limit {
        // XADD has just made the key a stream, so trimming it cannot fail.
        apply_trim(store, &key, limit).ok();
    }
    Ok(Reply::Bulk(format_stream_id(id)))
}

/// `Store::xadd` reports a too-small explicit ID as a type error, so the ID
/// is checked here first to give Redis's message.
fn check_explicit_id(store: &Store, key: &Bytes, id: StreamId) -> Result<()> {
    if id == (0, 0) {
        return Err(Error::new(
            "ERR The ID specified in XADD must be greater than 0-0",
        ));
    }
    if store.stream_last_id(key).is_some_and(|last| id <= last) {
        return Err(Error::new(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }
    Ok(())
}

pub(super) fn xlen(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(Reply::integer(store.xlen(&key)?))
}

/// An XRANGE bound: `-`, `+`, `ms`, or `ms-seq`, with a leading `(` for an
/// exclusive bound.  A bare `ms` covers every sequence number in that
/// millisecond.
pub(super) fn bound(value: &[u8], low: bool) -> Result<StreamId> {
    let (exclusive, text) = match value.strip_prefix(b"(") {
        Some(text) => (true, text),
        None => (false, value),
    };
    let id = match text {
        b"-" => Some((0, 0)),
        b"+" => Some((u64::MAX, u64::MAX)),
        _ if text.contains(&b'-') => parse_stream_id(text),
        _ => parse::<u64>(text).map(|ms| (ms, if low { 0 } else { u64::MAX })),
    }
    .ok_or_else(Error::invalid_stream_id)?;
    match (exclusive, low) {
        (false, _) => Some(id),
        (true, true) => step_up(id),
        (true, false) => step_down(id),
    }
    .ok_or_else(|| {
        Error::new(if low {
            "ERR invalid start ID for the interval"
        } else {
            "ERR invalid end ID for the interval"
        })
    })
}

fn step_up((ms, seq): StreamId) -> Option<StreamId> {
    match seq.checked_add(1) {
        Some(seq) => Some((ms, seq)),
        None => Some((ms.checked_add(1)?, 0)),
    }
}

fn step_down((ms, seq): StreamId) -> Option<StreamId> {
    match seq.checked_sub(1) {
        Some(seq) => Some((ms, seq)),
        None => Some((ms.checked_sub(1)?, u64::MAX)),
    }
}

fn count_option(args: &mut Args) -> Result<usize> {
    match args.next_keyword().as_deref() {
        None => Ok(usize::MAX),
        Some("COUNT") => {
            let count = args.next_int()?;
            args.end()?;
            Ok(count)
        }
        Some(_) => Err(Error::syntax()),
    }
}

/// The entries between two inclusive bounds, oldest first.
fn range(store: &Store, key: &Bytes, min: StreamId, max: StreamId) -> Result<Vec<Entry>> {
    // The stream's ordered map panics on a range that ends before it starts.
    if min > max {
        return Ok(Vec::new());
    }
    Ok(store.xrange(key, min, max, None)?)
}

pub(super) fn xrange(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let min = bound(&args.next()?, true)?;
    let max = bound(&args.next()?, false)?;
    let count = count_option(args)?;
    let entries = range(store, &key, min, max)?;
    Ok(entries_reply(entries.into_iter().take(count).collect()))
}

pub(super) fn xrevrange(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let max = bound(&args.next()?, false)?;
    let min = bound(&args.next()?, true)?;
    let count = count_option(args)?;
    let entries = range(store, &key, min, max)?;
    Ok(entries_reply(
        entries.into_iter().rev().take(count).collect(),
    ))
}

pub(super) fn xdel(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let ids = args
        .rest()?
        .iter()
        .map(|id| parse_stream_id(id).ok_or_else(Error::invalid_stream_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(Reply::Integer(store.xdel(&key, &ids)?))
}

pub(super) fn xtrim(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let strategy = args.next_str()?.to_ascii_uppercase();
    if !matches!(strategy.as_str(), "MAXLEN" | "MINID") {
        return Err(Error::syntax());
    }
    let trim = trim(args, &strategy)?;
    args.end()?;
    Ok(Reply::integer(apply_trim(store, &key, trim)?))
}
