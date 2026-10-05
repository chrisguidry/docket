//! XREAD and XREADGROUP, which may wait for entries.  Parsing happens here;
//! the waiting happens in the session, which can also watch for the client
//! going away.

use std::time::Duration;

use bytes::Bytes;

use super::args::{Args, Error, Result, parse, parse_stream_id};
use super::streams::{Entry, entries_reply};
use crate::memory::engine::store::Store;
use crate::memory::engine::streams::{self, StreamId};
use crate::memory::resp::Reply;

/// How long a read waits for entries when there are none yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Block {
    Never,
    For(Duration),
    Forever,
}

/// A parsed stream read, ready to try as many times as it has to.
pub(crate) struct Read {
    pub(crate) block: Block,
    query: Query,
}

enum Query {
    Streams {
        keys: Vec<Bytes>,
        ids: Vec<StreamId>,
        count: Option<usize>,
    },
    Group {
        group: Bytes,
        consumer: Bytes,
        keys: Vec<Bytes>,
        ids: Vec<String>,
        count: Option<usize>,
    },
}

impl Read {
    /// Tries the read once.  `None` means it found nothing new, and a
    /// blocking read should wait and try again.
    pub(crate) fn attempt(&self, store: &Store) -> Option<Reply> {
        let (result, history) = match &self.query {
            Query::Streams { keys, ids, count } => (store.xread(keys, ids, *count), false),
            Query::Group {
                group,
                consumer,
                keys,
                ids,
                count,
            } => (
                store.xreadgroup(group, consumer, keys, ids, *count),
                // Reading a consumer's own pending entries never waits.
                ids.iter().any(|id| id != ">"),
            ),
        };
        match result {
            Err(error) => Some(Error::from(error).into()),
            Ok(streams) if streams.is_empty() && !history => None,
            Ok(streams) => Some(streams_reply(streams)),
        }
    }
}

/// `[[key, [[id, [field, value, ...]], ...]], ...]`
fn streams_reply(streams: Vec<(Bytes, Vec<Entry>)>) -> Reply {
    Reply::Array(
        streams
            .into_iter()
            .map(|(key, entries)| Reply::Array(vec![Reply::Bulk(key), entries_reply(entries)]))
            .collect(),
    )
}

struct Options {
    count: Option<usize>,
    block: Block,
    keys: Vec<Bytes>,
    ids: Vec<Bytes>,
}

/// Reads `[COUNT n] [BLOCK ms] [NOACK] STREAMS key... id...`.
fn options(args: &mut Args, command: &str, any_id: &str) -> Result<Options> {
    let (mut count, mut block) = (None, Block::Never);
    loop {
        match args.next_keyword().ok_or_else(Error::syntax)?.as_str() {
            // COUNT 0 means no limit, as in Redis.
            "COUNT" => count = Some(args.next_int::<usize>()?).filter(|count| *count > 0),
            "BLOCK" => block = timeout(&args.next()?)?,
            "NOACK" if command == "xreadgroup" => {}
            "STREAMS" => break,
            _ => return Err(Error::syntax()),
        }
    }
    let mut keys = args.remaining();
    if keys.is_empty() || !keys.len().is_multiple_of(2) {
        return Err(Error::new(format!(
            "ERR Unbalanced '{command}' list of streams: for each stream key an ID or '{any_id}' must be specified."
        )));
    }
    let ids = keys.split_off(keys.len() / 2);
    Ok(Options {
        count,
        block,
        keys,
        ids,
    })
}

fn timeout(value: &[u8]) -> Result<Block> {
    let milliseconds = parse::<i64>(value)
        .ok_or_else(|| Error::new("ERR timeout is not an integer or out of range"))?;
    match u64::try_from(milliseconds) {
        Ok(0) => Ok(Block::Forever),
        Ok(milliseconds) => Ok(Block::For(Duration::from_millis(milliseconds))),
        Err(_) => Err(Error::new("ERR timeout is negative")),
    }
}

pub(super) fn xread(store: &Store, args: &mut Args) -> Result<Read> {
    let Options {
        count,
        block,
        keys,
        ids,
    } = options(args, "xread", "$")?;
    // `$` means the entries added after this call, so it is fixed now and
    // not on each try.
    let ids = keys
        .iter()
        .zip(&ids)
        .map(|(key, id)| match id.as_ref() {
            b"$" => Ok(store.stream_last_id(key).unwrap_or((0, 0))),
            id => parse_stream_id(id).ok_or_else(Error::invalid_stream_id),
        })
        .collect::<Result<_>>()?;
    Ok(Read {
        block,
        query: Query::Streams { keys, ids, count },
    })
}

pub(super) fn xreadgroup(_store: &Store, args: &mut Args) -> Result<Read> {
    if args.next_keyword().is_none_or(|word| word != "GROUP") {
        return Err(Error::syntax());
    }
    let group = args.next()?;
    let consumer = args.next()?;
    let Options {
        count,
        block,
        keys,
        ids,
    } = options(args, "xreadgroup", ">")?;
    let ids = ids
        .iter()
        .map(|id| match id.as_ref() {
            b">" => Ok(">".to_owned()),
            id => parse_stream_id(id)
                .map(streams::format_stream_id)
                .ok_or_else(Error::invalid_stream_id),
        })
        .collect::<Result<_>>()?;
    Ok(Read {
        block,
        query: Query::Group {
            group,
            consumer,
            keys,
            ids,
            count,
        },
    })
}
