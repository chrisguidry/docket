//! Consumer groups: XGROUP, XACK, XPENDING, XAUTOCLAIM, XCLAIM, and XINFO
//! GROUPS.

use std::time::{SystemTime, UNIX_EPOCH};

use super::args::{Args, Error, Result, format_stream_id, parse_stream_id};
use super::streams::{bound, entries_reply, ids_reply};
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

fn unknown_subcommand(command: &str, subcommand: &str) -> Error {
    Error::new(format!(
        "ERR unknown subcommand '{subcommand}'. Try {command} HELP."
    ))
}

pub(super) fn xgroup(store: &Store, args: &mut Args) -> Result<Reply> {
    let subcommand = args.next_str()?;
    match subcommand.to_ascii_uppercase().as_str() {
        "CREATE" => {
            let key = args.next()?;
            let group = args.next()?;
            // `Store` reads the largest possible ID as `$`, the stream's last ID.
            let id = match args.next()?.as_ref() {
                b"$" => (u64::MAX, u64::MAX),
                id => parse_stream_id(id).ok_or_else(Error::invalid_stream_id)?,
            };
            let mut mkstream = false;
            while let Some(option) = args.next_keyword() {
                match option.as_str() {
                    "MKSTREAM" => mkstream = true,
                    "ENTRIESREAD" => {
                        args.next_int::<i64>()?;
                    }
                    _ => return Err(Error::syntax()),
                }
            }
            store.xgroup_create(&key, group, id, mkstream)?;
            Ok(Reply::ok())
        }
        "DESTROY" => {
            let key = args.next()?;
            let group = args.next()?;
            args.end()?;
            Ok(Reply::Integer(store.xgroup_destroy(&key, &group)?.into()))
        }
        _ => Err(unknown_subcommand("XGROUP", &subcommand)),
    }
}

pub(super) fn xack(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let group = args.next()?;
    let ids = args
        .rest()?
        .iter()
        .map(|id| parse_stream_id(id).ok_or_else(Error::invalid_stream_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(Reply::Integer(store.xack(&key, &group, &ids)?))
}

pub(super) fn xpending(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let group = args.next()?;
    if args.is_empty() {
        return xpending_summary(store, &key, &group);
    }
    let mut min_idle = None;
    if args
        .peek()
        .is_some_and(|word| word.eq_ignore_ascii_case(b"IDLE"))
    {
        args.next_keyword();
        min_idle = Some(args.next_int()?);
    }
    let start = bound(&args.next()?, true)?;
    let end = bound(&args.next()?, false)?;
    let count = args.next_int()?;
    let consumer = args.remaining();
    if consumer.len() > 1 {
        return Err(Error::syntax());
    }
    let pending =
        store.xpending_range(&key, &group, start, end, count, consumer.first(), min_idle)?;
    Ok(Reply::Array(
        pending
            .into_iter()
            .map(|(id, consumer, idle, deliveries)| {
                Reply::Array(vec![
                    Reply::Bulk(format_stream_id(id)),
                    Reply::Bulk(consumer),
                    Reply::integer(idle),
                    Reply::integer(deliveries),
                ])
            })
            .collect(),
    ))
}

/// `[count, lowest id, highest id, [[consumer, count], ...]]`, with the
/// last three nil when nothing is pending.
fn xpending_summary(store: &Store, key: &bytes::Bytes, group: &bytes::Bytes) -> Result<Reply> {
    let (total, min, max, mut consumers) = store.xpending_summary(key, group)?;
    if total == 0 {
        return Ok(Reply::Array(vec![
            Reply::Integer(0),
            Reply::Nil,
            Reply::Nil,
            Reply::Nil,
        ]));
    }
    consumers.sort();
    Ok(Reply::Array(vec![
        Reply::integer(total),
        min.map(format_stream_id).into(),
        max.map(format_stream_id).into(),
        Reply::Array(
            consumers
                .into_iter()
                .map(|(consumer, count)| {
                    Reply::Array(vec![Reply::Bulk(consumer), Reply::bulk(count.to_string())])
                })
                .collect(),
        ),
    ]))
}

pub(super) fn xautoclaim(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let group = args.next()?;
    let consumer = args.next()?;
    let min_idle = args.next_int()?;
    let start = bound(&args.next()?, true)?;
    let (mut count, mut justid) = (None, false);
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "COUNT" => count = Some(args.next_int()?),
            "JUSTID" => justid = true,
            _ => return Err(Error::syntax()),
        }
    }
    let (next, claimed, deleted) =
        store.xautoclaim(&key, &group, consumer, min_idle, start, count)?;
    let claimed = if justid {
        ids_reply(claimed.into_iter().map(|(id, _)| id))
    } else {
        entries_reply(claimed)
    };
    Ok(Reply::Array(vec![
        Reply::Bulk(format_stream_id(next)),
        claimed,
        ids_reply(deleted),
    ]))
}

pub(super) fn xclaim(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let group = args.next()?;
    let consumer = args.next()?;
    let min_idle = args.next_int()?;
    let mut ids = Vec::new();
    while let Some(id) = args.peek().and_then(|id| parse_stream_id(id)) {
        ids.push(id);
        args.next_keyword();
    }
    if ids.is_empty() {
        return Err(args.wrong_number());
    }
    // Redis resets a claimed entry's idle time unless IDLE or TIME says otherwise.
    let (mut idle, mut retrycount, mut force, mut justid) = (0, None, false, false);
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "IDLE" => idle = args.next_int()?,
            "TIME" => idle = milliseconds_since(args.next_int()?),
            "RETRYCOUNT" => retrycount = Some(args.next_int()?),
            "FORCE" => force = true,
            "JUSTID" => justid = true,
            "LASTID" => {
                args.next_stream_id()?;
            }
            _ => return Err(Error::syntax()),
        }
    }
    let claimed = store.xclaim(
        &key,
        &group,
        consumer,
        min_idle,
        &ids,
        Some(idle),
        None,
        retrycount,
        force,
        justid,
    )?;
    Ok(if justid {
        ids_reply(claimed.into_iter().map(|(id, _)| id))
    } else {
        entries_reply(
            claimed
                .into_iter()
                .filter_map(|(id, fields)| fields.map(|fields| (id, fields)))
                .collect(),
        )
    })
}

fn milliseconds_since(unix_ms: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(now)
        .unwrap_or(u64::MAX)
        .saturating_sub(unix_ms)
}

pub(super) fn xinfo(store: &Store, args: &mut Args) -> Result<Reply> {
    let subcommand = args.next_str()?;
    if !subcommand.eq_ignore_ascii_case("GROUPS") {
        return Err(unknown_subcommand("XINFO", &subcommand));
    }
    let key = args.next()?;
    args.end()?;
    let mut groups = store.xinfo_groups(&key)?;
    groups.sort_by(|a, b| a.get("name").cmp(&b.get("name")));
    Ok(Reply::Array(
        groups
            .into_iter()
            .map(|mut info| {
                let mut field = |name: &str| info.remove(name).unwrap_or_default();
                let (name, consumers, pending, last) = (
                    field("name"),
                    field("consumers"),
                    field("pending"),
                    field("last-delivered-id"),
                );
                Reply::Array(vec![
                    Reply::bulk("name"),
                    Reply::bulk(name),
                    Reply::bulk("consumers"),
                    Reply::Integer(consumers.parse().unwrap_or_default()),
                    Reply::bulk("pending"),
                    Reply::Integer(pending.parse().unwrap_or_default()),
                    Reply::bulk("last-delivered-id"),
                    Reply::bulk(last),
                ])
            })
            .collect(),
    ))
}
