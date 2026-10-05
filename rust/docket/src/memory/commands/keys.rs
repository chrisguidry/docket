//! Strings and the commands that work on any key: SET, SETEX, PSETEX, GET,
//! MGET, DEL, EXISTS, and the expiry commands.

use std::time::Duration;

use bytes::Bytes;

use super::args::{Args, Error, Result};
use super::scripting;
use crate::memory::engine::store::{PersistableStore, Store};
use crate::memory::resp::Reply;

pub(super) fn get(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(store.get(&key)?.into())
}

pub(super) fn set(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let value = args.next()?;
    let (mut nx, mut xx, mut ttl) = (false, false, None);
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "NX" if !xx => nx = true,
            "XX" if !nx => xx = true,
            "EX" if ttl.is_none() => ttl = Some(Duration::from_secs(expire_time(args)?)),
            "PX" if ttl.is_none() => ttl = Some(Duration::from_millis(expire_time(args)?)),
            _ => return Err(Error::syntax()),
        }
    }
    if store.set(key, value, ttl, nx, xx) {
        Ok(Reply::ok())
    } else {
        Ok(Reply::Nil)
    }
}

fn expire_time(args: &mut Args) -> Result<u64> {
    let time: i64 = args.next_int()?;
    u64::try_from(time)
        .ok()
        .filter(|time| *time > 0)
        .ok_or_else(|| {
            Error::new(format!(
                "ERR invalid expire time in '{}' command",
                args.name()
            ))
        })
}

pub(super) fn setex(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let ttl = Duration::from_secs(expire_time(args)?);
    let value = args.next()?;
    args.end()?;
    store.set(key, value, Some(ttl), false, false);
    Ok(Reply::ok())
}

pub(super) fn psetex(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let ttl = Duration::from_millis(expire_time(args)?);
    let value = args.next()?;
    args.end()?;
    store.set(key, value, Some(ttl), false, false);
    Ok(Reply::ok())
}

pub(super) fn mget(store: &Store, args: &mut Args) -> Result<Reply> {
    let keys = args.rest()?;
    Ok(Reply::Array(
        store.mget(&keys).into_iter().map(Reply::from).collect(),
    ))
}

pub(super) fn del(store: &Store, args: &mut Args) -> Result<Reply> {
    Ok(Reply::Integer(store.delete(&args.rest()?)))
}

pub(super) fn exists(store: &Store, args: &mut Args) -> Result<Reply> {
    Ok(Reply::Integer(store.exists(&args.rest()?)))
}

/// Redis deletes a key whose new timeout is zero or negative.
fn expire_now(store: &Store, key: Bytes) -> Reply {
    Reply::Integer(store.delete(&[key]))
}

pub(super) fn expire(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let seconds: i64 = args.next_int()?;
    args.end()?;
    Ok(match u64::try_from(seconds) {
        Ok(seconds) if seconds > 0 => Reply::Integer(store.expire(&key, seconds).into()),
        _ => expire_now(store, key),
    })
}

/// `Store` sets timeouts in whole seconds only, and its Lua dispatcher is the
/// one place that sets them in milliseconds.
const PEXPIRE_SCRIPT: &str = "return redis.call('PEXPIRE', KEYS[1], ARGV[1])";

pub(super) fn pexpire(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let milliseconds: i64 = args.next_int()?;
    args.end()?;
    if milliseconds <= 0 {
        return Ok(expire_now(store, key));
    }
    let milliseconds = Bytes::from(milliseconds.to_string());
    Ok(scripting::reply(store.eval(
        PEXPIRE_SCRIPT,
        vec![key],
        vec![milliseconds],
    )))
}

pub(super) fn ttl(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(Reply::Integer(store.ttl(&key)))
}

/// `Store` reports remaining time in whole seconds only.  Its snapshot is the
/// one view with milliseconds, and it copies the whole keyspace, so PTTL
/// costs time in proportion to the number of keys.
pub(super) fn pttl(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    let snapshot = PersistableStore::from_store(store);
    let entry = snapshot.entries.into_iter().find(|(name, _)| *name == key);
    Ok(Reply::Integer(entry.map_or(-2, |(_, entry)| {
        entry
            .ttl_remaining_ms
            .map_or(-1, |remaining| i64::try_from(remaining).unwrap_or(i64::MAX))
    })))
}
