//! Hashes: HSET, HGET, HGETALL, HDEL, HINCRBY, and HEXISTS.

use super::args::{Args, Result};
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

pub(super) fn hset(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    Ok(Reply::Integer(store.hset(key, args.pairs()?)?))
}

pub(super) fn hget(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let field = args.next()?;
    args.end()?;
    Ok(store.hget(&key, &field)?.into())
}

pub(super) fn hgetall(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(Reply::bulks(
        store
            .hgetall(&key)?
            .into_iter()
            .flat_map(|(field, value)| [field, value]),
    ))
}

pub(super) fn hdel(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    Ok(Reply::Integer(store.hdel(&key, &args.rest()?)?))
}

pub(super) fn hincrby(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let field = args.next()?;
    let increment = args.next_int()?;
    args.end()?;
    Ok(Reply::Integer(store.hincrby(key, field, increment)?))
}

pub(super) fn hexists(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let field = args.next()?;
    args.end()?;
    Ok(Reply::Integer(store.hexists(&key, &field)?.into()))
}
