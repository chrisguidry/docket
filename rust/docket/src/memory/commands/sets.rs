//! Sets: SADD, SREM, and SMEMBERS.

use super::args::{Args, Result};
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

pub(super) fn sadd(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    Ok(Reply::Integer(store.sadd(key, args.rest()?)?))
}

pub(super) fn srem(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    Ok(Reply::Integer(store.srem(&key, &args.rest()?)?))
}

pub(super) fn smembers(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(Reply::bulks(store.smembers(&key)?))
}
