//! Sorted sets: ZADD, ZREM, ZCARD, ZCOUNT, ZSCORE, ZRANGE, ZRANGEBYSCORE,
//! and ZREMRANGEBYSCORE.

use bytes::Bytes;

use super::args::{Args, Error, Result, parse};
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

pub(super) fn zadd(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let (mut nx, mut xx, mut gt, mut lt, mut ch) = (false, false, false, false, false);
    loop {
        let flag = args
            .peek()
            .map(|value| String::from_utf8_lossy(value).to_ascii_uppercase());
        match flag.as_deref() {
            Some("NX") => nx = true,
            Some("XX") => xx = true,
            Some("GT") => gt = true,
            Some("LT") => lt = true,
            Some("CH") => ch = true,
            _ => break,
        }
        args.next_keyword();
    }
    if nx && xx {
        return Err(Error::new(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if (gt && lt) || (nx && (gt || lt)) {
        return Err(Error::new(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    let rest = args.rest()?;
    let (pairs, odd) = rest.as_chunks::<2>();
    if !odd.is_empty() {
        return Err(Error::syntax());
    }
    let members = pairs
        .iter()
        .map(|[score_text, member]| Ok((score(score_text)?, member.clone())))
        .collect::<Result<Vec<_>>>()?;
    Ok(Reply::Integer(
        store.zadd(key, members, nx, xx, gt, lt, ch)?,
    ))
}

fn score(value: &[u8]) -> Result<f64> {
    parse::<f64>(value)
        .filter(|score| !score.is_nan())
        .ok_or_else(|| Error::new("ERR value is not a valid float"))
}

/// Rust prints floats the way Redis does for the common cases: `3` for 3.0,
/// `1.5`, `inf`, and `-inf`.
fn format_score(score: f64) -> Bytes {
    Bytes::from(score.to_string())
}

/// A ZRANGEBYSCORE-style bound.  `Store` takes inclusive bounds only, so an
/// exclusive bound such as `(5` becomes the next float past 5.
fn bound(value: &[u8], low: bool) -> Result<f64> {
    let (exclusive, number) = match value.strip_prefix(b"(") {
        Some(number) => (true, number),
        None => (false, value),
    };
    let number = parse::<f64>(number)
        .filter(|number| !number.is_nan())
        .ok_or_else(|| Error::new("ERR min or max is not a float"))?;
    Ok(match (exclusive, low) {
        (false, _) => number,
        (true, true) => number.next_up(),
        (true, false) => number.next_down(),
    })
}

fn range_bounds(args: &mut Args) -> Result<(f64, f64)> {
    Ok((bound(&args.next()?, true)?, bound(&args.next()?, false)?))
}

fn members_reply(members: Vec<(Bytes, Option<f64>)>) -> Reply {
    Reply::bulks(
        members
            .into_iter()
            .flat_map(|(member, score)| std::iter::once(member).chain(score.map(format_score))),
    )
}

pub(super) fn zrem(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    Ok(Reply::Integer(store.zrem(&key, &args.rest()?)?))
}

pub(super) fn zcard(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    args.end()?;
    Ok(Reply::Integer(store.zcard(&key)?))
}

pub(super) fn zcount(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let (min, max) = range_bounds(args)?;
    args.end()?;
    Ok(Reply::Integer(store.zcount(&key, min, max)?))
}

pub(super) fn zscore(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let member = args.next()?;
    args.end()?;
    Ok(store.zscore(&key, &member)?.map(format_score).into())
}

pub(super) fn zrange(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let start = args.next_int()?;
    let stop = args.next_int()?;
    let mut withscores = false;
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "WITHSCORES" => withscores = true,
            _ => return Err(Error::syntax()),
        }
    }
    Ok(members_reply(store.zrange(&key, start, stop, withscores)?))
}

pub(super) fn zrangebyscore(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let (min, max) = range_bounds(args)?;
    let (mut withscores, mut offset, mut count) = (false, 0, usize::MAX);
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                // A negative offset selects nothing and a negative count selects everything.
                offset = usize::try_from(args.next_int::<i64>()?).unwrap_or(usize::MAX);
                count = usize::try_from(args.next_int::<i64>()?).unwrap_or(usize::MAX);
            }
            _ => return Err(Error::syntax()),
        }
    }
    let members = store.zrangebyscore(&key, min, max, withscores)?;
    Ok(members_reply(
        members.into_iter().skip(offset).take(count).collect(),
    ))
}

pub(super) fn zremrangebyscore(store: &Store, args: &mut Args) -> Result<Reply> {
    let key = args.next()?;
    let (min, max) = range_bounds(args)?;
    args.end()?;
    Ok(Reply::Integer(store.zremrangebyscore(&key, min, max)?))
}
