//! The commands the in-process server answers, by data type, each one a
//! translation from Redis arguments to a typed `Store` call and back.

mod args;
mod hashes;
mod keys;
mod scripting;
mod server;
mod sets;
mod sorted_sets;
mod stream_groups;
mod stream_reads;
mod streams;

pub(crate) use args::{Args, Result};
pub(crate) use stream_reads::{Block, Read};

use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

/// How a command runs.
#[derive(Clone, Copy)]
pub(crate) enum Command {
    /// Answers at once.
    Reply(fn(&Store, &mut Args) -> Result<Reply>),
    /// Reads streams, and may wait for entries to arrive.
    Read(fn(&Store, &mut Args) -> Result<Read>),
}

/// The command for an upper-case command name.
pub(crate) fn lookup(name: &str) -> Option<Command> {
    use Command::{Read, Reply};
    Some(match name {
        "GET" => Reply(keys::get),
        "SET" => Reply(keys::set),
        "SETEX" => Reply(keys::setex),
        "PSETEX" => Reply(keys::psetex),
        "MGET" => Reply(keys::mget),
        "DEL" => Reply(keys::del),
        "EXISTS" => Reply(keys::exists),
        "EXPIRE" => Reply(keys::expire),
        "PEXPIRE" => Reply(keys::pexpire),
        "TTL" => Reply(keys::ttl),
        "PTTL" => Reply(keys::pttl),
        "HSET" => Reply(hashes::hset),
        "HGET" => Reply(hashes::hget),
        "HGETALL" => Reply(hashes::hgetall),
        "HDEL" => Reply(hashes::hdel),
        "HINCRBY" => Reply(hashes::hincrby),
        "HEXISTS" => Reply(hashes::hexists),
        "SADD" => Reply(sets::sadd),
        "SREM" => Reply(sets::srem),
        "SMEMBERS" => Reply(sets::smembers),
        "ZADD" => Reply(sorted_sets::zadd),
        "ZREM" => Reply(sorted_sets::zrem),
        "ZCARD" => Reply(sorted_sets::zcard),
        "ZCOUNT" => Reply(sorted_sets::zcount),
        "ZSCORE" => Reply(sorted_sets::zscore),
        "ZRANGE" => Reply(sorted_sets::zrange),
        "ZRANGEBYSCORE" => Reply(sorted_sets::zrangebyscore),
        "ZREMRANGEBYSCORE" => Reply(sorted_sets::zremrangebyscore),
        "XADD" => Reply(streams::xadd),
        "XLEN" => Reply(streams::xlen),
        "XRANGE" => Reply(streams::xrange),
        "XREVRANGE" => Reply(streams::xrevrange),
        "XDEL" => Reply(streams::xdel),
        "XTRIM" => Reply(streams::xtrim),
        "XGROUP" => Reply(stream_groups::xgroup),
        "XACK" => Reply(stream_groups::xack),
        "XPENDING" => Reply(stream_groups::xpending),
        "XAUTOCLAIM" => Reply(stream_groups::xautoclaim),
        "XCLAIM" => Reply(stream_groups::xclaim),
        "XINFO" => Reply(stream_groups::xinfo),
        "XREAD" => Read(stream_reads::xread),
        "XREADGROUP" => Read(stream_reads::xreadgroup),
        "EVAL" => Reply(scripting::eval),
        "EVALSHA" => Reply(scripting::evalsha),
        "SCRIPT" => Reply(scripting::script),
        "PUBLISH" => Reply(server::publish),
        "PING" => Reply(server::ping),
        "ECHO" => Reply(server::echo),
        "TIME" => Reply(server::time),
        "CLIENT" => Reply(server::client),
        "SELECT" => Reply(server::select),
        "INFO" => Reply(server::info),
        "FLUSHALL" => Reply(server::flushall),
        _ => return None,
    })
}
