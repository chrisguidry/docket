//! Lua: EVAL, EVALSHA, and SCRIPT LOAD and EXISTS.

use bytes::Bytes;

use super::args::{Args, Error, Result};
use crate::memory::engine::scripting::RedisValue;
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

/// The reply for what a script returned or raised.
pub(super) fn reply(result: std::result::Result<RedisValue, String>) -> Reply {
    match result {
        Ok(value) => from_lua(value),
        Err(message) => Reply::Error(error_line(&message)),
    }
}

fn from_lua(value: RedisValue) -> Reply {
    match value {
        RedisValue::BulkString(bytes) => Reply::Bulk(bytes),
        RedisValue::Integer(number) => Reply::Integer(number),
        RedisValue::Array(items) => Reply::Array(items.into_iter().map(from_lua).collect()),
        RedisValue::Nil => Reply::Nil,
        RedisValue::Error(message) => Reply::Error(error_line(&message)),
        RedisValue::Status(text) => Reply::Status(text),
    }
}

/// Lua wraps a `redis.call` failure in its own text, such as
/// `runtime error: WRONGTYPE ...` plus a traceback.  The client maps the
/// error's first word to an error kind, so the reply has to start with the
/// Redis error code.
fn error_line(message: &str) -> String {
    let message = message
        .rsplit("runtime error: ")
        .next()
        .and_then(|cause| cause.lines().next())
        .unwrap_or_default();
    let code = message.split(' ').next().unwrap_or_default();
    if !code.is_empty() && code.bytes().all(|byte| byte.is_ascii_uppercase()) {
        message.to_owned()
    } else {
        format!("ERR {message}")
    }
}

fn keys_and_args(args: &mut Args) -> Result<(Vec<Bytes>, Vec<Bytes>)> {
    let count: i64 = args.next_int()?;
    let count =
        usize::try_from(count).map_err(|_| Error::new("ERR Number of keys can't be negative"))?;
    let mut rest = args.remaining();
    if count > rest.len() {
        return Err(Error::new(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let values = rest.split_off(count);
    Ok((rest, values))
}

pub(super) fn eval(store: &Store, args: &mut Args) -> Result<Reply> {
    let script = args.next_str()?;
    let (keys, values) = keys_and_args(args)?;
    Ok(reply(store.eval(&script, keys, values)))
}

pub(super) fn evalsha(store: &Store, args: &mut Args) -> Result<Reply> {
    let sha = args.next_str()?.to_ascii_lowercase();
    let (keys, values) = keys_and_args(args)?;
    Ok(reply(store.evalsha(&sha, keys, values)))
}

pub(super) fn script(store: &Store, args: &mut Args) -> Result<Reply> {
    let subcommand = args.next_str()?;
    match subcommand.to_ascii_uppercase().as_str() {
        "LOAD" => {
            let script = args.next_str()?;
            args.end()?;
            Ok(Reply::bulk(store.script_load(&script)))
        }
        "EXISTS" => {
            let shas = args
                .rest()?
                .iter()
                .map(|sha| String::from_utf8_lossy(sha).to_ascii_lowercase())
                .collect::<Vec<_>>();
            Ok(Reply::Array(
                store
                    .script_exists(&shas)
                    .into_iter()
                    .map(|exists| Reply::Integer(exists.into()))
                    .collect(),
            ))
        }
        _ => Err(Error::new(format!(
            "ERR unknown subcommand '{subcommand}'. Try SCRIPT HELP."
        ))),
    }
}
