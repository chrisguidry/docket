//! Commands about the server and the connection rather than any key, and
//! PUBLISH, which needs no subscription state of its own.

use std::time::{SystemTime, UNIX_EPOCH};

use super::args::{Args, Error, Result};
use crate::memory::engine::store::Store;
use crate::memory::resp::Reply;

/// Clients read the version to decide which commands they may send, so it
/// names the Redis whose commands the server mimics.
const INFO: &str = "# Server\r\nredis_version:7.4.0\r\n";

pub(super) fn publish(store: &Store, args: &mut Args) -> Result<Reply> {
    let channel = args.next()?;
    let message = args.next()?;
    args.end()?;
    Ok(Reply::Integer(store.publish(channel, message)))
}

pub(super) fn ping(_store: &Store, args: &mut Args) -> Result<Reply> {
    let message = args.peek().cloned();
    if args.remaining().len() > 1 {
        return Err(args.wrong_number());
    }
    Ok(message.map_or_else(|| Reply::Status("PONG".to_owned()), Reply::Bulk))
}

pub(super) fn echo(_store: &Store, args: &mut Args) -> Result<Reply> {
    let message = args.next()?;
    args.end()?;
    Ok(Reply::Bulk(message))
}

pub(super) fn time(_store: &Store, args: &mut Args) -> Result<Reply> {
    args.end()?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(Reply::bulks([
        now.as_secs().to_string(),
        now.subsec_micros().to_string(),
    ]))
}

/// Connection names and library details are accepted and forgotten, because
/// nothing in an in-process server lists its clients.
pub(super) fn client(_store: &Store, args: &mut Args) -> Result<Reply> {
    let subcommand = args.next_str()?;
    let (count, reply) = match subcommand.to_ascii_uppercase().as_str() {
        "SETNAME" => (1, Reply::ok()),
        "SETINFO" => (2, Reply::ok()),
        "GETNAME" => (0, Reply::Nil),
        _ => {
            return Err(Error::new(format!(
                "ERR unknown subcommand '{subcommand}'. Try CLIENT HELP."
            )));
        }
    };
    if args.remaining().len() != count {
        return Err(Error::new(format!(
            "ERR wrong number of arguments for 'client|{}' command",
            subcommand.to_ascii_lowercase()
        )));
    }
    Ok(reply)
}

/// The server has one database, so SELECT accepts only 0.
pub(super) fn select(_store: &Store, args: &mut Args) -> Result<Reply> {
    let index: i64 = args.next_int()?;
    args.end()?;
    if index == 0 {
        Ok(Reply::ok())
    } else {
        Err(Error::new("ERR DB index is out of range"))
    }
}

#[expect(clippy::unnecessary_wraps, reason = "every command has this signature")]
pub(super) fn info(_store: &Store, args: &mut Args) -> Result<Reply> {
    args.remaining();
    Ok(Reply::bulk(INFO))
}

pub(super) fn flushall(store: &Store, args: &mut Args) -> Result<Reply> {
    match args.next_keyword().as_deref() {
        None | Some("SYNC" | "ASYNC") => args.end()?,
        Some(_) => return Err(Error::syntax()),
    }
    store.delete(&store.keys(b"*"));
    Ok(Reply::ok())
}
