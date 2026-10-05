//! Tests that drive the in-process server through real redis-rs connections.
//!
//! Most tests run commands written as one line each and compare the replies
//! in a compact form: `ok` for OK, the text of any other status, `"text"`
//! for a bulk string, a bare number for an integer, `nil`, `[a, b]` for an
//! array, and `-CODE detail` for an error.

mod argument_errors;
mod hashes_and_sets;
mod keys;
mod pubsub;
mod registry;
mod resp;
mod scripting;
mod server_commands;
mod sorted_sets;
mod stream_groups;
mod stream_reads;
mod streams;
mod transactions;

use std::sync::atomic::{AtomicUsize, Ordering};

use redis::Value;
use redis::aio::{ConnectionLike, MultiplexedConnection};

use super::MemoryServer;

/// A URL no other test uses, so tests running at once do not share data.
pub(super) fn unique_url() -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!("memory://test-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

pub(super) async fn connect() -> (MemoryServer, MultiplexedConnection) {
    let server = MemoryServer::open(&unique_url());
    let connection = server.connection().await.unwrap();
    (server, connection)
}

/// Builds a command from a line of space-separated arguments.
pub(super) fn command(line: &str) -> redis::Cmd {
    let mut words = line.split_whitespace();
    let mut command = redis::cmd(words.next().unwrap());
    for word in words {
        command.arg(word);
    }
    command
}

/// Sends one command and returns its reply in the compact form.
pub(super) async fn run(connection: &mut MultiplexedConnection, line: &str) -> String {
    send(connection, &command(line)).await
}

pub(super) async fn send(connection: &mut MultiplexedConnection, command: &redis::Cmd) -> String {
    render(connection.req_packed_command(command).await.unwrap())
}

/// Runs the lines in order on a fresh server and returns every reply.
pub(super) async fn replies(lines: &[&str]) -> Vec<String> {
    let (_server, mut connection) = connect().await;
    let mut replies = Vec::new();
    for line in lines {
        replies.push(run(&mut connection, line).await);
    }
    replies
}

/// Runs the lines in order on a fresh server and returns the last reply.
pub(super) async fn last_reply(lines: &[&str]) -> String {
    replies(lines).await.pop().unwrap()
}

pub(super) fn render(value: Value) -> String {
    match value {
        Value::Nil => "nil".to_owned(),
        Value::Int(number) => number.to_string(),
        Value::BulkString(bytes) => format!("{:?}", String::from_utf8_lossy(&bytes)),
        Value::Array(items) => format!(
            "[{}]",
            items.into_iter().map(render).collect::<Vec<_>>().join(", ")
        ),
        Value::SimpleString(text) => text,
        Value::ServerError(error) => {
            format!("-{} {}", error.code(), error.details().unwrap_or_default())
        }
        other => format!("{other:?}"),
    }
}
