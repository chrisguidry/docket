use std::time::{Duration, Instant};

use redis::aio::MultiplexedConnection;
use rstest::rstest;

use super::{command, connect, last_reply, run, send, unique_url};
use crate::memory::MemoryServer;

#[rstest]
#[case::xread(&["XADD s 1-1 f a", "XREAD STREAMS s 0"], "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]")]
#[case::xread_after_id(&["XADD s 1-1 f a", "XADD s 1-2 f b", "XREAD COUNT 5 STREAMS s 1-1"], "[[\"s\", [[\"1-2\", [\"f\", \"b\"]]]]]")]
#[case::xread_count(&["XADD s 1-1 f a", "XADD s 1-2 f b", "XREAD COUNT 1 STREAMS s 0-0"], "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]")]
#[case::xread_count_zero_is_unlimited(&["XADD s 1-1 f a", "XADD s 1-2 f b", "XREAD COUNT 0 STREAMS s 0"], "[[\"s\", [[\"1-1\", [\"f\", \"a\"]], [\"1-2\", [\"f\", \"b\"]]]]]")]
#[case::xread_dollar_without_block(&["XADD s 1-1 f a", "XREAD STREAMS s $"], "nil")]
#[case::xread_nothing(&["XREAD STREAMS s 0"], "nil")]
#[case::xread_unbalanced(&["XREAD STREAMS s t 0"], "-ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.")]
#[case::xread_no_streams(&["XREAD COUNT 1"], "-ERR syntax error")]
#[case::xread_unknown_option(&["XREAD NOACK STREAMS s 0"], "-ERR syntax error")]
#[case::xread_bad_id(&["XREAD STREAMS s x"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::xread_bad_timeout(&["XREAD BLOCK soon STREAMS s 0"], "-ERR timeout is not an integer or out of range")]
#[case::xread_negative_timeout(&["XREAD BLOCK -1 STREAMS s 0"], "-ERR timeout is negative")]
#[case::xread_wrong_type(&["SET s v", "XREAD BLOCK 1000 STREAMS s 0"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::group_read(&["XADD s 1-1 f a", "XGROUP CREATE s g 0", "XREADGROUP GROUP g c NOACK STREAMS s >"], "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]")]
#[case::group_read_history(&["XADD s 1-1 f a", "XGROUP CREATE s g 0", "XREADGROUP GROUP g c STREAMS s >", "XREADGROUP GROUP g c STREAMS s 0"], "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]")]
#[case::group_read_empty_history(&["XGROUP CREATE s g 0 MKSTREAM", "XREADGROUP GROUP g c BLOCK 0 STREAMS s 0"], "[]")]
#[case::group_read_nothing_new(&["XGROUP CREATE s g 0 MKSTREAM", "XREADGROUP GROUP g c STREAMS s >"], "nil")]
#[case::group_read_no_group(&["XREADGROUP GROUP g c BLOCK 1000 STREAMS s >"], "-NOGROUP No such key 's' or consumer group 'g'")]
#[case::group_read_without_group(&["XREADGROUP STREAMS s >"], "-ERR syntax error")]
#[case::group_read_unbalanced(&["XREADGROUP GROUP g c STREAMS s"], "-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.")]
#[case::group_read_bad_id(&["XREADGROUP GROUP g c STREAMS s x"], "-ERR Invalid stream ID specified as stream command argument")]
#[tokio::test]
async fn stream_reads(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

/// Starts `line` on its own connection, so the caller can add entries while
/// it blocks.
fn start(
    mut connection: MultiplexedConnection,
    line: &'static str,
) -> tokio::task::JoinHandle<String> {
    tokio::spawn(async move { run(&mut connection, line).await })
}

#[tokio::test]
async fn a_blocked_group_read_wakes_on_xadd_from_another_connection() {
    let (server, mut connection) = connect().await;
    run(&mut connection, "XGROUP CREATE s g $ MKSTREAM").await;
    let read = start(
        server.connection().await.unwrap(),
        "XREADGROUP GROUP g c BLOCK 5000 STREAMS s >",
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    run(&mut connection, "XADD other 1-1 f ignored").await;
    run(&mut connection, "XADD s 1-1 f a").await;
    assert_eq!(
        read.await.unwrap(),
        "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]"
    );
}

#[tokio::test]
async fn a_blocked_read_of_new_entries_wakes_on_xadd() {
    let (server, mut connection) = connect().await;
    run(&mut connection, "XADD s 1-1 f old").await;
    let read = start(
        server.connection().await.unwrap(),
        "XREAD BLOCK 5000 STREAMS s $",
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    run(&mut connection, "XADD s 1-2 f new").await;
    assert_eq!(
        read.await.unwrap(),
        "[[\"s\", [[\"1-2\", [\"f\", \"new\"]]]]]"
    );
}

#[tokio::test]
async fn a_blocked_read_times_out_with_nil() {
    let (_server, mut connection) = connect().await;
    run(&mut connection, "XGROUP CREATE s g $ MKSTREAM").await;
    let started = Instant::now();
    let reply = run(&mut connection, "XREADGROUP GROUP g c BLOCK 50 STREAMS s >").await;
    assert_eq!(reply, "nil");
    assert!(started.elapsed() >= Duration::from_millis(50));
}

#[tokio::test]
async fn block_zero_waits_until_an_entry_arrives() {
    let (server, mut connection) = connect().await;
    run(&mut connection, "XGROUP CREATE s g $ MKSTREAM").await;
    let read = start(
        server.connection().await.unwrap(),
        "XREADGROUP GROUP g c BLOCK 0 STREAMS s >",
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!read.is_finished());
    run(&mut connection, "XADD s 1-1 f a").await;
    assert_eq!(
        read.await.unwrap(),
        "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]"
    );
}

#[tokio::test]
async fn requests_behind_a_blocked_read_run_after_it() {
    let (server, mut connection) = connect().await;
    let mut blocked = server.connection().await.unwrap();
    let mut behind = blocked.clone();
    let read = tokio::spawn(async move { run(&mut blocked, "XREAD BLOCK 0 STREAMS s $").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let echo = tokio::spawn(async move { send(&mut behind, &command("ECHO later")).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!echo.is_finished());
    run(&mut connection, "XADD s 1-1 f a").await;
    assert_eq!(
        read.await.unwrap(),
        "[[\"s\", [[\"1-1\", [\"f\", \"a\"]]]]]"
    );
    assert_eq!(echo.await.unwrap(), "\"later\"");
}

#[tokio::test]
async fn a_blocked_read_ends_when_its_client_disconnects() {
    let url = unique_url();
    let server = MemoryServer::open(&url);
    let mut connection = server.connection().await.unwrap();
    run(&mut connection, "SET k v").await;
    let read = start(connection, "XREAD BLOCK 0 STREAMS s $");
    tokio::time::sleep(Duration::from_millis(50)).await;
    read.abort();
    drop(server);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The session no longer holds the store, so the URL starts empty.
    let server = MemoryServer::open(&url);
    let mut connection = server.connection().await.unwrap();
    assert_eq!(run(&mut connection, "GET k").await, "nil");
}
