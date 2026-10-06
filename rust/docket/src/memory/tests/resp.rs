use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use rstest::rstest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::memory::engine::store::Store;
use crate::memory::resp::{ProtocolError, Reply, parse_request};
use crate::memory::session::Session;

fn parse(input: &[u8]) -> (Result<Option<Vec<Bytes>>, ProtocolError>, usize) {
    let mut buf = BytesMut::from(input);
    let parsed = parse_request(&mut buf);
    (parsed, buf.len())
}

#[test]
fn a_whole_request_is_taken_off_the_buffer() {
    let (parsed, left) = parse(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n");
    assert_eq!(parsed, Ok(Some(vec![Bytes::from("GET"), Bytes::from("k")])));
    assert_eq!(left, 4);
}

#[rstest]
#[case::nothing(b"")]
#[case::partial_count(b"*2")]
#[case::partial_header(b"*2\r\n$3")]
#[case::partial_body(b"*1\r\n$3\r\nGE")]
#[case::missing_terminator(b"*1\r\n$3\r\nGET")]
#[case::missing_argument(b"*2\r\n$3\r\nGET\r\n")]
fn a_partial_request_waits_for_more(#[case] input: &[u8]) {
    assert_eq!(parse(input), (Ok(None), input.len()));
}

#[rstest]
#[case::inline(b"PING\r\n", "expected '*', got 'P'")]
#[case::not_bulk(b"*1\r\n:1\r\n", "expected '$', got ':'")]
#[case::negative_count(b"*-1\r\n", "invalid multibulk length")]
#[case::huge_count(b"*99999999\r\n", "invalid multibulk length")]
#[case::bad_length(b"*1\r\n$x\r\n", "invalid bulk length")]
#[case::wrong_terminator(b"*1\r\n$1\r\nab\r\n", "expected CRLF after bulk string")]
fn malformed_input_is_a_protocol_error(#[case] input: &[u8], #[case] message: &str) {
    assert_eq!(parse(input).0, Err(ProtocolError(message.to_owned())));
}

#[rstest]
#[case::status(Reply::ok(), "+OK\r\n")]
#[case::error(Reply::Error("ERR two\r\nlines".to_owned()), "-ERR two  lines\r\n")]
#[case::integer(Reply::Integer(-3), ":-3\r\n")]
#[case::bulk(Reply::bulk("hi"), "$2\r\nhi\r\n")]
#[case::nil(Reply::Nil, "$-1\r\n")]
#[case::array(Reply::Array(vec![Reply::Integer(1), Reply::Nil]), "*2\r\n:1\r\n$-1\r\n")]
fn replies_encode_as_resp2(#[case] reply: Reply, #[case] expected: &str) {
    let mut out = BytesMut::new();
    reply.encode(&mut out);
    assert_eq!(out, expected.as_bytes());
}

/// Writes raw bytes to a fresh session and returns everything it sends back
/// before it closes the connection.
async fn exchange(input: &[u8]) -> String {
    let (mut client, server) = tokio::io::duplex(1024);
    tokio::spawn(Session::new(Arc::new(Store::new()), server).run());
    client.write_all(input).await.unwrap();
    let mut output = Vec::new();
    client.read_to_end(&mut output).await.unwrap();
    String::from_utf8(output).unwrap()
}

#[tokio::test]
async fn the_server_closes_the_connection_after_a_protocol_error() {
    let output = exchange(b"*1\r\n$4\r\nPING\r\nPING\r\n*1\r\n$4\r\nPING\r\n").await;
    assert_eq!(
        output,
        "+PONG\r\n-ERR Protocol error: expected '*', got 'P'\r\n"
    );
}

#[tokio::test]
async fn a_session_fails_when_its_client_leaves_before_the_reply() {
    let (mut client, server) = tokio::io::duplex(1024);
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    drop(client);
    let session = Session::new(Arc::new(Store::new()), server);
    assert!(session.run().await.is_err());
}

#[tokio::test]
async fn an_empty_request_gets_no_reply() {
    let output = exchange(b"*0\r\n*1\r\n$4\r\nPING\r\nPING").await;
    assert_eq!(
        output,
        "+PONG\r\n-ERR Protocol error: expected '*', got 'P'\r\n"
    );
}
