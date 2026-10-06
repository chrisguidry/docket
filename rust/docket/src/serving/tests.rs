use std::net::SocketAddr;

use rstest::rstest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{MAX_HEAD, serve};

/// Serves `body` on a free local port, for as long as the test runs.
async fn server(body: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, "text/plain", move || body.to_owned()));
    address
}

/// Sends `request`, then closes the sending half, as a client that has said
/// all it will, and reads the whole answer.
async fn exchange(address: SocketAddr, request: &[u8], close: bool) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(request).await.unwrap();
    if close {
        stream.shutdown().await.unwrap();
    }
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    answer
}

const OK: &str = "HTTP/1.1 200 OK\r\n\
                  Content-Type: text/plain\r\n\
                  Content-Length: 2\r\n\
                  Connection: close\r\n\r\n\
                  OK";

#[rstest]
#[case::get(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n", false)]
#[case::any_path(b"GET /metrics?x=1 HTTP/1.1\r\n\r\n", false)]
#[case::bare_newlines(b"GET / HTTP/1.0\nHost: localhost\n\n", false)]
#[case::head_cut_short(b"GET / HTTP/1.1\r\nHost: local", true)]
#[case::nothing_sent(b"", true)]
#[tokio::test]
async fn answers_every_request(#[case] request: &[u8], #[case] close: bool) {
    let address = server("OK").await;
    assert_eq!(exchange(address, request, close).await, OK);
}

#[tokio::test]
async fn answers_a_head_too_long_to_read() {
    let address = server("OK").await;
    let request = vec![b'x'; usize::try_from(MAX_HEAD).unwrap()];
    assert_eq!(exchange(address, &request, false).await, OK);
}

#[tokio::test]
async fn keeps_answering_after_each_connection() {
    let address = server("OK").await;
    assert_eq!(
        exchange(address, b"GET / HTTP/1.1\r\n\r\n", false).await,
        OK
    );
    assert_eq!(
        exchange(address, b"GET / HTTP/1.1\r\n\r\n", false).await,
        OK
    );
}

#[tokio::test]
async fn counts_the_body_in_bytes() {
    let address = server("✓").await;
    let answer = exchange(address, b"GET / HTTP/1.1\r\n\r\n", false).await;
    assert!(answer.contains("Content-Length: 3\r\n"), "{answer}");
    assert!(answer.ends_with("\r\n\r\n✓"), "{answer}");
}
