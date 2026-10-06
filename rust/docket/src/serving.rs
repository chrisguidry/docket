//! The smallest HTTP server that a healthcheck or a Prometheus scrape needs.
//!
//! Both clients send one request and read one response, so the server reads
//! the request head, answers it, and closes the connection.  It answers
//! every path, as pydocket's servers do.  This keeps an HTTP framework out
//! of docket's dependencies.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// The most of a request head that the server reads.  A client that sends
/// more gets its answer early, so that it cannot fill the server's memory.
const MAX_HEAD: u64 = 16 * 1024;

/// Answers each connection with `body()`, until the future is dropped.
///
/// Each connection runs in a task of its own, so that a slow client does
/// not hold up the next one.  `body` runs after the request head arrives,
/// so the answer is as fresh as the request.  A connection that fails
/// before it is accepted, such as one that the client reset, has no one to
/// answer, so the server skips it, as Python's `socketserver` does.
pub(crate) async fn serve<F>(
    listener: TcpListener,
    content_type: &'static str,
    body: F,
) -> io::Result<()>
where
    F: Fn() -> String + Clone + Send + 'static,
{
    loop {
        let accepted = listener.accept().await;
        let _ =
            accepted.map(|(stream, _)| tokio::spawn(answer(stream, content_type, body.clone())));
    }
}

/// Reads the request head and writes the answer.  A client that goes away
/// before the answer is written has no one to tell, so a failed read ends
/// the head and a failed write ends the answer.
async fn answer(mut stream: TcpStream, content_type: &str, body: impl Fn() -> String) {
    read_head(&mut stream).await;
    let body = body();
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n\
         {body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Reads up to the blank line that ends a request head.  A header line is
/// at least three bytes, and the blank line is `\r\n` or `\n`, so a line of
/// two bytes or fewer ends the head.  So does the end of the stream, or of
/// [`MAX_HEAD`], where a read returns no bytes.
async fn read_head(stream: &mut TcpStream) {
    let mut reader = BufReader::new(stream.take(MAX_HEAD));
    let mut line = Vec::new();
    while matches!(reader.read_until(b'\n', &mut line).await, Ok(read) if read > 2) {
        line.clear();
    }
}

#[cfg(test)]
mod tests;
