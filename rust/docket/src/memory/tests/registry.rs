use std::time::Duration;

use super::{run, unique_url};
use crate::memory::MemoryServer;

#[tokio::test]
async fn servers_on_the_same_url_share_data() {
    let url = unique_url();
    let first = MemoryServer::open(&url);
    let second = MemoryServer::open(&url);
    run(&mut first.connection().await.unwrap(), "SET k v").await;
    assert_eq!(
        run(&mut second.connection().await.unwrap(), "GET k").await,
        "\"v\""
    );
}

#[tokio::test]
async fn servers_on_different_urls_are_separate() {
    let first = MemoryServer::open(&unique_url());
    let second = MemoryServer::open(&unique_url());
    run(&mut first.connection().await.unwrap(), "SET k v").await;
    assert_eq!(
        run(&mut second.connection().await.unwrap(), "GET k").await,
        "nil"
    );
}

#[tokio::test]
async fn a_url_keeps_its_data_after_every_server_on_it_is_gone() {
    let url = unique_url();
    let first = MemoryServer::open(&url);
    let mut connection = first.connection().await.unwrap();
    run(&mut connection, "SET k v").await;
    drop(connection);
    drop(first);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = MemoryServer::open(&url);
    assert_eq!(
        run(&mut second.connection().await.unwrap(), "GET k").await,
        "\"v\""
    );
}

#[test]
fn a_server_opens_without_a_runtime() {
    let server = MemoryServer::open(&unique_url());
    assert_eq!(server.store().keys(b"*").len(), 0);
}

#[tokio::test]
async fn the_sweeper_removes_expired_keys_that_nothing_reads() {
    let server = MemoryServer::open(&unique_url());
    run(&mut server.connection().await.unwrap(), "SET k v PX 20").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.store().sweep_expired(), 0);
}
