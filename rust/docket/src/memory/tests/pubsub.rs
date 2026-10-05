use std::time::Duration;

use futures_util::StreamExt;
use redis::Value;
use redis::aio::PubSub;
use rstest::rstest;

use super::{connect, last_reply, render, run};

/// The channel, pattern (empty for a plain message), and payload of the next
/// message, failing after a second.
async fn next_message(pubsub: &mut PubSub) -> (String, String, String) {
    let message = tokio::time::timeout(Duration::from_secs(1), pubsub.on_message().next())
        .await
        .unwrap()
        .unwrap();
    let pattern = message.get_pattern::<Option<String>>().unwrap();
    (
        message.get_channel_name().to_owned(),
        pattern.unwrap_or_default(),
        message.get_payload().unwrap(),
    )
}

fn message(channel: &str, pattern: &str, payload: &str) -> (String, String, String) {
    (channel.to_owned(), pattern.to_owned(), payload.to_owned())
}

#[tokio::test]
async fn a_subscriber_receives_published_messages() {
    let (server, mut connection) = connect().await;
    let mut pubsub = server.pubsub().await.unwrap();
    pubsub.subscribe("news").await.unwrap();
    assert_eq!(run(&mut connection, "PUBLISH news hello").await, "1");
    assert_eq!(
        next_message(&mut pubsub).await,
        message("news", "", "hello")
    );
}

#[tokio::test]
async fn a_pattern_subscriber_receives_pmessages() {
    let (server, mut connection) = connect().await;
    let mut pubsub = server.pubsub().await.unwrap();
    pubsub.psubscribe("news.*").await.unwrap();
    assert_eq!(run(&mut connection, "PUBLISH news.sports goal").await, "1");
    assert_eq!(
        next_message(&mut pubsub).await,
        message("news.sports", "news.*", "goal")
    );
}

#[tokio::test]
async fn publish_counts_channel_and_pattern_subscribers() {
    let (server, mut connection) = connect().await;
    let mut first = server.pubsub().await.unwrap();
    let mut second = server.pubsub().await.unwrap();
    first.subscribe("news").await.unwrap();
    first.psubscribe("n*").await.unwrap();
    second.subscribe("news").await.unwrap();
    assert_eq!(run(&mut connection, "PUBLISH news hello").await, "3");
}

#[tokio::test]
async fn a_subscriber_only_receives_its_own_channels() {
    let (server, mut connection) = connect().await;
    let mut mine = server.pubsub().await.unwrap();
    let mut theirs = server.pubsub().await.unwrap();
    mine.subscribe("mine").await.unwrap();
    mine.psubscribe("m*").await.unwrap();
    theirs.subscribe("theirs").await.unwrap();
    theirs.psubscribe("t*").await.unwrap();
    run(&mut connection, "PUBLISH theirs skipped").await;
    run(&mut connection, "PUBLISH mine kept").await;
    assert_eq!(next_message(&mut mine).await, message("mine", "m*", "kept"));
    assert_eq!(next_message(&mut mine).await, message("mine", "", "kept"));
}

#[tokio::test]
async fn unsubscribing_stops_delivery() {
    let (server, mut connection) = connect().await;
    let mut pubsub = server.pubsub().await.unwrap();
    pubsub.subscribe(&["a", "b"]).await.unwrap();
    pubsub.psubscribe("p*").await.unwrap();
    pubsub.unsubscribe("a").await.unwrap();
    pubsub.punsubscribe("p*").await.unwrap();
    assert_eq!(run(&mut connection, "PUBLISH a x").await, "0");
    assert_eq!(run(&mut connection, "PUBLISH b x").await, "1");
}

#[tokio::test]
async fn closing_a_subscriber_removes_its_subscriptions() {
    let (server, mut connection) = connect().await;
    let mut pubsub = server.pubsub().await.unwrap();
    pubsub.subscribe("news").await.unwrap();
    drop(pubsub);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(run(&mut connection, "PUBLISH news hello").await, "0");
}

#[tokio::test]
async fn ping_answers_in_both_modes() {
    let (server, _connection) = connect().await;
    let mut pubsub = server.pubsub().await.unwrap();
    let before: Value = pubsub.ping().await.unwrap();
    pubsub.subscribe("news").await.unwrap();
    let after: Value = pubsub.ping_message("hi").await.unwrap();
    assert_eq!(
        (render(before), render(after)),
        ("PONG".to_owned(), "[\"pong\", \"hi\"]".to_owned())
    );
}

#[rstest]
#[case::subscribe(&["SUBSCRIBE c"], "[\"subscribe\", \"c\", 1]")]
#[case::psubscribe(&["SUBSCRIBE c", "PSUBSCRIBE p*"], "[\"psubscribe\", \"p*\", 2]")]
#[case::other_commands_refused(&["SUBSCRIBE c", "GET k"], "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")]
#[case::ping_without_message(&["SUBSCRIBE c", "PING"], "[\"pong\", \"\"]")]
#[case::unsubscribe_all(&["SUBSCRIBE c", "UNSUBSCRIBE"], "[\"unsubscribe\", \"c\", 0]")]
#[case::commands_after_unsubscribing(&["SUBSCRIBE c", "UNSUBSCRIBE c", "GET k"], "nil")]
#[case::unsubscribe_nothing(&["UNSUBSCRIBE"], "[\"unsubscribe\", nil, 0]")]
#[case::punsubscribe_nothing(&["SUBSCRIBE c", "PUNSUBSCRIBE"], "[\"punsubscribe\", nil, 1]")]
#[case::subscribe_nothing(&["SUBSCRIBE"], "-ERR wrong number of arguments for 'subscribe' command")]
#[tokio::test]
async fn subscribe_mode(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}
