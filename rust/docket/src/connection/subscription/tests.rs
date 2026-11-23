use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::{BasicAuth, RedisError, RedisResult, StreamingCredentialsProvider};
use tokio::sync::broadcast;

use super::super::{Backend, Connection, Kind, Provider, Settings, credentials};
use super::{Subscription, Topics};
use crate::telemetry::tests::Logs;

#[derive(Clone, Debug)]
enum Rotation {
    To(BasicAuth),
    Fail,
    End,
}

/// A provider that gives its credentials, then whatever the test sends.
struct Rotating {
    auth: BasicAuth,
    rotations: broadcast::Sender<Rotation>,
}

impl StreamingCredentialsProvider for Rotating {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        let later = stream::unfold(self.rotations.subscribe(), |mut rotations| async move {
            match rotations.recv().await {
                Ok(Rotation::To(auth)) => Some((Ok(auth), rotations)),
                Ok(Rotation::Fail) => Some((
                    Err(RedisError::from((
                        redis::ErrorKind::AuthenticationFailed,
                        "the refresh failed",
                    ))),
                    rotations,
                )),
                _ => None,
            }
        });
        Box::pin(stream::iter([Ok(self.auth.clone())]).chain(later))
    }
}

/// A backend on the suite's Redis whose provider gives the URL's
/// credentials, or the default user, which takes any password on a server
/// with none.  The sender rotates them.
fn backend() -> (Arc<Backend>, BasicAuth, broadcast::Sender<Rotation>) {
    let url = std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| "memory://subscription-tests".to_owned());
    let (scheme, rest) = url.split_once("://").unwrap();
    let (url, auth) = match rest.split_once('@') {
        Some((userinfo, host)) => {
            let (user, password) = userinfo.split_once(':').unwrap();
            (
                format!("{scheme}://{host}"),
                BasicAuth::new(user.into(), password.into()),
            )
        }
        None => (
            url.clone(),
            BasicAuth::new("default".into(), "anything".into()),
        ),
    };
    let (rotations, _) = broadcast::channel(64);
    let provider = Provider(Arc::new(Rotating {
        auth: auth.clone(),
        rotations: rotations.clone(),
    }));
    let backend = Backend::open(&url, Some(provider), Settings::default()).unwrap();
    (Arc::new(backend), auth, rotations)
}

fn channel() -> String {
    format!("subscription-{}", uuid::Uuid::now_v7())
}

/// Publishes on the node the subscriptions connect to.  On a cluster, a
/// message published on another node reaches that one later, through the
/// cluster bus, so it could reach both connections while a subscription
/// moves, or arrive behind a later message.
async fn publish(backend: &Backend, channel: &str, messages: std::ops::Range<usize>) {
    let mut connection = match &backend.kind {
        Kind::Cluster { pubsub, .. } => {
            let provider = backend.credentials.as_ref().unwrap();
            let auth = credentials::next(&mut provider.subscribe()).await.unwrap();
            let client = credentials::authenticate(pubsub, &auth);
            Connection::Single(client.get_multiplexed_async_connection().await.unwrap())
        }
        _ => backend.connect().await.unwrap(),
    };
    for message in messages {
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(message)
            .query_async::<i64>(&mut connection)
            .await
            .unwrap();
    }
}

async fn read(subscription: &mut Subscription, count: usize) -> Vec<usize> {
    let mut received = Vec::new();
    while received.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .expect("the message arrives in time")
            .expect("the subscription is open");
        received.push(message.get_payload().unwrap());
    }
    received
}

#[tokio::test]
async fn a_rotation_moves_a_subscription_without_losing_a_message() {
    let (backend, auth, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    publish(&backend, &channel, 0..5).await;
    rotations.send(Rotation::To(auth)).unwrap();
    publish(&backend, &channel, 5..10).await;
    assert_eq!(
        read(&mut subscription, 10).await,
        (0..10).collect::<Vec<_>>()
    );

    publish(&backend, &channel, 10..15).await;
    assert_eq!(
        read(&mut subscription, 5).await,
        (10..15).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_rotation_moves_a_pattern_subscription() {
    let (backend, auth, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Pattern(format!("{channel}:*")))
        .await
        .unwrap();

    publish(&backend, &format!("{channel}:a"), 0..1).await;
    rotations.send(Rotation::To(auth)).unwrap();
    publish(&backend, &format!("{channel}:b"), 1..2).await;
    assert_eq!(read(&mut subscription, 2).await, vec![0, 1]);
}

#[tokio::test]
async fn a_failed_refresh_or_an_ended_provider_keeps_the_subscription() {
    let (logs, _guard) = Logs::capture();
    let (backend, _, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    rotations.send(Rotation::Fail).unwrap();
    logs.wait_for("the credential provider failed to refresh")
        .await;
    rotations.send(Rotation::End).unwrap();
    publish(&backend, &channel, 0..1).await;
    assert_eq!(read(&mut subscription, 1).await, vec![0]);
    publish(&backend, &channel, 1..2).await;
    assert_eq!(read(&mut subscription, 1).await, vec![1]);
}

/// A backend on the suite's server, or `None` on the memory backend, which
/// takes any credentials.
fn on_a_server() -> Option<(Arc<Backend>, BasicAuth, broadcast::Sender<Rotation>)> {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .map(|_| backend())
}

#[tokio::test]
async fn a_move_the_server_refuses_keeps_the_subscription() {
    let (logs, _guard) = Logs::capture();
    let Some((backend, _, rotations)) = on_a_server() else {
        return;
    };
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    let refused = BasicAuth::new("docket-test-nobody".into(), "wrong".into());
    rotations.send(Rotation::To(refused)).unwrap();
    // Nothing is published yet, so the next poll takes the rotation.
    let next = tokio::time::timeout(Duration::from_secs(1), subscription.next()).await;
    assert!(next.is_err(), "no message arrives");
    assert!(logs.contains("moving a subscription to rotated credentials failed"));

    publish(&backend, &channel, 0..1).await;
    assert_eq!(read(&mut subscription, 1).await, vec![0]);
}

#[tokio::test]
async fn rotations_lose_nothing_published_while_the_subscription_moves() {
    let (backend, auth, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    let publishing = tokio::spawn({
        let backend = Arc::clone(&backend);
        let channel = channel.clone();
        async move {
            for batch in 0..20 {
                publish(&backend, &channel, batch * 10..batch * 10 + 10).await;
                rotations.send(Rotation::To(auth.clone())).unwrap();
            }
        }
    });
    let mut received = std::collections::BTreeSet::new();
    while received.len() < 200 {
        received.extend(read(&mut subscription, 1).await);
    }
    publishing.await.unwrap();
    assert_eq!(received, (0..200).collect());
}

#[tokio::test]
async fn a_rotation_keeps_a_backlog_of_messages() {
    let (backend, auth, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    publish(&backend, &channel, 0..600).await;
    // Time for the old connection to queue the backlog before the move.
    tokio::time::sleep(Duration::from_millis(100)).await;
    rotations.send(Rotation::To(auth)).unwrap();
    assert_eq!(
        read(&mut subscription, 600).await,
        (0..600).collect::<Vec<_>>()
    );
}

/// Gives each subscriber a channel that holds one set of credentials, the
/// way redis-rs's Entra ID provider does.
struct OneAtATime {
    auth: BasicAuth,
    senders: Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::Sender<BasicAuth>>>>,
}

impl StreamingCredentialsProvider for OneAtATime {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        self.senders.lock().unwrap().push(sender);
        let later = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|auth| (Ok(auth), receiver))
        });
        Box::pin(stream::iter([Ok(self.auth.clone())]).chain(later))
    }
}

#[tokio::test]
async fn an_unread_subscription_never_holds_up_the_provider() {
    let senders = Arc::new(std::sync::Mutex::new(Vec::new()));
    let auth = BasicAuth::new("default".into(), "anything".into());
    let provider = Provider(Arc::new(OneAtATime {
        auth: auth.clone(),
        senders: Arc::clone(&senders),
    }));
    let backend = Arc::new(
        Backend::open(
            "memory://subscription-tests",
            Some(provider),
            Settings::default(),
        )
        .unwrap(),
    );
    let _unread = backend
        .subscribe(Topics::Channels(vec![channel()]))
        .await
        .unwrap();

    let sender = senders.lock().unwrap()[0].clone();
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), sender.send(auth.clone()))
            .await
            .expect("the provider can send its next credentials")
            .unwrap();
    }
}

#[tokio::test]
async fn a_dropped_subscription_stops_following_the_provider() {
    let (backend, _, rotations) = backend();
    let subscription = backend
        .subscribe(Topics::Channels(vec![channel()]))
        .await
        .unwrap();
    assert_eq!(rotations.receiver_count(), 1);

    drop(subscription);
    tokio::time::timeout(Duration::from_secs(5), async {
        while rotations.receiver_count() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the provider's stream is dropped");
}
