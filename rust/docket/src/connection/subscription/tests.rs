use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::{BasicAuth, RedisError, RedisResult, StreamingCredentialsProvider};
use tokio::sync::broadcast;

use super::super::{Backend, Provider, Settings};
use super::{Subscription, Topics};

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

async fn publish(backend: &Backend, channel: &str, messages: std::ops::Range<usize>) {
    let mut connection = backend.connect().await.unwrap();
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
    let (backend, _, rotations) = backend();
    let channel = channel();
    let mut subscription = backend
        .subscribe(Topics::Channels(vec![channel.clone()]))
        .await
        .unwrap();

    rotations.send(Rotation::Fail).unwrap();
    rotations.send(Rotation::End).unwrap();
    publish(&backend, &channel, 0..1).await;
    assert_eq!(read(&mut subscription, 1).await, vec![0]);
    publish(&backend, &channel, 1..2).await;
    assert_eq!(read(&mut subscription, 1).await, vec![1]);
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
