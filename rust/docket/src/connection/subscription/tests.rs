use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::{BasicAuth, RedisError, RedisResult, StreamingCredentialsProvider};
use rstest::rstest;
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

/// The suite's Redis, or an in-process one.
fn suite_url() -> String {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| "memory://subscription-tests".to_owned())
}

/// The suite's Redis without credentials in its URL, and the credentials
/// for it: the URL's, or the default user, which takes any password on a
/// server with none.
fn split(url: &str) -> (String, BasicAuth) {
    let (scheme, rest) = url.split_once("://").unwrap();
    match rest.split_once('@') {
        Some((userinfo, host)) => {
            let (user, password) = userinfo.split_once(':').unwrap();
            (
                format!("{scheme}://{host}"),
                BasicAuth::new(user.into(), password.into()),
            )
        }
        None => (
            url.to_owned(),
            BasicAuth::new("default".into(), "anything".into()),
        ),
    }
}

/// A backend on the suite's Redis whose provider gives the suite's
/// credentials.  The sender rotates them.
fn backend() -> (Arc<Backend>, broadcast::Sender<Rotation>) {
    let (url, auth) = split(&suite_url());
    let (rotations, _) = broadcast::channel(64);
    let provider = Provider(Arc::new(Rotating {
        auth,
        rotations: rotations.clone(),
    }));
    let backend = Backend::open(&url, Some(provider), Settings::default()).unwrap();
    (Arc::new(backend), rotations)
}

/// A backend on the suite's Redis with the credentials in its URL.
fn without_a_provider() -> Arc<Backend> {
    Arc::new(Backend::open(&suite_url(), None, Settings::default()).unwrap())
}

/// A backend on the suite's server, or `None` on the memory backend, which
/// ignores credentials and so has nothing to rotate.
fn on_a_server() -> Option<(Arc<Backend>, broadcast::Sender<Rotation>)> {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .map(|_| backend())
}

fn channel() -> String {
    format!("subscription-{}", uuid::Uuid::now_v7())
}

/// A connection to the server that the subscriptions connect to: on a
/// cluster, the one node they use.  A message published on another node
/// reaches that one later, through the cluster bus, so it could arrive
/// behind a later message.
async fn admin(backend: &Backend) -> Connection {
    let Kind::Cluster { pubsub, .. } = &backend.kind else {
        return backend.connect().await.unwrap();
    };
    let client = match &backend.credentials {
        Some(provider) => {
            let auth = credentials::next(&mut provider.subscribe()).await.unwrap();
            credentials::resp3(pubsub, &auth)
        }
        None => pubsub.clone(),
    };
    Connection::Single(client.get_multiplexed_async_connection().await.unwrap())
}

async fn publish(backend: &Backend, channel: &str, messages: std::ops::Range<usize>) {
    let mut connection = admin(backend).await;
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

fn channels(channel: &str) -> Topics {
    Topics::Channels(vec![channel.to_owned()])
}

fn pattern(channel: &str) -> Topics {
    Topics::Pattern(format!("{channel}*"))
}

#[rstest]
#[case::channels_without_a_provider(without_a_provider, channels)]
#[case::pattern_without_a_provider(without_a_provider, pattern)]
#[case::channels_with_a_provider(|| backend().0, channels)]
#[case::pattern_with_a_provider(|| backend().0, pattern)]
#[tokio::test]
async fn a_subscription_receives_what_is_published(
    #[case] backend: fn() -> Arc<Backend>,
    #[case] topics: fn(&str) -> Topics,
) {
    let backend = backend();
    let channel = channel();
    let mut subscription = backend.subscribe(topics(&channel)).await.unwrap();

    publish(&backend, &channel, 0..3).await;
    assert_eq!(read(&mut subscription, 3).await, vec![0, 1, 2]);
}

/// Two users of the suite's server for the credentials to rotate between.
/// Setting a user again changes nothing, so every test can set them.
async fn users(backend: &Backend) -> [BasicAuth; 2] {
    let mut admin = admin(backend).await;
    let users = [
        BasicAuth::new("docket-test-alice".into(), "apw".into()),
        BasicAuth::new("docket-test-bob".into(), "bpw".into()),
    ];
    for user in &users {
        redis::cmd("ACL")
            .arg("SETUSER")
            .arg(user.username())
            .arg("on")
            .arg(format!(">{}", user.password()))
            .arg("~*")
            .arg("&*")
            .arg("+@all")
            .query_async::<()>(&mut admin)
            .await
            .unwrap();
    }
    users
}

/// A user of the suite's server who may run any command on any key, and
/// subscribe to no channel.
async fn without_channels(backend: &Backend) -> BasicAuth {
    let user = BasicAuth::new("docket-test-no-channels".into(), "npw".into());
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(user.username())
        .arg("on")
        .arg(format!(">{}", user.password()))
        .arg("~*")
        .arg("resetchannels")
        .arg("+@all")
        .query_async::<()>(&mut admin(backend).await)
        .await
        .unwrap();
    user
}

#[tokio::test]
async fn a_subscription_the_server_refuses_fails() {
    let Some((server, _)) = on_a_server() else {
        return;
    };
    let user = without_channels(&server).await;
    let (url, _) = split(&suite_url());
    let (rotations, _) = broadcast::channel(1);
    let provider = Provider(Arc::new(Rotating {
        auth: user,
        rotations,
    }));
    let backend = Backend::open(&url, Some(provider), Settings::default()).unwrap();

    let error = backend.subscribe(channels(&channel())).await.err().unwrap();
    assert_eq!(error.code(), Some("NOPERM"), "{error}");
}

/// The subscribed connection's `CLIENT INFO`, as `name=value` fields.
async fn client_info(subscription: &Subscription) -> String {
    let mut connection = subscription.connection().unwrap();
    redis::cmd("CLIENT")
        .arg("INFO")
        .query_async(&mut connection)
        .await
        .unwrap()
}

fn field<'a>(info: &'a str, name: &str) -> &'a str {
    info.split_whitespace()
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
        .unwrap()
}

/// Waits until the subscribed connection is authenticated as `user`, and
/// returns its `CLIENT INFO` then.
async fn authenticated_as(subscription: &Subscription, user: &BasicAuth) -> String {
    let waiting = async {
        loop {
            let info = client_info(subscription).await;
            if field(&info, "user") == user.username() {
                return info;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the connection authenticates as the user in time")
}

/// Nothing reads the subscription until the rotations are done, so they
/// don't wait for a reader.
#[tokio::test]
async fn a_rotation_authenticates_the_subscribed_connection_again() {
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let [alice, bob] = users(&backend).await;
    let channel = channel();
    let mut subscription = backend.subscribe(channels(&channel)).await.unwrap();
    let before = client_info(&subscription).await;

    rotations.send(Rotation::To(alice.clone())).unwrap();
    authenticated_as(&subscription, &alice).await;
    rotations.send(Rotation::To(bob.clone())).unwrap();
    let after = authenticated_as(&subscription, &bob).await;

    assert_eq!(field(&after, "id"), field(&before, "id"));
    assert_eq!(field(&after, "resp"), "3");
    publish(&backend, &channel, 0..3).await;
    assert_eq!(read(&mut subscription, 3).await, vec![0, 1, 2]);
}

#[tokio::test]
async fn messages_published_through_rotations_arrive_once_and_in_order() {
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let users = users(&backend).await;
    let channel = channel();
    let mut subscription = backend.subscribe(channels(&channel)).await.unwrap();

    let publishing = tokio::spawn({
        let backend = Arc::clone(&backend);
        let channel = channel.clone();
        async move { publish(&backend, &channel, 0..2000).await }
    });
    for user in users.iter().cycle().take(20) {
        rotations.send(Rotation::To(user.clone())).unwrap();
        authenticated_as(&subscription, user).await;
    }
    assert_eq!(
        read(&mut subscription, 2000).await,
        (0..2000).collect::<Vec<_>>()
    );
    publishing.await.unwrap();
}

#[tokio::test]
async fn a_refused_auth_keeps_the_subscription_and_the_next_rotation_succeeds() {
    let (logs, _guard) = Logs::capture();
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let [_, bob] = users(&backend).await;
    let channel = channel();
    let mut subscription = backend.subscribe(channels(&channel)).await.unwrap();
    let before = client_info(&subscription).await;

    let refused = BasicAuth::new(bob.username().into(), "wrong".into());
    rotations.send(Rotation::To(refused)).unwrap();
    logs.wait_for("re-authenticating a subscription failed")
        .await;
    assert_eq!(
        field(&client_info(&subscription).await, "user"),
        field(&before, "user")
    );
    publish(&backend, &channel, 0..3).await;
    assert_eq!(read(&mut subscription, 3).await, vec![0, 1, 2]);

    rotations.send(Rotation::To(bob.clone())).unwrap();
    authenticated_as(&subscription, &bob).await;
    publish(&backend, &channel, 3..6).await;
    assert_eq!(read(&mut subscription, 3).await, vec![3, 4, 5]);
}

/// Waits until `condition` holds.
async fn until(condition: impl Fn() -> bool) {
    let waiting = async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the condition holds in time");
}

#[tokio::test]
async fn a_failed_refresh_or_an_ended_provider_keeps_the_subscription() {
    let (logs, _guard) = Logs::capture();
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let channel = channel();
    let mut subscription = backend.subscribe(channels(&channel)).await.unwrap();

    rotations.send(Rotation::Fail).unwrap();
    logs.wait_for("the credential provider failed to refresh")
        .await;
    rotations.send(Rotation::End).unwrap();
    until(|| rotations.receiver_count() == 0).await;
    publish(&backend, &channel, 0..1).await;
    assert_eq!(read(&mut subscription, 1).await, vec![0]);
}

/// Waits until `CLIENT LIST` has no connection with this `id`.
async fn gone(admin: &mut Connection, id: &str) {
    let waiting = async {
        loop {
            let listed: String = redis::cmd("CLIENT")
                .arg("LIST")
                .arg("ID")
                .arg(id)
                .query_async(admin)
                .await
                .unwrap();
            if listed.trim().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the connection closes in time");
}

#[tokio::test]
async fn a_dropped_subscription_closes_its_connection_and_leaves_the_provider() {
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let subscription = backend.subscribe(channels(&channel())).await.unwrap();
    let info = client_info(&subscription).await;
    let id = field(&info, "id");
    let mut admin = admin(&backend).await;
    let following = rotations.receiver_count();

    drop(subscription);
    gone(&mut admin, id).await;
    until(|| rotations.receiver_count() == following - 1).await;
}

#[tokio::test]
async fn a_subscription_ends_when_the_server_closes_its_connection() {
    let Some((backend, _rotations)) = on_a_server() else {
        return;
    };
    let mut subscription = backend.subscribe(channels(&channel())).await.unwrap();
    let info = client_info(&subscription).await;

    redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(field(&info, "id"))
        .query_async::<()>(&mut admin(&backend).await)
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await
        .expect("the subscription ends in time");
    assert!(next.is_none());
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
    if on_a_server().is_none() {
        return;
    }
    let (url, auth) = split(&suite_url());
    let senders = Arc::new(std::sync::Mutex::new(Vec::new()));
    let provider = Provider(Arc::new(OneAtATime {
        auth: auth.clone(),
        senders: Arc::clone(&senders),
    }));
    let backend = Backend::open(&url, Some(provider), Settings::default()).unwrap();
    let _unread = backend.subscribe(channels(&channel())).await.unwrap();

    let sender = senders.lock().unwrap()[0].clone();
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), sender.send(auth.clone()))
            .await
            .expect("the provider can send its next credentials")
            .unwrap();
    }
}
