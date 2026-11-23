use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::{BasicAuth, RedisError, RedisResult, StreamingCredentialsProvider};
use rstest::rstest;
use tokio::sync::broadcast;

use super::super::{Backend, Connection, Kind, Provider, Settings, credentials};
use super::{Subscription, Topics};

mod renewal;

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

/// A subscription follows the backend's checked credentials, and only the
/// backend follows the provider.
#[tokio::test]
async fn a_dropped_subscription_closes_its_connection_and_stops_following_the_credentials() {
    let Some((backend, rotations)) = on_a_server() else {
        return;
    };
    let subscription = backend.subscribe(channels(&channel())).await.unwrap();
    let info = client_info(&subscription).await;
    let id = field(&info, "id");
    let mut admin = admin(&backend).await;
    let checked = backend.credentials.as_ref().unwrap();
    let following = checked.followers();

    drop(subscription);
    gone(&mut admin, id).await;
    until(|| checked.followers() == following - 1).await;
    assert_eq!(rotations.receiver_count(), 1);
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
