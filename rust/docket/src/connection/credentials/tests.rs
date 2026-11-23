//! A rotation of a provider's credentials reaches every command connection
//! that a backend opens.  redis-rs sends the AUTH on each of them, so these
//! read `CLIENT LIST` on every node of the suite's server to see which user
//! each connection is authenticated as.  The memory backend ignores
//! credentials, so on it they have nothing to check.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::aio::ConnectionLike;
use redis::{BasicAuth, RedisResult, StreamingCredentialsProvider};
use rstest::rstest;
use tokio::sync::watch;

use super::super::{Backend, Connection, Settings, Shared, Subscription, Topics};
use super::Provider;

/// Gives the credentials in force when a connection subscribes, then each
/// later set, the way a provider that refreshes a token does.
struct Rotating(watch::Sender<BasicAuth>);

impl StreamingCredentialsProvider for Rotating {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        let mut receiver = self.0.subscribe();
        let current = receiver.borrow_and_update().clone();
        let later = stream::unfold(receiver, |mut receiver| async move {
            receiver.changed().await.ok()?;
            let auth = receiver.borrow_and_update().clone();
            Some((Ok(auth), receiver))
        });
        Box::pin(stream::iter([Ok(current)]).chain(later))
    }
}

/// A provider that starts with `auth`, and the sender that rotates it.
fn rotating(auth: &BasicAuth) -> (Provider, watch::Sender<BasicAuth>) {
    let (rotations, _) = watch::channel(auth.clone());
    (Provider(Arc::new(Rotating(rotations.clone()))), rotations)
}

/// The suite's server, or `None` on the memory backend.
fn server_url() -> Option<String> {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

/// A backend on the suite's server that takes its credentials from
/// `provider` in place of the ones in the suite's URL.
fn open(url: &str, provider: &Provider) -> Arc<Backend> {
    let (scheme, rest) = url.split_once("://").unwrap();
    let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let url = format!("{scheme}://{host}");
    Arc::new(Backend::open(&url, Some(provider.clone()), Settings::default()).unwrap())
}

/// A connection, as the suite's user, to each node of the suite's server:
/// the one server, the Sentinel primary, or each node of the cluster.  A
/// cluster keeps its ACL users and its client list per node.
async fn nodes(url: &str) -> Vec<Connection> {
    let backend = Backend::open(url, None, Settings::default()).unwrap();
    let mut seed = backend.connect().await.unwrap();
    if !backend.is_cluster() {
        return vec![seed];
    }
    let listing: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut seed)
        .await
        .unwrap();
    let mut nodes = Vec::new();
    for line in listing.lines() {
        let address = line.split_whitespace().nth(1).unwrap();
        let address = address.split('@').next().unwrap();
        let node = Backend::open(&format!("redis://{address}/0"), None, Settings::default());
        nodes.push(node.unwrap().connect().await.unwrap());
    }
    nodes
}

/// Two users of every node with the same access, named for `test` so that
/// tests running at once never count each other's connections: the user
/// before a rotation and the user after it.  Setting a user again changes
/// nothing, so a test sets them on every run.
async fn users(nodes: &mut [Connection], test: &str) -> [BasicAuth; 2] {
    let users = ["before", "after"]
        .map(|when| BasicAuth::new(format!("docket-test-{test}-{when}"), format!("{when}-pw")));
    for node in nodes.iter_mut() {
        for user in &users {
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg(user.username())
                .arg("on")
                .arg(format!(">{}", user.password()))
                .arg("~*")
                .arg("&*")
                .arg("+@all")
                .query_async::<()>(node)
                .await
                .unwrap();
        }
    }
    users
}

/// The user of each command connection, on every node, that is
/// authenticated as one of `users`, sorted.  Redis flags a subscribed
/// connection `P`, and these leave it out.
async fn command_connections(nodes: &mut [Connection], users: &[BasicAuth; 2]) -> Vec<String> {
    let mut found = Vec::new();
    for node in nodes.iter_mut() {
        let listing: String = redis::cmd("CLIENT")
            .arg("LIST")
            .query_async(node)
            .await
            .unwrap();
        found.extend(listing.lines().filter_map(|client| {
            let field = |name: &str| {
                client
                    .split_whitespace()
                    .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
                    .unwrap()
            };
            let user = field("user");
            let ours = users.iter().any(|candidate| candidate.username() == user);
            (ours && !field("flags").contains('P')).then(|| user.to_owned())
        }));
    }
    found.sort();
    found
}

/// Waits until the command connections of `users` are `expected`, and
/// fails with what they are if they are not within ten seconds.
async fn settle(nodes: &mut [Connection], users: &[BasicAuth; 2], expected: &[String]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = command_connections(nodes, users).await;
        if found == expected || tokio::time::Instant::now() > deadline {
            assert_eq!(found, expected);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs a command on keys `b`, `c`, and `a`, which hash to slots 3300,
/// 7365, and 15495: one in each third of the slots, so one on each node of
/// the test cluster.
async fn touch(connection: &mut (impl ConnectionLike + Send)) {
    for key in ["b", "c", "a"] {
        redis::cmd("EXISTS")
            .arg(format!("docket-test-rotation-{key}"))
            .query_async::<i64>(connection)
            .await
            .unwrap();
    }
}

/// Each kind of connection a docket opens on `backend`, each after a
/// command: its shared connection, the connection a blocking loop opens for
/// itself, an application's connection, and a subscription.  The
/// subscription follows the provider on its own, and must not keep the
/// command connections from following it too.
async fn hold(backend: Arc<Backend>) -> (Arc<Shared>, Connection, Connection, Subscription) {
    let shared = Arc::new(Shared::new(Arc::clone(&backend)));
    touch(&mut shared.get().await.unwrap()).await;
    let mut blocking = backend.connect().await.unwrap();
    touch(&mut blocking).await;
    let mut application = backend.connect_for_application().await.unwrap();
    touch(&mut application).await;
    let topics = Topics::Channels(vec!["docket-test-rotation".to_owned()]);
    let subscription = backend.subscribe(topics).await.unwrap();
    (shared, blocking, application, subscription)
}

/// Each backend holds three command connections to each node: on a cluster,
/// each of its three cluster connections opens one to every node.
#[rstest]
#[case::one_backend("one-backend", 1)]
#[case::backends_sharing_a_provider("shared-provider", 2)]
#[tokio::test]
async fn a_rotation_reauthenticates_every_command_connection(
    #[case] test: &str,
    #[case] backends: usize,
) {
    let Some(url) = server_url() else { return };
    let mut nodes = nodes(&url).await;
    let users = users(&mut nodes, test).await;
    let [before, after] = &users;
    let (provider, rotations) = rotating(before);
    let _held = futures::future::join_all((0..backends).map(|_| hold(open(&url, &provider)))).await;
    let opened = 3 * nodes.len() * backends;
    assert_eq!(
        command_connections(&mut nodes, &users).await,
        vec![before.username().to_owned(); opened]
    );

    rotations.send_replace(after.clone());

    settle(
        &mut nodes,
        &users,
        &vec![after.username().to_owned(); opened],
    )
    .await;
}

#[tokio::test]
async fn a_connection_opened_after_a_rotation_authenticates_as_the_new_user() {
    let Some(url) = server_url() else { return };
    let mut nodes = nodes(&url).await;
    let users = users(&mut nodes, "opened-after").await;
    let [before, after] = &users;
    let (provider, rotations) = rotating(before);
    let backend = open(&url, &provider);

    rotations.send_replace(after.clone());
    let _held = hold(backend).await;

    assert_eq!(
        command_connections(&mut nodes, &users).await,
        vec![after.username().to_owned(); 3 * nodes.len()]
    );
}
