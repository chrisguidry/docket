use std::collections::BTreeSet;
use std::pin::Pin;
use std::time::Duration;

use docket::{
    BasicAuth, Docket, Event, Events, RedisConnection, State, StreamingCredentialsProvider,
};
use futures::{Stream, StreamExt, stream};
use tokio::sync::watch;

use crate::support::{Echo, proxy, url, within, worker};

/// Gives one set of credentials and keeps the stream open, the way a
/// provider for tokens that never expire would.
struct Fixed(BasicAuth);

impl StreamingCredentialsProvider for Fixed {
    fn subscribe(
        &self,
    ) -> Pin<Box<dyn Stream<Item = redis::RedisResult<BasicAuth>> + Send + 'static>> {
        Box::pin(stream::iter([Ok(self.0.clone())]).chain(stream::pending()))
    }
}

/// The test URL without its credentials, and a provider that gives them.
/// Without credentials in the URL, the provider gives the default user,
/// which takes any password on a server with none.
fn split(url: &str) -> (String, Fixed) {
    let (scheme, rest) = url.split_once("://").unwrap();
    match rest.split_once('@') {
        Some((userinfo, host)) => {
            let (user, password) = userinfo.split_once(':').unwrap();
            (
                format!("{scheme}://{host}"),
                Fixed(BasicAuth::new(user.into(), password.into())),
            )
        }
        None => (
            url.to_owned(),
            Fixed(BasicAuth::new("default".into(), "anything".into())),
        ),
    }
}

#[tokio::test]
async fn a_docket_runs_with_credentials_from_a_provider() {
    let (url, provider) = split(&url());
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url)
        .credentials_provider(provider)
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("authenticated")).await.unwrap();
    let mut events = execution.subscribe().await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(execution.result().await.unwrap(), "authenticated");
    let first = within(10, events.next()).await.unwrap().unwrap();
    assert!(
        matches!(first, Event::State(state) if state.state == State::Queued || state.state.is_terminal())
    );
}

/// Gives the credentials in force when a connection subscribes, then each
/// later set, the way a provider that refreshes a token does.
struct Rotating(watch::Sender<BasicAuth>);

impl StreamingCredentialsProvider for Rotating {
    fn subscribe(
        &self,
    ) -> Pin<Box<dyn Stream<Item = redis::RedisResult<BasicAuth>> + Send + 'static>> {
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

/// Gives the same credentials again, the way a provider refreshes a token
/// that keeps its user.
fn refresh(rotations: &watch::Sender<BasicAuth>) {
    rotations.send_modify(|_| ());
}

/// Reads a run's events until it finishes, and returns whether it completed.
async fn completes(mut events: Events) -> bool {
    while let Some(event) = events.next().await {
        if let Event::State(state) = event.unwrap()
            && state.state.is_terminal()
        {
            return state.state == State::Completed;
        }
    }
    false
}

async fn until(condition: impl Fn() -> bool) {
    within(10, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// A run's events, through `proxy`, from a docket whose provider the
/// returned sender rotates.
async fn events_through(
    proxy: &crate::support::proxy::Proxy,
) -> (Docket, Events, watch::Sender<BasicAuth>) {
    let (url, Fixed(auth)) = split(&proxy.url());
    let (rotations, _) = watch::channel(auth);
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url)
        .credentials_provider(Rotating(rotations.clone()))
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("rotated")).await.unwrap();
    let events = execution.subscribe().await.unwrap();
    (docket, events, rotations)
}

#[tokio::test]
async fn a_rotation_authenticates_a_runs_events_again_on_their_connection() {
    let Some(proxy) = proxy().await else { return };
    let (docket, events, rotations) = events_through(&proxy).await;
    let subscribed = proxy.count("SUBSCRIBE");

    refresh(&rotations);
    until(|| proxy.count_on_subscribers("AUTH") == 1).await;
    assert_eq!(proxy.count("SUBSCRIBE"), subscribed);

    let reading = tokio::spawn(completes(events));
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    assert!(within(10, reading).await.unwrap());
}

#[tokio::test]
async fn a_refused_auth_keeps_a_runs_events_arriving() {
    let Some(proxy) = proxy().await else { return };
    let (docket, events, rotations) = events_through(&proxy).await;

    proxy.fail_on_subscribers("AUTH", 1);
    refresh(&rotations);
    until(|| proxy.count_on_subscribers("AUTH") == 1).await;
    refresh(&rotations);
    until(|| proxy.count_on_subscribers("AUTH") == 2).await;

    let reading = tokio::spawn(completes(events));
    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    assert!(within(10, reading).await.unwrap());
}

/// A connection, as the suite's user, to each node of the suite's server:
/// the one server, the Sentinel primary, or each node of the cluster.  A
/// cluster keeps its ACL users and its client list per node.
async fn nodes(url: &str) -> Vec<RedisConnection> {
    let connect = |url: String| async move {
        let admin = Docket::builder("docket-test-admin", url).connect().await;
        admin.unwrap().redis().await.unwrap()
    };
    let mut seed = connect(url.to_owned()).await;
    if !url.starts_with("redis+cluster://") {
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
        nodes.push(connect(format!("redis://{address}/0")).await);
    }
    nodes
}

/// Two users of every node with the same access: the user before a
/// rotation and the user after it.  Setting a user again changes nothing,
/// so the test sets them on every run, after the run before revoked one.
async fn users(nodes: &mut [RedisConnection]) -> [BasicAuth; 2] {
    let users = ["before", "after"]
        .map(|when| BasicAuth::new(format!("docket-test-worker-{when}"), format!("{when}-pw")));
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

/// The users that the connections authenticated as one of `users` are
/// authenticated as, on every node, subscribed or not.
async fn connected_as(nodes: &mut [RedisConnection], users: &[BasicAuth; 2]) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for node in nodes.iter_mut() {
        let listing: String = redis::cmd("CLIENT")
            .arg("LIST")
            .query_async(node)
            .await
            .unwrap();
        found.extend(listing.lines().filter_map(|client| {
            let user = client
                .split_whitespace()
                .find_map(|pair| pair.strip_prefix("user="))
                .unwrap();
            users
                .iter()
                .any(|candidate| candidate.username() == user)
                .then(|| user.to_owned())
        }));
    }
    found
}

/// Waits until every connection of `users` is authenticated as `user`, and
/// fails with what they are if they are not within ten seconds.
async fn settle(nodes: &mut [RedisConnection], users: &[BasicAuth; 2], user: &BasicAuth) {
    let expected = BTreeSet::from([user.username().to_owned()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = connected_as(nodes, users).await;
        if found == expected || tokio::time::Instant::now() > deadline {
            assert_eq!(found, expected);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Deletes `user` from every node, which closes its connections.
async fn revoke(nodes: &mut [RedisConnection], user: &BasicAuth) {
    for node in nodes.iter_mut() {
        redis::cmd("ACL")
            .arg("DELUSER")
            .arg(user.username())
            .query_async::<()>(node)
            .await
            .unwrap();
    }
}

/// The server closes a connection whose token expired, as it does for a
/// user it revokes.  So once the rotation reaches every connection, the old
/// user goes, and the running worker must take and finish the next task.
///
/// A blocking read takes the AUTH that a rotation queues behind it only
/// when its block ends, and the strike list blocks for up to a minute.  A
/// response timeout of two seconds limits every block to one second.
#[tokio::test]
async fn a_worker_runs_tasks_before_and_after_a_rotation() {
    let Some(suite) = std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        return;
    };
    let mut nodes = nodes(&suite).await;
    let users = users(&mut nodes).await;
    let [before, after] = &users;
    let (url, _) = split(&suite);
    let (rotations, _) = watch::channel(before.clone());
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url)
        .credentials_provider(Rotating(rotations.clone()))
        .response_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let running = tokio::spawn(worker(&docket).run_forever());
    let first = docket.add(Echo::new("before")).await.unwrap();
    assert_eq!(within(10, first.result()).await.unwrap(), "before");

    rotations.send_replace(after.clone());
    settle(&mut nodes, &users, after).await;
    revoke(&mut nodes, before).await;

    let second = docket.add(Echo::new("after")).await.unwrap();
    assert_eq!(within(10, second.result()).await.unwrap(), "after");
    running.abort();
}
