//! A subscription with a credentials provider sends AUTH on its own
//! connection each time the backend passes on new credentials.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use redis::{BasicAuth, Client, RedisResult, StreamingCredentialsProvider};
use tokio::sync::broadcast;

use super::super::super::{Backend, Checked, Provider, Server, Settings};
use super::super::Subscription;
use super::{
    Rotating, Rotation, channel, channels, client_info, field, on_a_server, publish, read, split,
    suite_url, until, users, without_a_provider,
};
use crate::telemetry::tests::Logs;

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
/// don't wait for a reader.  Only RESP3 lets a subscribed connection, which
/// Redis flags `P`, answer `CLIENT INFO`.  Redis 6.2 reports no `resp`
/// field, so the flag and the answer show the protocol there too.
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
    assert!(field(&after, "flags").contains('P'), "{after}");
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

/// The backend checks the refused set and skips it, so the subscription
/// never sends it.
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
    logs.wait_for("the server refused the credential provider's new credentials")
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

/// A backend on the suite's server whose checks of new credentials can't
/// reach any server, so it passes on every set unchecked.  The sender
/// rotates them.
fn unchecked() -> (Backend, broadcast::Sender<Rotation>) {
    let (url, auth) = split(&suite_url());
    let (rotations, _) = broadcast::channel(64);
    let provider = Provider(Arc::new(Rotating {
        auth,
        rotations: rotations.clone(),
    }));
    let mut backend = Backend::open(&url, Some(provider.clone()), Settings::default()).unwrap();
    let nowhere = Client::open("redis://127.0.0.1:1/0").unwrap();
    let server = Server::Direct(Box::new(nowhere));
    backend.credentials = Some(Checked::new(provider, server, Duration::from_secs(1)));
    (backend, rotations)
}

/// A set the check could not run on reaches the subscription, and the
/// server can still refuse its AUTH there.  New connections from the
/// backend would take the refused set, so the test publishes through a
/// backend without the provider.
#[tokio::test]
async fn a_refused_auth_on_the_subscription_keeps_it_delivering() {
    let (logs, _guard) = Logs::capture();
    if on_a_server().is_none() {
        return;
    }
    let (backend, rotations) = unchecked();
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
    publish(&without_a_provider(), &channel, 0..3).await;
    assert_eq!(read(&mut subscription, 3).await, vec![0, 1, 2]);
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

/// A run's events can outlive the docket that subscribed to them.  The
/// subscription goes on delivering, and the task that renews its
/// credentials ends once nothing passes on new ones.  The test publishes
/// through a backend without the provider, because a connection from the
/// subscription's own backend would keep the credentials alive.
#[tokio::test]
async fn a_subscription_outlives_its_backend_and_its_renewal_ends() {
    let Some((backend, _rotations)) = on_a_server() else {
        return;
    };
    let channel = channel();
    let mut subscription = backend.subscribe(channels(&channel)).await.unwrap();

    drop(backend);
    until(|| subscription.renewal.as_ref().unwrap().task.is_finished()).await;

    publish(&without_a_provider(), &channel, 0..1).await;
    assert_eq!(read(&mut subscription, 1).await, vec![0]);
}
