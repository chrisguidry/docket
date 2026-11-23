use std::pin::Pin;
use std::time::Duration;

use docket::{BasicAuth, Docket, Event, Events, State, StreamingCredentialsProvider};
use futures::{Stream, StreamExt, stream};
use tokio::sync::broadcast;

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

/// Gives its credentials, then the same credentials again each time the
/// test rotates them, the way a provider refreshes a token.
struct Rotating {
    auth: BasicAuth,
    rotations: broadcast::Sender<()>,
}

impl StreamingCredentialsProvider for Rotating {
    fn subscribe(
        &self,
    ) -> Pin<Box<dyn Stream<Item = redis::RedisResult<BasicAuth>> + Send + 'static>> {
        let auth = self.auth.clone();
        let later = stream::unfold(self.rotations.subscribe(), move |mut rotations| {
            let auth = auth.clone();
            async move { rotations.recv().await.ok().map(|()| (Ok(auth), rotations)) }
        });
        Box::pin(stream::iter([Ok(self.auth.clone())]).chain(later))
    }
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
) -> (Docket, Events, broadcast::Sender<()>) {
    let (url, Fixed(auth)) = split(&proxy.url());
    let (rotations, _) = broadcast::channel(8);
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), url)
        .credentials_provider(Rotating {
            auth,
            rotations: rotations.clone(),
        })
        .connect()
        .await
        .unwrap();
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let execution = docket.add(Echo::new("rotated")).await.unwrap();
    let events = execution.subscribe().await.unwrap();
    (docket, events, rotations)
}

#[tokio::test]
async fn a_rotation_moves_a_runs_events_to_a_new_connection() {
    let Some(proxy) = proxy().await else { return };
    let (docket, events, rotations) = events_through(&proxy).await;
    let subscribed = proxy.count("SUBSCRIBE");

    rotations.send(()).unwrap();
    let reading = tokio::spawn(completes(events));
    until(|| proxy.count("UNSUBSCRIBE") == 1).await;
    assert_eq!(proxy.count("SUBSCRIBE"), subscribed + 1);

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    assert!(within(10, reading).await.unwrap());
}

#[tokio::test]
async fn a_failed_move_keeps_the_subscription_it_has() {
    let Some(proxy) = proxy().await else { return };
    let (docket, events, rotations) = events_through(&proxy).await;
    let subscribed = proxy.count("SUBSCRIBE");

    proxy.fail("SUBSCRIBE", 1);
    rotations.send(()).unwrap();
    let reading = tokio::spawn(completes(events));
    until(|| proxy.count("SUBSCRIBE") == subscribed + 1).await;

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();
    assert!(within(10, reading).await.unwrap());
}
