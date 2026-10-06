use std::pin::Pin;

use docket::{BasicAuth, Docket, Event, State, StreamingCredentialsProvider};
use futures::{Stream, StreamExt, stream};

use crate::support::{Echo, url, within, worker};

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
