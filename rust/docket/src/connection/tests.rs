use std::sync::Arc;

use redis::aio::ConnectionLike;
use rstest::rstest;

use super::url::SentinelUrl;
use super::{Backend, Provider, Settings, Shared, sentinel_client};

/// Opens a backend with the default connection settings.
fn open(url: &str, credentials: Option<Provider>) -> crate::Result<Backend> {
    Backend::open(url, credentials, Settings::default())
}

fn sentinel_url(tls: bool, credentials: Option<&str>) -> SentinelUrl {
    let credential = credentials.map(str::to_owned);
    SentinelUrl {
        sentinels: vec![
            ("sentinel-one".to_owned(), 26379),
            ("sentinel-two".to_owned(), 26380),
        ],
        service: "mymaster".to_owned(),
        db: 2,
        tls,
        username: credential.clone(),
        password: credential.clone(),
        daemon_username: credential.clone(),
        daemon_password: credential,
    }
}

#[rstest]
#[case::plain(false, None)]
#[case::tls_and_credentials(true, Some("secret"))]
fn a_sentinel_client_builds_without_connecting(
    #[case] tls: bool,
    #[case] credentials: Option<&str>,
) {
    assert!(sentinel_client(sentinel_url(tls, credentials), &Settings::default()).is_ok());
}

/// rustls has no crypto backend of its own, so without the one the `tls`
/// feature brings, a TLS connect panics before it reaches the network.
#[cfg(feature = "tls")]
#[tokio::test]
async fn a_tls_connect_reaches_the_network() {
    let backend = open("rediss://127.0.0.1:1", None).unwrap();
    let Err(error) = backend.connect().await else {
        panic!("nothing listens on port 1");
    };
    assert!(error.is_connection_refusal(), "{error}");
}

/// The Redis the suite runs against, or an in-process one.
fn url() -> String {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| "memory://connection-tests".to_owned())
}

#[tokio::test]
async fn a_shared_connection_reports_its_database() {
    let shared = Arc::new(Shared::new(Arc::new(open(&url(), None).unwrap())));
    let handle = shared.get().await.unwrap();
    assert_eq!(handle.get_db(), 0);
}

mod credentials {
    use std::pin::Pin;
    use std::sync::Arc;

    use futures::{Stream, stream};
    use redis::{BasicAuth, StreamingCredentialsProvider};
    use rstest::rstest;

    use super::super::Provider;
    use super::open;

    /// A provider whose stream ends without giving credentials.
    struct Empty;

    impl StreamingCredentialsProvider for Empty {
        fn subscribe(
            &self,
        ) -> Pin<Box<dyn Stream<Item = redis::RedisResult<BasicAuth>> + Send + 'static>> {
            Box::pin(stream::empty())
        }
    }

    fn empty() -> Provider {
        Provider(Arc::new(Empty))
    }

    /// Gives one set of credentials and keeps the stream open.
    struct Fixed(BasicAuth);

    impl StreamingCredentialsProvider for Fixed {
        fn subscribe(
            &self,
        ) -> Pin<Box<dyn Stream<Item = redis::RedisResult<BasicAuth>> + Send + 'static>> {
            Box::pin(futures::StreamExt::chain(
                stream::iter([Ok(self.0.clone())]),
                stream::pending(),
            ))
        }
    }

    /// The suite's URL without credentials, and a provider that gives them,
    /// or the default user, which takes any password on a server with none.
    fn split(url: &str) -> (String, Option<Provider>) {
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
                url.to_owned(),
                BasicAuth::new("default".into(), "anything".into()),
            ),
        };
        (url, Some(Provider(Arc::new(Fixed(auth)))))
    }

    #[rstest]
    #[case::memory("memory://credentials")]
    #[case::standalone("redis://localhost:1/0")]
    #[case::cluster("redis+cluster://localhost:1")]
    #[case::sentinel("redis+sentinel://localhost:1/mymaster")]
    fn every_kind_of_url_takes_a_provider(#[case] url: &str) {
        assert!(open(url, split(url).1).is_ok());
        assert!(open(url, None).is_ok());
    }

    #[rstest]
    #[case::no_scheme("localhost:6379")]
    #[case::bad_port("redis://localhost:port/0")]
    #[case::bad_cluster_port("redis+cluster://localhost:port")]
    fn a_url_docket_cannot_use_does_not_open(#[case] url: &str) {
        assert!(open(url, split("redis://x").1).is_err());
    }

    #[tokio::test]
    async fn a_subscription_opens_without_a_provider() {
        let backend = open(&super::url(), None).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_ok());
    }

    #[tokio::test]
    async fn an_unreachable_sentinel_fails_a_subscription() {
        let backend = open("redis+sentinel://localhost:1/mymaster", None).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_err());
    }

    #[tokio::test]
    async fn a_subscription_opens_with_the_providers_credentials() {
        let (url, provider) = split(&super::url());
        let backend = open(&url, provider).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_ok());
    }

    #[rstest]
    #[case::standalone("redis://user:pass@localhost:6379/0")]
    #[case::password_only("redis://:pass@localhost:6379/0")]
    #[case::cluster("redis+cluster://user:pass@localhost:7000")]
    fn a_url_with_credentials_refuses_a_provider(#[case] url: &str) {
        let error = open(url, Some(empty())).err().unwrap();
        assert!(
            error
                .to_string()
                .ends_with("it carries credentials, and a credential provider gives them too"),
            "{error}"
        );
    }

    #[test]
    fn a_provider_is_fine_without_url_credentials() {
        assert!(open("redis://localhost:6379/0", Some(empty())).is_ok());
        assert!(open("redis+sentinel://localhost:26379/mymaster", Some(empty())).is_ok());
        assert_eq!(format!("{:?}", empty()), "Provider(..)");
    }

    #[tokio::test]
    async fn a_provider_that_gives_nothing_fails_a_subscription() {
        let backend = open("redis://localhost:1/0", Some(empty())).unwrap();
        let error = backend
            .subscribe(&["channel".to_owned()])
            .await
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("the credential provider gave no credentials"),
            "{error}"
        );
    }
}

mod settings {
    use std::net::SocketAddr;
    use std::time::Duration;

    use redis::{AsyncCommands, RedisResult};
    use rstest::rstest;
    use tokio::net::TcpListener;

    use super::super::{Backend, Settings, slots};
    use crate::Docket;

    /// The address of a server that accepts connections and never answers.
    async fn silent() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        address
    }

    #[test]
    fn a_backend_refuses_settings_that_would_fail_every_connection() {
        let settings = Settings {
            response_timeout: Some(Duration::ZERO),
            ..Settings::default()
        };
        let error = Backend::open("memory://settings", None, settings).err();
        assert!(matches!(error, Some(crate::Error::Invalid(_))), "{error:?}");
    }

    #[tokio::test]
    async fn a_subscription_to_a_silent_server_times_out() {
        let settings = Settings {
            connection_timeout: Duration::from_millis(200),
            response_timeout: Some(Duration::from_millis(200)),
            ..Settings::default()
        };
        let url = format!("redis://{}/0", silent().await);
        let backend = Backend::open(&url, None, settings).unwrap();
        let subscribed = tokio::time::timeout(
            Duration::from_secs(3),
            backend.subscribe(&["channel".to_owned()]),
        )
        .await
        .expect("the subscription gives up in time");
        assert!(subscribed.is_err());
    }

    /// A slot move makes the cluster redirect commands to the slot's new
    /// node, and a command with no retries left fails on the redirect.
    #[rstest]
    #[case::default(None, true)]
    #[case::no_retries(Some(0), false)]
    #[tokio::test]
    async fn retries_carry_a_command_across_a_slot_move(
        #[case] retries: Option<u32>,
        #[case] succeeds: bool,
    ) {
        let Some(url) = slots::cluster_url() else {
            return;
        };
        let mut builder = Docket::builder(format!("retries-{}", uuid::Uuid::now_v7()), &url);
        if let Some(retries) = retries {
            builder = builder.retries(retries);
        }
        let docket = builder.connect().await.unwrap();
        let key = format!("{}:moved", docket.keys().prefix());
        let mut handle = docket.handle();
        let () = handle.set(&key, "moved").await.unwrap();

        slots::move_slot(&url, &key).await;
        let read: RedisResult<String> = handle.get(&key).await;
        assert_eq!(read.is_ok(), succeeds, "{read:?}");
    }
}
