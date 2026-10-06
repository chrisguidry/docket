use std::sync::Arc;

use redis::aio::ConnectionLike;
use rstest::rstest;

use super::url::SentinelUrl;
use super::{Backend, Shared, sentinel_client};

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
    assert!(sentinel_client(sentinel_url(tls, credentials)).is_ok());
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
    let shared = Arc::new(Shared::new(Arc::new(Backend::open(&url(), None).unwrap())));
    let handle = shared.get().await.unwrap();
    assert_eq!(handle.get_db(), 0);
}

mod credentials {
    use std::pin::Pin;
    use std::sync::Arc;

    use futures::{Stream, stream};
    use redis::{BasicAuth, StreamingCredentialsProvider};
    use rstest::rstest;

    use super::super::{Backend, Provider};

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
        assert!(Backend::open(url, split(url).1).is_ok());
        assert!(Backend::open(url, None).is_ok());
    }

    #[rstest]
    #[case::no_scheme("localhost:6379")]
    #[case::bad_port("redis://localhost:port/0")]
    #[case::bad_cluster_port("redis+cluster://localhost:port")]
    fn a_url_docket_cannot_use_does_not_open(#[case] url: &str) {
        assert!(Backend::open(url, split("redis://x").1).is_err());
    }

    #[tokio::test]
    async fn a_subscription_opens_without_a_provider() {
        let backend = Backend::open(&super::url(), None).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_ok());
    }

    #[tokio::test]
    async fn an_unreachable_sentinel_fails_a_subscription() {
        let backend = Backend::open("redis+sentinel://localhost:1/mymaster", None).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_err());
    }

    #[tokio::test]
    async fn a_subscription_opens_with_the_providers_credentials() {
        let (url, provider) = split(&super::url());
        let backend = Backend::open(&url, provider).unwrap();
        assert!(backend.subscribe(&["channel".to_owned()]).await.is_ok());
    }

    #[rstest]
    #[case::standalone("redis://user:pass@localhost:6379/0")]
    #[case::password_only("redis://:pass@localhost:6379/0")]
    #[case::cluster("redis+cluster://user:pass@localhost:7000")]
    fn a_url_with_credentials_refuses_a_provider(#[case] url: &str) {
        let error = Backend::open(url, Some(empty())).err().unwrap();
        assert!(
            error
                .to_string()
                .ends_with("it carries credentials, and a credential provider gives them too"),
            "{error}"
        );
    }

    #[test]
    fn a_provider_is_fine_without_url_credentials() {
        assert!(Backend::open("redis://localhost:6379/0", Some(empty())).is_ok());
        assert!(Backend::open("redis+sentinel://localhost:26379/mymaster", Some(empty())).is_ok());
        assert_eq!(format!("{:?}", empty()), "Provider(..)");
    }

    #[tokio::test]
    async fn a_provider_that_gives_nothing_fails_a_subscription() {
        let backend = Backend::open("redis://localhost:1/0", Some(empty())).unwrap();
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
