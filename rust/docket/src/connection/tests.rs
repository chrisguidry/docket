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
    let shared = Arc::new(Shared::new(Arc::new(Backend::open(&url()).unwrap())));
    let handle = shared.get().await.unwrap();
    assert_eq!(handle.get_db(), 0);
}
