pub mod proxy;
pub mod telemetry;

use std::time::Duration;

use docket::{Docket, DocketBuilder, Task, Worker};
use serde::{Deserialize, Serialize};

/// Where the tests' Redis is.  Each `memory://` URL is its own server, so
/// tests on the in-process engine never share data.
pub fn url() -> String {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| format!("memory://{}", uuid::Uuid::now_v7()))
}

/// A docket of the test's own.
pub async fn docket() -> Docket {
    docket_with(|builder| builder).await
}

/// A docket of the test's own, with the settings `configure` chooses.
pub async fn docket_with(configure: impl FnOnce(DocketBuilder) -> DocketBuilder) -> Docket {
    telemetry::install();
    let url = url();
    let builder = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), &url);
    let docket = configure(builder)
        .connect()
        .await
        .expect("the test docket connects");
    URLS.lock().unwrap().insert(docket.name().to_owned(), url);
    docket
}

static URLS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
    std::sync::LazyLock::new(Default::default);

/// The URL a test docket connected to, for a second connection to it.
pub fn shared_url(docket: &Docket) -> String {
    URLS.lock().unwrap()[docket.name()].clone()
}

/// A worker that polls often, so tests finish quickly.
pub fn worker(docket: &Docket) -> Worker {
    Worker::new(docket.clone())
        .minimum_check_interval(Duration::from_millis(20))
        .scheduling_resolution(Duration::from_millis(20))
}

/// A task that echoes its argument.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "echo", output = String)]
pub struct Echo {
    pub text: String,
}

impl Echo {
    pub fn new(text: &str) -> Self {
        Self {
            text: text.to_owned(),
        }
    }
}

/// A task that does nothing.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "noop")]
pub struct Noop;

/// Runs `future`, and fails the test if it takes longer than `seconds`.
pub async fn within<F: std::future::Future>(seconds: u64, future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(seconds), future)
        .await
        .expect("the test finishes in time")
}

/// A fault proxy in front of the test Redis, or `None` when the tests run
/// against something a plain TCP proxy cannot stand in for, such as the
/// in-process engine or a cluster.
pub async fn proxy() -> Option<proxy::Proxy> {
    let url = std::env::var("DOCKET_TEST_URL").ok()?;
    let authority = url.strip_prefix("redis://")?.split('/').next()?;
    let (userinfo, upstream) = match authority.rsplit_once('@') {
        Some((userinfo, upstream)) => (Some(userinfo.to_owned()), upstream),
        None => (None, authority),
    };
    Some(proxy::Proxy::start(upstream.to_owned(), userinfo).await)
}

/// A docket of the test's own, through `proxy`.
pub async fn docket_through(proxy: &proxy::Proxy) -> Docket {
    telemetry::install();
    Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), proxy.url())
        .connect()
        .await
        .expect("the test docket connects")
}
