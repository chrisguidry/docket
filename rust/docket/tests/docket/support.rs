use std::time::Duration;

use docket::{Docket, Task, Worker};
use serde::{Deserialize, Serialize};

/// Where the tests' Redis is.  Each `memory://` URL is its own server, so
/// tests on the in-process engine never share data.
pub fn url() -> String {
    std::env::var("DOCKET_TEST_URL")
        .unwrap_or_else(|_| format!("memory://{}", uuid::Uuid::now_v7()))
}

/// A docket of the test's own.
pub async fn docket() -> Docket {
    let url = url();
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), &url)
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
