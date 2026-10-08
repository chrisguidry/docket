//! The builder's settings for connections to Redis: timeouts, retries, and
//! TCP keepalive.

use std::net::SocketAddr;
use std::time::Duration;

use docket::{Docket, Error};
use rstest::rstest;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::support::telemetry::points;
use crate::support::{Noop, docket_with, proxy, within, worker};

/// A server that accepts connections and never answers, like a Redis
/// behind a network that silently drops its packets.
struct Silent {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Silent {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        Self { address, task }
    }
}

impl Drop for Silent {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[rstest]
#[case::standalone("redis", "")]
#[case::cluster("redis+cluster", "")]
#[case::sentinel("redis+sentinel", "/mymaster")]
#[tokio::test]
async fn a_connect_to_a_silent_server_fails_after_the_connection_timeout(
    #[case] scheme: &str,
    #[case] path: &str,
) {
    let silent = Silent::start().await;
    let docket = Docket::builder("silent", format!("{scheme}://{}{path}", silent.address))
        .connection_timeout(Duration::from_millis(200))
        .connect()
        .await
        .unwrap();
    let error = within(3, docket.snapshot()).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");
}

#[tokio::test]
async fn a_command_redis_never_answers_fails_after_the_response_timeout() {
    let Some(proxy) = proxy().await else { return };
    let docket = Docket::builder(format!("docket-test-{}", uuid::Uuid::now_v7()), proxy.url())
        .response_timeout(Duration::from_millis(300))
        .connect()
        .await
        .unwrap();
    docket.snapshot().await.unwrap();

    proxy.silence();
    let error = within(3, docket.snapshot()).await.unwrap_err();
    assert!(error.is_redis_unavailable(), "{error}");

    proxy.heal();
    within(10, async {
        while docket.snapshot().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

/// The keepalive timers of the established TCP connections to `port`, in
/// seconds, read from the kernel's socket table.  A connection without
/// keepalive has no timer.  The kernel reports timers in hundredths of a
/// second.
#[cfg(target_os = "linux")]
fn keepalive_timers(port: u16) -> Vec<Option<u64>> {
    let remote = format!(":{port:04X}");
    ["/proc/net/tcp", "/proc/net/tcp6"]
        .iter()
        .filter_map(|table| std::fs::read_to_string(table).ok())
        .flat_map(|table| {
            table
                .lines()
                .skip(1)
                .map(|line| line.split_whitespace().map(str::to_owned).collect())
                .collect::<Vec<Vec<String>>>()
        })
        .filter(|fields| fields[2].ends_with(&remote) && fields[3] == "01")
        .map(|fields| {
            let (kind, when) = fields[5].split_once(':').unwrap();
            (kind == "02").then(|| u64::from_str_radix(when, 16).unwrap() / 100)
        })
        .collect()
}

#[cfg(target_os = "linux")]
#[rstest]
#[case::default(None, 30)]
#[case::chosen(Some(600), 600)]
#[tokio::test]
async fn connections_probe_an_idle_peer_after_the_keepalive_idle_time(
    #[case] idle: Option<u64>,
    #[case] expected: u64,
) {
    let silent = Silent::start().await;
    let mut builder = Docket::builder("keepalive", format!("redis://{}/0", silent.address))
        .connection_timeout(Duration::from_secs(30));
    if let Some(idle) = idle {
        builder = builder.tcp_keepalive(Duration::from_secs(idle), Duration::from_secs(5), 3);
    }
    let docket = builder.connect().await.unwrap();
    let snapshot = tokio::spawn(async move { docket.snapshot().await });

    let timers = within(5, async {
        loop {
            let timers = keepalive_timers(silent.address.port());
            if !timers.is_empty() {
                return timers;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    snapshot.abort();
    for timer in timers {
        let seconds = timer.expect("the connection has a keepalive timer");
        assert!(
            (expected - 5..=expected).contains(&seconds),
            "{seconds} seconds"
        );
    }
}

#[tokio::test]
async fn the_strike_monitor_blocks_for_less_than_the_response_timeout() {
    let docket = docket_with(|builder| builder.response_timeout(Duration::from_millis(400))).await;
    docket.strikes_loaded().await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(points(&docket, "docket_redis_disruptions"), vec![]);
}

#[tokio::test]
async fn a_worker_blocks_for_less_than_the_response_timeout() {
    let docket = docket_with(|builder| builder.response_timeout(Duration::from_millis(400))).await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let worker = worker(&docket).minimum_check_interval(Duration::from_secs(5));
    let name = format!("docket.worker={}", worker.worker_name());
    let run = tokio::spawn(worker.run_until(tokio::time::sleep(Duration::from_millis(1500))));

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let execution = docket.add(Noop).await.unwrap();
    within(10, execution.result()).await.unwrap();
    within(10, run).await.unwrap().unwrap();

    let disruptions = points(&docket, "docket_redis_disruptions");
    assert!(
        disruptions
            .iter()
            .all(|(labels, _)| !labels.contains(&name)),
        "{disruptions:?}"
    );
}

#[rstest]
#[case::connection_timeout(Docket::builder("x", "memory://x").connection_timeout(Duration::ZERO))]
#[case::response_timeout(Docket::builder("x", "memory://x").response_timeout(Duration::ZERO))]
#[case::backoff(
    Docket::builder("x", "memory://x")
        .retry_backoff(Duration::from_secs(2), Duration::from_secs(1))
)]
#[case::idle(Docket::builder("x", "memory://x").tcp_keepalive(Duration::ZERO, Duration::from_secs(1), 3))]
#[case::interval(Docket::builder("x", "memory://x").tcp_keepalive(Duration::from_secs(1), Duration::ZERO, 3))]
#[case::probes(Docket::builder("x", "memory://x").tcp_keepalive(Duration::from_secs(1), Duration::from_secs(1), 0))]
#[tokio::test]
async fn settings_that_would_fail_every_connection_are_refused(
    #[case] builder: docket::DocketBuilder,
) {
    let error = builder.connect().await.unwrap_err();
    assert!(matches!(error, Error::Invalid(_)), "{error}");
}

#[rstest]
#[case::standalone(
    "redis://me:hunter2@localhost:6379/0",
    "redis://me:***@localhost:6379/0"
)]
#[case::password_only("redis://:hunter2@localhost:6379/0", "redis://:***@localhost:6379/0")]
#[case::cluster(
    "redis+cluster://me:hunter2@node:7000",
    "redis+cluster://me:***@node:7000"
)]
#[case::sentinel(
    "redis+sentinel://me:hunter2@s1/svc?sentinel_username=sen&sentinel_password=hunter2",
    "redis+sentinel://me:***@s1/svc?sentinel_username=sen&sentinel_password=***"
)]
#[case::no_password("redis://me@localhost:6379/0", "redis://me@localhost:6379/0")]
#[case::no_scheme("me:hunter2@localhost:6379", "me:***@localhost:6379")]
fn a_builder_never_shows_a_password(#[case] url: &str, #[case] shown: &str) {
    let debug = format!("{:?}", Docket::builder("x", url));
    assert!(debug.contains(&format!("url: {shown:?}")), "{debug}");
    assert!(!debug.contains("hunter2"), "{debug}");
}

#[rstest]
#[case::no_scheme("me:hunter2@localhost:6379")]
#[case::unknown_scheme("http://me:hunter2@localhost")]
#[case::sentinel_without_service("redis+sentinel://me:hunter2@s1?sentinel_password=hunter2")]
#[tokio::test]
async fn a_url_error_never_shows_a_password(#[case] url: &str) {
    let error = Docket::connect("x", url).await.unwrap_err();
    let message = error.to_string();
    assert!(matches!(error, Error::Url { .. }), "{message}");
    assert!(!message.contains("hunter2"), "{message}");
    assert!(!format!("{error:?}").contains("hunter2"), "{error:?}");
}
