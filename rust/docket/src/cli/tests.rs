use std::time::Duration;

use clap::{CommandFactory, Parser};
use rstest::rstest;

use super::{WorkerArgs, parse_duration};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    worker: WorkerArgs,
}

fn parse(arguments: &[&str]) -> WorkerArgs {
    Cli::try_parse_from(std::iter::once("app").chain(arguments.iter().copied()))
        .unwrap()
        .worker
}

#[rstest]
#[case::seconds("30", Duration::from_secs(30))]
#[case::seconds_suffix("5s", Duration::from_secs(5))]
#[case::milliseconds("250ms", Duration::from_millis(250))]
#[case::minutes("2m", Duration::from_secs(120))]
#[case::hours("1h", Duration::from_secs(3600))]
#[case::minutes_and_seconds("01:30", Duration::from_secs(90))]
#[case::hours_minutes_and_seconds("1:02:03", Duration::from_secs(3723))]
fn parses_durations(#[case] text: &str, #[case] expected: Duration) {
    assert_eq!(parse_duration(text), Ok(expected));
}

#[rstest]
#[case::word("soon")]
#[case::too_many_colons("1:2:3:4")]
#[case::bad_part("1:x")]
#[case::bad_millis("xms")]
#[case::empty("")]
fn rejects_durations(#[case] text: &str) {
    assert!(
        parse_duration(text)
            .unwrap_err()
            .starts_with(&format!("{text} is not a duration"))
    );
}

#[test]
fn defaults_match_pydocket() {
    let args = parse(&[]);
    assert_eq!(args.docket, "docket");
    assert_eq!(args.url, "redis://localhost:6379/0");
    assert_eq!(args.name, None);
    assert_eq!((args.concurrency, args.message_batch), (10, 1000));
    assert_eq!(args.redelivery_timeout, Duration::from_secs(300));
    assert_eq!(args.reconnection_delay, Duration::from_secs(5));
    assert_eq!(args.minimum_check_interval, Duration::from_millis(100));
    assert_eq!(args.scheduling_resolution, Duration::from_millis(250));
    assert!(args.schedule_automatic_tasks);
    assert!(!args.until_finished);
    assert_eq!((args.healthcheck_port, args.metrics_port), (None, None));
}

#[test]
fn reads_every_option() {
    let args = parse(&[
        "--docket",
        "orders",
        "--url",
        "memory://cli",
        "--name",
        "w1",
        "--concurrency",
        "3",
        "--message-batch",
        "7",
        "--redelivery-timeout",
        "1m",
        "--reconnection-delay",
        "2s",
        "--minimum-check-interval",
        "50ms",
        "--scheduling-resolution",
        "75ms",
        "--schedule-automatic-tasks",
        "false",
        "--until-finished",
        "--healthcheck-port",
        "8080",
        "--metrics-port",
        "9090",
    ]);
    assert_eq!(
        (args.docket.as_str(), args.url.as_str()),
        ("orders", "memory://cli")
    );
    assert_eq!(args.name.as_deref(), Some("w1"));
    assert_eq!((args.concurrency, args.message_batch), (3, 7));
    assert_eq!(args.redelivery_timeout, Duration::from_secs(60));
    assert!(!args.schedule_automatic_tasks);
    assert!(args.until_finished);
    assert_eq!(
        (args.healthcheck_port, args.metrics_port),
        (Some(8080), Some(9090))
    );
}

#[test]
fn debug_output_hides_the_url_passwords() {
    let args = parse(&[
        "--url",
        "redis+sentinel://docket:hunter2@sentinel:26379/mymaster?sentinel_password=swordfish",
    ]);
    let debug = format!("{args:?}");
    assert!(
        debug.contains("docket:***@sentinel:26379/mymaster?sentinel_password=***"),
        "{debug}"
    );
    assert!(!debug.contains("hunter2"));
    assert!(!debug.contains("swordfish"));
}

/// Prints the help.  `help_hides_the_url_from_the_environment` runs it in a
/// child process with `DOCKET_URL` set, because clap reads the environment
/// when it builds the command.
#[test]
#[ignore = "help_hides_the_url_from_the_environment runs it"]
fn print_help() {
    println!("{}", Cli::command().render_help());
}

#[test]
fn help_hides_the_url_from_the_environment() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "cli::tests::print_help",
            "--nocapture",
        ])
        .env("DOCKET_URL", "redis://docket:hunter2@redis:6379/0")
        .output()
        .unwrap();
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("[env: DOCKET_URL]"), "{help}");
    assert!(!help.contains("hunter2"));
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn runs_a_worker_from_its_options() {
    let args = parse(&[
        "--url",
        "memory://cli-run",
        "--name",
        "from-cli",
        "--until-finished",
    ]);
    let docket = args.docket().await.unwrap();
    assert_eq!(args.worker(&docket).worker_name(), "from-cli");
    let unnamed = parse(&["--url", "memory://cli-run"]);
    assert_ne!(unnamed.worker(&docket).worker_name(), "from-cli");
    args.run(&docket).await.unwrap();
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn a_docket_from_the_options_takes_builder_settings() {
    let args = parse(&["--docket", "orders", "--url", "memory://cli-builder"]);

    let docket = args
        .docket_builder()
        .execution_ttl(Duration::from_secs(7))
        .connect()
        .await
        .unwrap();

    assert_eq!(docket.name(), "orders");
    assert_eq!(docket.settings().execution_ttl, Duration::from_secs(7));
}

#[test]
fn an_application_changes_the_options_defaults() {
    use clap::{CommandFactory, FromArgMatches};

    let command = <Cli as CommandFactory>::command()
        .mut_arg("docket", |arg| arg.default_value("orders"))
        .mut_arg("concurrency", |arg| arg.default_value("2"));
    let matches = command.try_get_matches_from(["app"]).unwrap();
    let args = Cli::from_arg_matches(&matches).unwrap().worker;

    assert_eq!((args.docket.as_str(), args.concurrency), ("orders", 2));
}

/// Signals reach every listener in the process, so the tests that send them
/// take turns.
#[cfg(all(unix, feature = "memory"))]
static SIGNALS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Sends this process `signal` until `run` finishes.  The worker listens
/// only once it starts, and a signal sent before then reaches no listener.
#[cfg(all(unix, feature = "memory"))]
async fn signal_until_finished(signal: &str, run: &tokio::task::JoinHandle<crate::Result<()>>) {
    let pid = std::process::id().to_string();
    let signalling = async {
        while !run.is_finished() {
            let status = std::process::Command::new("kill")
                .args([format!("-{signal}").as_str(), pid.as_str()])
                .status()
                .unwrap();
            assert!(status.success());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), signalling)
        .await
        .expect("the worker stops on the signal");
}

#[cfg(all(unix, feature = "memory"))]
#[rstest]
#[case::terminate("TERM")]
#[case::interrupt("INT")]
#[tokio::test]
async fn a_worker_stops_on_a_shutdown_signal(#[case] signal: &str) {
    use tokio::signal::unix::{SignalKind, signal as listen};

    let _turn = SIGNALS.lock().await;
    // Listening before any signal is sent replaces the default action, which
    // would end the test process.
    let _terminate = listen(SignalKind::terminate()).unwrap();
    let _interrupt = listen(SignalKind::interrupt()).unwrap();
    let url = format!("memory://{}", uuid::Uuid::now_v7());
    let args = parse(&["--url", &url]);
    let docket = args.docket().await.unwrap();
    let run = tokio::spawn(async move { args.run(&docket).await });

    signal_until_finished(signal, &run).await;

    run.await.unwrap().unwrap();
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn a_worker_stops_when_its_shutdown_completes() {
    let url = format!("memory://{}", uuid::Uuid::now_v7());
    let args = parse(&["--url", &url]);
    let docket = args.docket().await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(async move {
        args.run_until(&docket, async {
            stopped.await.ok();
        })
        .await
    });

    stop.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the worker stops on its shutdown")
        .unwrap()
        .unwrap();
}

/// A port that nothing listens on.  The servers listen on every address,
/// and the tests reach them on the loopback one.
#[cfg(feature = "memory")]
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[cfg(feature = "memory")]
async fn get(port: u16) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    answer
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn serves_a_healthcheck_and_the_metrics_it_installed() {
    let (healthcheck, metrics) = (free_port(), free_port());
    let args = parse(&[
        "--url",
        "memory://cli-servers",
        "--healthcheck-port",
        &healthcheck.to_string(),
        "--metrics-port",
        &metrics.to_string(),
    ]);
    let _docket = args.docket().await.unwrap();
    let _servers = args.servers().await.unwrap();
    opentelemetry::global::meter("cli-test")
        .u64_counter("cli_scrapes")
        .build()
        .add(1, &[]);

    let health = get(healthcheck).await;
    let page = get(metrics).await;

    assert!(health.contains("Content-Type: text/plain\r\n"), "{health}");
    assert!(health.ends_with("\r\n\r\nOK"), "{health}");
    assert!(
        page.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"),
        "{page}"
    );
    assert!(page.contains("\ncli_scrapes_total 1.0\n"), "{page}");
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn the_servers_stop_when_they_drop() {
    let healthcheck = free_port();
    let args = parse(&[
        "--url",
        "memory://cli-servers-drop",
        "--healthcheck-port",
        &healthcheck.to_string(),
    ]);
    let servers = args.servers().await.unwrap();
    assert!(get(healthcheck).await.ends_with("OK"));

    drop(servers);

    let refused = async {
        while tokio::net::TcpStream::connect(("127.0.0.1", healthcheck))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), refused)
        .await
        .expect("the healthcheck stops listening");
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn runs_a_worker_with_its_servers() {
    let args = parse(&[
        "--url",
        "memory://cli-run-servers",
        "--until-finished",
        "--healthcheck-port",
        &free_port().to_string(),
        "--metrics-port",
        &free_port().to_string(),
    ]);
    let docket = args.docket().await.unwrap();
    args.run(&docket).await.unwrap();
}

#[cfg(feature = "memory")]
#[rstest]
#[case::healthcheck("--healthcheck-port", "the healthcheck")]
#[case::metrics("--metrics-port", "the metrics")]
#[tokio::test]
async fn refuses_a_port_in_use(#[case] option: &str, #[case] server: &str) {
    let taken = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let args = parse(&[
        "--url",
        "memory://cli-port-in-use",
        option,
        &port.to_string(),
    ]);
    let docket = args.docket().await.unwrap();

    let error = args.run(&docket).await.unwrap_err();

    assert!(
        error
            .to_string()
            .starts_with(&format!("cannot serve {server} on port {port}: ")),
        "{error}"
    );
}
