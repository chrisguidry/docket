use std::time::Duration;

use clap::Parser;
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
