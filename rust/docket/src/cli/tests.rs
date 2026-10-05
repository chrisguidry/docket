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
