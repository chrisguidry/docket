//! `docket::cli`'s operator commands, run on a docket as a worker binary runs
//! them.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use docket::cli::Command;
use docket::{Docket, Error, State, Task};
use rstest::rstest;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::support::{Echo, Noop, docket, shared_url, within, worker};

mod binaries;
mod watch;

#[derive(Parser)]
struct App {
    #[command(subcommand)]
    command: Command,
}

fn command(arguments: &[&str]) -> Command {
    App::try_parse_from(std::iter::once("app").chain(arguments.iter().copied()))
        .unwrap()
        .command
}

/// Runs a command and returns what it printed.
async fn run(docket: &Docket, arguments: &[&str]) -> String {
    let mut out = Vec::new();
    command(arguments).run_with(docket, &mut out).await.unwrap();
    String::from_utf8(out).unwrap()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "charge")]
struct Charge {
    customer: u32,
}

#[tokio::test]
async fn a_snapshot_shows_each_tasks_arguments_as_json() {
    let docket = docket().await;
    docket.add(Charge { customer: 7 }).key("now").await.unwrap();
    for (key, hours) in [("soon", 1), ("later", 2)] {
        docket
            .add(Echo::new(key))
            .key(key)
            .after(Duration::from_hours(hours))
            .await
            .unwrap();
    }

    let output = run(&docket, &["snapshot", "--stats"]).await;

    assert!(
        output.contains(&format!("Docket: '{}'", docket.name())),
        "{output}"
    );
    assert!(output.contains("0 workers, 0/3 running"), "{output}");
    assert!(
        output.contains("┃ Key   ┃ Worker ┃ Started ┃ Arguments        ┃"),
        "{output}"
    );
    let row = |key: &str| {
        output
            .lines()
            .find(|line| line.contains(&format!(" {key} ")))
            .unwrap_or_else(|| panic!("no row for {key} in {output}"))
            .to_owned()
    };
    assert!(row("now").contains(r#"│ {"customer":7}   │"#), "{output}");
    assert!(row("soon").contains(r#"{"text":"soon"}"#), "{output}");
    assert!(row("later").contains(" at "), "{output}");

    assert!(
        output.contains("Task Count Statistics by Function"),
        "{output}"
    );
    let echo = output
        .lines()
        .find(|line| line.starts_with("│ echo"))
        .unwrap();
    let cells: Vec<&str> = echo.split('│').map(str::trim).collect();
    assert_eq!(cells[1..5], ["echo", "2", "0", "2"], "{output}");
    assert!(cells[5].starts_with("at ") && cells[6].starts_with("at "));
    assert_ne!(cells[5], cells[6]);
}

#[tokio::test]
async fn a_snapshot_of_an_empty_docket_has_no_statistics() {
    let docket = docket().await;
    let output = run(&docket, &["snapshot", "--stats"]).await;
    assert!(output.contains("0 workers, 0/0 running"), "{output}");
    assert!(!output.contains("Statistics"), "{output}");
}

#[tokio::test]
async fn a_snapshot_shows_who_runs_a_task_and_since_when() {
    let docket = docket().await;
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (on_start, on_release) = (Arc::clone(&started), Arc::clone(&release));
    docket.register(move |_ctx, _: Noop| {
        let (on_start, on_release) = (Arc::clone(&on_start), Arc::clone(&on_release));
        async move {
            on_start.notify_one();
            on_release.notified().await;
            Ok::<_, std::io::Error>(())
        }
    });
    docket.add(Noop).key("busy").await.unwrap();
    let run_worker = tokio::spawn(worker(&docket).name("w-cli").run_until_finished());
    within(10, started.notified()).await;

    let output = run(&docket, &["snapshot", "--stats"]).await;
    release.notify_one();
    within(10, run_worker).await.unwrap().unwrap();

    assert!(output.contains("1 workers, 1/1 running"), "{output}");
    let row = output.lines().find(|line| line.contains(" busy ")).unwrap();
    let cells: Vec<&str> = row.split('│').map(str::trim).collect();
    assert_eq!(cells[2..5], ["noop", "busy", "w-cli"], "{output}");
    assert!(cells[5].ends_with(" ago"), "{output}");
    assert_eq!(cells[6], "null");
    let stats = output
        .lines()
        .rfind(|line| line.starts_with("│ noop"))
        .unwrap();
    let cells: Vec<&str> = stats.split('│').map(str::trim).collect();
    assert_eq!(cells[1..7], ["noop", "1", "1", "0", "", ""], "{output}");
}

#[tokio::test]
async fn workers_lists_the_workers_and_their_tasks() {
    let docket = docket().await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    docket.register(|_ctx, args: Echo| async move { Ok::<_, std::io::Error>(args.text) });
    let stop = Arc::new(Notify::new());
    let shutdown = Arc::clone(&stop);
    let running = tokio::spawn(
        worker(&docket)
            .name("w-list")
            .run_until(async move { shutdown.notified().await }),
    );
    within(10, async {
        while docket.workers().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let listed = run(&docket, &["workers", "ls"]).await;
    let for_echo = run(&docket, &["workers", "for-task", "echo"]).await;
    let for_other = run(&docket, &["workers", "for-task", "other"]).await;
    stop.notify_one();
    within(10, running).await.unwrap().unwrap();

    for output in [&listed, &for_echo] {
        // The title wraps at the table's width, as rich wraps it.
        let title: String = output
            .split('┏')
            .next()
            .unwrap()
            .split_whitespace()
            .collect();
        assert_eq!(title, format!("WorkersinDocket:{}", docket.name()));
        let row = output.lines().find(|line| line.contains("w-list")).unwrap();
        assert!(row.contains(" ago "), "{output}");
        assert!(row.contains("│ echo "), "{output}");
        assert!(output.contains("│ noop "), "{output}");
    }
    assert!(!for_other.contains("w-list"), "{for_other}");
}

#[tokio::test]
async fn a_running_worker_honors_a_strike_from_the_command_until_it_is_restored() {
    let operator = docket().await;
    let url = shared_url(&operator);
    let workers = Docket::connect(operator.name(), &url).await.unwrap();
    let producer = Docket::connect(operator.name(), &url).await.unwrap();
    let charged = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&charged);
    workers.register(move |_ctx, args: Charge| {
        let record = Arc::clone(&record);
        async move {
            record.lock().unwrap().push(args.customer);
            Ok::<_, std::io::Error>(())
        }
    });
    let stop = Arc::new(Notify::new());
    let shutdown = Arc::clone(&stop);
    let running =
        tokio::spawn(worker(&workers).run_until(async move { shutdown.notified().await }));

    // Added before the strike, so only the worker can stop them.
    let due = Duration::from_secs(1);
    let struck = producer
        .add(Charge { customer: 7 })
        .after(due)
        .await
        .unwrap();
    let allowed = producer
        .add(Charge { customer: 8 })
        .after(due)
        .await
        .unwrap();
    let striking = run(&operator, &["strike", "charge", "customer", "==", "int:7"]).await;
    assert_eq!(striking, "Striking charge customer == 7\n");

    within(10, async {
        while struck.status().await.unwrap().unwrap().state != State::Cancelled
            || allowed.status().await.unwrap().unwrap().state != State::Completed
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(*charged.lock().unwrap(), [8]);

    let restoring = run(&operator, &["restore", "charge", "customer", "==", "int:7"]).await;
    assert_eq!(restoring, "Restoring charge customer == 7\n");
    let again = operator
        .add(Charge { customer: 7 })
        .after(due)
        .await
        .unwrap();
    within(10, async {
        while again.status().await.unwrap().unwrap().state != State::Completed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    stop.notify_one();
    within(10, running).await.unwrap().unwrap();
    assert_eq!(*charged.lock().unwrap(), [8, 7]);
}

#[rstest]
#[case::whole_task(&["strike", "charge"], "Striking charge\n")]
#[case::any_task(&["strike", "*", "customer", ">=", "int:100"], "Striking (all tasks) customer >= 100\n")]
#[case::between(&["strike", "", "customer", "between", "json:[1,10]"], "Striking (all tasks) customer between [1, 10]\n")]
#[case::no_value(&["restore", "charge", "customer"], "Restoring charge customer == None\n")]
#[tokio::test]
async fn strike_and_restore_print_what_they_did(
    #[case] arguments: &[&str],
    #[case] expected: &str,
) {
    let docket = docket().await;
    assert_eq!(run(&docket, arguments).await, expected);
}

#[tokio::test]
async fn clear_removes_queued_and_scheduled_tasks() {
    let docket = docket().await;
    docket.add(Noop).key("queued").await.unwrap();
    docket
        .add(Echo::new("x"))
        .key("scheduled")
        .after(Duration::from_hours(1))
        .await
        .unwrap();

    let output = run(&docket, &["clear"]).await;

    assert_eq!(
        output,
        format!("Cleared 2 tasks from docket '{}'\n", docket.name())
    );
    assert_eq!(docket.snapshot().await.unwrap().total_tasks, 0);
}

#[rstest]
#[case::both_wildcards(&["strike"], "Must provide either a function and/or a parameter")]
#[case::restore_both_wildcards(&["restore", "*", "*"], "Must provide either a function and/or a parameter")]
#[case::between_one_bound(&["strike", "charge", "customer", "between", "int:3"], "between needs a value of two bounds")]
#[tokio::test]
async fn strike_refuses_arguments_that_name_no_strike(
    #[case] arguments: &[&str],
    #[case] expected: &str,
) {
    let docket = docket().await;
    let error = command(arguments)
        .run_with(&docket, &mut Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Invalid(_)), "{error}");
    assert!(error.to_string().starts_with(expected), "{error}");
}

const COMMANDS: &[&[&str]] = &[
    &["snapshot"],
    &["workers", "ls"],
    &["workers", "for-task", "noop"],
    &["strike", "noop"],
    &["restore", "noop"],
    &["clear"],
    &["watch", "k"],
];

#[tokio::test]
async fn every_command_reports_that_redis_is_unreachable() {
    let docket = Docket::connect("unreachable", "redis://127.0.0.1:1/0")
        .await
        .unwrap();
    for arguments in COMMANDS {
        let error = command(arguments)
            .run_with(&docket, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(error.is_redis_unavailable(), "{arguments:?}: {error}");
    }
}

struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::ErrorKind::BrokenPipe.into())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn every_command_reports_output_it_cannot_write() {
    let docket = docket().await;
    docket.add(Noop).key("k").await.unwrap();
    for arguments in COMMANDS {
        let error = command(arguments)
            .run_with(&docket, &mut Broken)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Output(_)), "{arguments:?}: {error}");
    }
}

#[tokio::test]
async fn run_prints_to_stdout() {
    let docket = docket().await;
    command(&["clear"]).run(&docket).await.unwrap();
}
