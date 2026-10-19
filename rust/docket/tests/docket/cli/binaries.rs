//! The commands in a worker binary, and in docket-rs's own binary.

use clap::{Parser, Subcommand};
use docket::cli::{Command, WorkerArgs};
use futures::FutureExt;
use futures::future::BoxFuture;
use rstest::rstest;

use crate::support::{Noop, docket, shared_url, within};

/// The binary from `docket::cli`'s documentation, which carries the commands
/// next to its worker's options.
#[derive(Parser)]
struct WorkerBinary {
    #[command(flatten)]
    worker: WorkerArgs,
    #[command(subcommand)]
    command: Option<WorkerCommands>,
}

#[derive(Subcommand)]
enum WorkerCommands {
    #[command(subcommand)]
    Docket(Command),
}

#[tokio::test]
async fn a_worker_binary_runs_the_commands_with_its_own_options() {
    let docket = docket().await;
    docket.add(Noop).await.unwrap();
    let url = shared_url(&docket);
    let parsed = WorkerBinary::try_parse_from([
        "my-worker",
        "--docket",
        docket.name(),
        "--url",
        &url,
        "docket",
        "clear",
    ])
    .unwrap();
    let Some(WorkerCommands::Docket(command)) = &parsed.command else {
        panic!("no docket command");
    };
    let connected = parsed.worker.docket().await.unwrap();
    let mut out = Vec::new();
    command.run_with(&connected, &mut out).await.unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        format!("Cleared 1 tasks from docket '{}'\n", docket.name())
    );
}

type Binary = fn(String, String) -> BoxFuture<'static, std::process::Output>;

fn docket_rs(arguments: Vec<String>) -> BoxFuture<'static, std::process::Output> {
    tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_docket-rs"))
            .args(arguments)
            .env_remove("DOCKET_NAME")
            .env_remove("DOCKET_URL")
            .output()
            .unwrap()
    })
    .map(Result::unwrap)
    .boxed()
}

#[rstest]
#[case::options_first(|name, url| docket_rs(vec!["--docket".into(), name, "--url".into(), url, "clear".into()]))]
#[case::options_last(|name, url| docket_rs(vec!["clear".into(), "--docket".into(), name, "--url".into(), url]))]
#[tokio::test]
async fn the_binary_runs_a_command_on_a_shared_docket(#[case] binary: Binary) {
    let docket = docket().await;
    let url = shared_url(&docket);
    if url.starts_with("memory://") {
        return;
    }
    docket.add(Noop).await.unwrap();
    let output = within(30, binary(docket.name().to_owned(), url)).await;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("Cleared 1 tasks from docket '{}'\n", docket.name())
    );
}

#[rstest]
#[case::bad_url(&["--url", "nope://x", "snapshot"], 1, "Error: nope://x is not a docket URL")]
#[case::unreachable(&["--url", "redis://127.0.0.1:1/0", "snapshot"], 1, "Error: ")]
#[case::memory(&["--url", "memory://x", "snapshot"], 2, "memory:// URL scheme is not supported")]
#[case::bad_value(&["strike", "charge", "customer", "==", "int:x"], 2, "x is not an int")]
#[tokio::test]
async fn the_binary_reports_failures_on_stderr(
    #[case] arguments: &[&str],
    #[case] code: i32,
    #[case] expected: &str,
) {
    let output = within(
        30,
        docket_rs(
            arguments
                .iter()
                .map(|&argument| argument.to_owned())
                .collect(),
        ),
    )
    .await;
    assert_eq!(output.status.code(), Some(code), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(expected), "{stderr}");
}
