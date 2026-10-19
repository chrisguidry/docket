//! `docket-rs`: pydocket's operator commands for a docket that docket-rs
//! workers use.  See [`docket::cli`].

use std::process::ExitCode;

use clap::Parser;
use docket::cli::{Command, DocketArgs};

/// Inspect and manage a docket that docket-rs workers use.
#[derive(Parser)]
#[command(name = "docket-rs", version)]
struct Cli {
    #[command(flatten)]
    docket: DocketArgs,
    #[command(subcommand)]
    command: Command,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let ran = match cli.docket.docket().await {
        Ok(docket) => cli.command.run(&docket).await,
        Err(error) => Err(error),
    };
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}
