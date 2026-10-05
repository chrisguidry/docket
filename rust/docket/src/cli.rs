//! A worker's command-line options, for an application's own command line.
//!
//! docket-rs is a library, so it ships no worker binary.  [`WorkerArgs`]
//! gives an application's binary the same options, with the same
//! environment variables, as pydocket's `docket worker`:
//!
//! ```no_run
//! use clap::Parser;
//!
//! #[derive(Parser)]
//! struct Cli {
//!     #[command(flatten)]
//!     worker: docket::cli::WorkerArgs,
//!     /// The application's own options sit next to docket's.
//!     #[arg(long)]
//!     verbose: bool,
//! }
//!
//! # async fn example() -> docket::Result<()> {
//! let cli = Cli::parse();
//! let docket = cli.worker.docket().await?;
//! // Register the application's tasks here.
//! cli.worker.run(&docket).await
//! # }
//! ```

use std::time::Duration;

use crate::docket::Docket;
use crate::error::Result;
use crate::worker::Worker;

/// A docket worker's command-line options.
#[derive(clap::Args, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WorkerArgs {
    /// The name of the docket.
    #[arg(long = "docket", env = "DOCKET_NAME", default_value = "docket")]
    pub docket: String,

    /// The URL of the Redis server.
    #[arg(long, env = "DOCKET_URL", default_value = "redis://localhost:6379/0")]
    pub url: String,

    /// The name of the worker.  The default is `{hostname}#{pid}`.
    #[arg(long, env = "DOCKET_WORKER_NAME")]
    pub name: Option<String>,

    /// How many tasks run at once.
    #[arg(long, env = "DOCKET_WORKER_CONCURRENCY", default_value_t = 10)]
    pub concurrency: usize,

    /// How many deliveries one read takes at most.
    #[arg(long, env = "DOCKET_WORKER_MESSAGE_BATCH", default_value_t = 1000)]
    pub message_batch: usize,

    /// How long a delivery may go without renewal before another worker
    /// takes it over.
    #[arg(long, env = "DOCKET_WORKER_REDELIVERY_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    pub redelivery_timeout: Duration,

    /// How long to wait before reconnecting to Redis.
    #[arg(long, env = "DOCKET_WORKER_RECONNECTION_DELAY", default_value = "5s", value_parser = parse_duration)]
    pub reconnection_delay: Duration,

    /// How long one read waits for new tasks.
    #[arg(long, env = "DOCKET_WORKER_MINIMUM_CHECK_INTERVAL", default_value = "100ms", value_parser = parse_duration)]
    pub minimum_check_interval: Duration,

    /// How often to move due future tasks onto the stream.
    #[arg(long, env = "DOCKET_WORKER_SCHEDULING_RESOLUTION", default_value = "250ms", value_parser = parse_duration)]
    pub scheduling_resolution: Duration,

    /// Whether to schedule automatic perpetual tasks at startup.
    #[arg(
        long,
        env = "DOCKET_WORKER_SCHEDULE_AUTOMATIC_TASKS",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_name = "BOOL"
    )]
    pub schedule_automatic_tasks: bool,

    /// Stop once nothing is left to run, instead of on SIGTERM or SIGINT.
    #[arg(long)]
    pub until_finished: bool,
}

impl WorkerArgs {
    /// Connects to the docket the options name.
    pub async fn docket(&self) -> Result<Docket> {
        Docket::connect(&self.docket, &self.url).await
    }

    /// A worker with the options' settings.
    #[must_use]
    pub fn worker(&self, docket: &Docket) -> Worker {
        let worker = Worker::new(docket.clone())
            .concurrency(self.concurrency)
            .message_batch(self.message_batch)
            .redelivery_timeout(self.redelivery_timeout)
            .reconnection_delay(self.reconnection_delay)
            .minimum_check_interval(self.minimum_check_interval)
            .scheduling_resolution(self.scheduling_resolution)
            .schedule_automatic_tasks(self.schedule_automatic_tasks);
        match &self.name {
            Some(name) => worker.name(name),
            None => worker,
        }
    }

    /// Runs a worker until nothing is left with `--until-finished`, or
    /// otherwise until SIGTERM or SIGINT, after which running tasks finish.
    pub async fn run(&self, docket: &Docket) -> Result<()> {
        let worker = self.worker(docket);
        if self.until_finished {
            worker.run_until_finished().await
        } else {
            worker.run_until(shutdown_signal()).await
        }
    }
}

/// Completes on SIGTERM or SIGINT, the signals that stop a deployed worker.
///
/// # Panics
///
/// When the process cannot listen for the signals, which happens only
/// without a Tokio runtime that has signal handling enabled.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("the process can listen for SIGTERM");
        tokio::select! {
            _ = terminate.recv() => {}
            result = tokio::signal::ctrl_c() => result.expect("the process can listen for SIGINT"),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("the process can listen for Ctrl-C");
}

/// Reads a duration the way pydocket's CLI does: a number of seconds, or a
/// number with `ms`, `s`, `m`, or `h`, or `mm:ss`, or `hh:mm:ss`.
pub fn parse_duration(text: &str) -> std::result::Result<Duration, String> {
    let invalid = || format!("{text} is not a duration, such as 30, 250ms, 5s, 2m, 1h, or 01:30");
    let number = |digits: &str| digits.parse::<u64>().map_err(|_| invalid());
    if text.contains(':') {
        let parts = text
            .split(':')
            .map(number)
            .collect::<std::result::Result<Vec<u64>, _>>()?;
        return match parts[..] {
            [minutes, seconds] => Ok(Duration::from_secs(minutes * 60 + seconds)),
            [hours, minutes, seconds] => {
                Ok(Duration::from_secs(hours * 3600 + minutes * 60 + seconds))
            }
            _ => Err(invalid()),
        };
    }
    if let Some(millis) = text.strip_suffix("ms") {
        return Ok(Duration::from_millis(number(millis)?));
    }
    let (digits, unit) = match text.char_indices().last() {
        Some((index, unit @ ('s' | 'm' | 'h'))) => (&text[..index], unit),
        _ => (text, 's'),
    };
    let amount = number(digits)?;
    Ok(match unit {
        'm' => Duration::from_secs(amount * 60),
        'h' => Duration::from_secs(amount * 3600),
        _ => Duration::from_secs(amount),
    })
}

#[cfg(test)]
mod tests;
