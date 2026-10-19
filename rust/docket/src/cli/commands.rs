//! The operator commands: pydocket's `snapshot`, `workers`, `strike`,
//! `restore`, `clear`, and `watch`, for a docket-rs docket.

use std::io::Write;

use chrono::{DateTime, Utc};

use super::display::{Justify, Table, local_time, relative_time, repr_str, timedelta};
use super::strike::StrikeArgs;
use super::watch;
use crate::docket::{Docket, Snapshot, WorkerSummary};
use crate::error::{Error, Result};

/// The commands that inspect and manage a docket, with pydocket's names,
/// arguments, and output.
#[derive(clap::Subcommand, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Command {
    /// Shows a snapshot of what's on the docket right now.
    Snapshot(SnapshotArgs),
    /// Look at the workers on a docket.
    #[command(subcommand)]
    Workers(WorkersCommand),
    /// Strikes a task or parameters from the docket.
    Strike(StrikeArgs),
    /// Restores a task or parameters to the docket.
    Restore(StrikeArgs),
    /// Clear all queued and scheduled tasks from the docket.
    Clear,
    /// Monitor progress of a specific task execution.
    Watch(WatchArgs),
}

/// The options of `snapshot`.
#[derive(clap::Args, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotArgs {
    /// Show task count statistics by function name.
    #[arg(long)]
    pub stats: bool,
}

/// The commands under `workers`.
#[derive(clap::Subcommand, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkersCommand {
    /// List all workers on the docket.
    Ls,
    /// List the workers on the docket that can process a certain task.
    ForTask {
        /// The name of the task.
        task: String,
    },
}

/// The arguments of `watch`.
#[derive(clap::Args, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WatchArgs {
    /// The task execution key to monitor.
    pub key: String,
}

impl Command {
    /// Runs the command on `docket` and prints its output to stdout.
    pub async fn run(&self, docket: &Docket) -> Result<()> {
        self.run_with(docket, &mut std::io::stdout()).await
    }

    /// Runs the command on `docket` and writes its output to `out`.
    pub async fn run_with(&self, docket: &Docket, out: &mut (dyn Write + Send)) -> Result<()> {
        match self {
            Self::Snapshot(args) => {
                let snapshot = docket.snapshot().await?;
                emit(out, &render_snapshot(docket.name(), &snapshot, args.stats))
            }
            Self::Workers(WorkersCommand::Ls) => {
                let workers = docket.workers().await?;
                emit(out, &render_workers(docket.name(), docket.now(), &workers))
            }
            Self::Workers(WorkersCommand::ForTask { task }) => {
                let workers = docket.task_workers(task).await?;
                emit(out, &render_workers(docket.name(), docket.now(), &workers))
            }
            Self::Strike(args) => {
                let strike = args.strike()?;
                docket.strike(strike).await?;
                emit(out, &args.describe("Striking"))
            }
            Self::Restore(args) => {
                let strike = args.strike()?;
                docket.restore(strike).await?;
                emit(out, &args.describe("Restoring"))
            }
            Self::Clear => {
                let cleared = docket.clear().await?;
                let name = docket.name();
                emit(
                    out,
                    &format!("Cleared {cleared} tasks from docket '{name}'\n"),
                )
            }
            Self::Watch(args) => watch::watch(docket, &args.key, out).await,
        }
    }
}

/// Writes one piece of a command's output.
pub(super) fn emit(out: &mut (dyn Write + Send), text: &str) -> Result<()> {
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .map_err(Error::Output)
}

/// pydocket's `snapshot` table, with the arguments as JSON in a last
/// column, and with `--stats` its table of counts by task.
pub(super) fn render_snapshot(name: &str, snapshot: &Snapshot, stats: bool) -> String {
    let relative = |when| relative_time(snapshot.taken, when);
    let title = format!(
        "Docket: {}\nas of {}\n{} workers, {}/{} running",
        repr_str(name),
        local_time(snapshot.taken),
        snapshot.workers.len(),
        snapshot.running.len(),
        snapshot.total_tasks,
    );
    let mut table = Table::new(title)
        .column("When", Justify::Left)
        .column("Function", Justify::Left)
        .column("Key", Justify::Left)
        .column("Worker", Justify::Left)
        .column("Started", Justify::Left)
        .column("Arguments", Justify::Left);
    for task in snapshot.running.iter().chain(&snapshot.future) {
        table.row(vec![
            relative(task.when),
            task.function.clone(),
            task.key.clone(),
            task.worker.clone().unwrap_or_default(),
            task.started.map(relative).unwrap_or_default(),
            task.args.to_string(),
        ]);
    }
    let mut out = table.render();
    if stats && !(snapshot.running.is_empty() && snapshot.future.is_empty()) {
        out.push('\n');
        out.push_str(&render_stats(snapshot));
    }
    out
}

/// One task's counts in pydocket's `snapshot --stats`.
struct TaskStats<'a> {
    function: &'a str,
    running: usize,
    queued: usize,
    oldest_queued: Option<DateTime<Utc>>,
    latest_queued: Option<DateTime<Utc>>,
}

fn render_stats(snapshot: &Snapshot) -> String {
    let mut stats: Vec<TaskStats> = Vec::new();
    let running = snapshot.running.iter().map(|task| (task, true));
    let future = snapshot.future.iter().map(|task| (task, false));
    for (task, is_running) in running.chain(future) {
        let index = stats
            .iter()
            .position(|counts| counts.function == task.function)
            .unwrap_or_else(|| {
                stats.push(TaskStats {
                    function: &task.function,
                    running: 0,
                    queued: 0,
                    oldest_queued: None,
                    latest_queued: None,
                });
                stats.len() - 1
            });
        let counts = &mut stats[index];
        if is_running {
            counts.running += 1;
        } else {
            counts.queued += 1;
            let when = task.when;
            counts.oldest_queued = Some(counts.oldest_queued.map_or(when, |old| old.min(when)));
            counts.latest_queued = Some(counts.latest_queued.map_or(when, |old| old.max(when)));
        }
    }
    // Most tasks first, to show a runaway task.  The sort is stable, so ties
    // keep the order pydocket's dict has them in.
    stats.sort_by_key(|counts| std::cmp::Reverse(counts.running + counts.queued));

    let relative = |when: Option<DateTime<Utc>>| {
        when.map(|when| relative_time(snapshot.taken, when))
            .unwrap_or_default()
    };
    let mut table = Table::new("Task Count Statistics by Function")
        .column("Function", Justify::Left)
        .column("Total", Justify::Right)
        .column("Running", Justify::Right)
        .column("Queued", Justify::Right)
        .column("Oldest Queued", Justify::Right)
        .column("Latest Queued", Justify::Right);
    for counts in &stats {
        table.row(vec![
            counts.function.to_owned(),
            (counts.running + counts.queued).to_string(),
            counts.running.to_string(),
            counts.queued.to_string(),
            relative(counts.oldest_queued),
            relative(counts.latest_queued),
        ]);
    }
    table.render()
}

/// pydocket's table of workers, most recently seen first.
pub(super) fn render_workers(name: &str, now: DateTime<Utc>, workers: &[WorkerSummary]) -> String {
    let mut workers: Vec<&WorkerSummary> = workers.iter().collect();
    workers.sort_by_key(|worker| std::cmp::Reverse(worker.last_seen));
    let mut table = Table::new(format!("Workers in Docket: {name}"))
        .column("Name", Justify::Left)
        .column("Last Seen", Justify::Left)
        .column("Tasks", Justify::Left);
    for worker in workers {
        let tasks = if worker.tasks.is_empty() {
            "(none)".to_owned()
        } else {
            worker.tasks.join("\n")
        };
        table.row(vec![
            worker.name.clone(),
            format!("{} ago", timedelta(now - worker.last_seen)),
            tasks,
        ]);
    }
    table.render()
}

#[cfg(test)]
mod tests;
