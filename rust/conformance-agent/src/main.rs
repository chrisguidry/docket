//! docket-rs's agent for the conformance driver in conformance/.
//!
//! ```text
//! docket-conformance-agent produce|worker --scenario NAME --url URL --docket NAME
//! ```
//!
//! Every language ships an agent with this command line.  `produce`
//! schedules the scenario's tasks and exits.  `worker` runs a worker for the
//! scenario's tasks until SIGTERM or SIGINT, then lets them finish.  The
//! scenario's tasks record what happens to them as events, and the driver
//! makes its assertions on those events.

mod events;
mod scenarios;

use clap::{Parser, ValueEnum};
use docket::Docket;

#[derive(Parser)]
struct Arguments {
    role: Role,
    #[arg(long)]
    scenario: String,
    #[arg(long)]
    url: String,
    #[arg(long)]
    docket: String,
}

#[derive(Clone, Copy, ValueEnum)]
enum Role {
    Produce,
    Worker,
}

#[tokio::main]
async fn main() -> docket::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .init();
    let arguments = Arguments::parse();
    let docket = Docket::connect(&arguments.docket, &arguments.url).await?;
    let events = events::Events::open(&arguments.url, &arguments.scenario)
        .await
        .map_err(docket::Error::from)?;
    let scenario = scenarios::find(&arguments.scenario);
    match arguments.role {
        Role::Produce => scenario.produce(&docket, &events).await,
        Role::Worker => {
            let worker = scenario.worker(&docket, &events);
            worker.run_until(shutdown()).await
        }
    }
}

/// Completes on SIGTERM or SIGINT, the way a deployed worker stops.
async fn shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("the agent can listen for SIGTERM");
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
