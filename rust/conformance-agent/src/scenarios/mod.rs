//! One module for each scenario, named after it with underscores, each with
//! `produce` and `worker`.

mod admission_failure;
mod backoff;
mod cancel_before_start;
mod chaos;
mod concurrency_limit;
mod graceful_drain;
mod perpetual;
mod perpetual_single_flight;
mod redelivery;
mod same_key;
mod telemetry;

use std::time::Duration;

use docket::{Docket, Worker};

use crate::events::Events;

pub enum Scenario {
    AdmissionFailure,
    Backoff,
    CancelBeforeStart,
    Chaos,
    ConcurrencyLimit,
    GracefulDrain,
    Perpetual,
    PerpetualSingleFlight,
    Redelivery,
    SameKey,
    Telemetry,
}

pub fn find(name: &str) -> Scenario {
    match name {
        "admission-failure" => Scenario::AdmissionFailure,
        "backoff" => Scenario::Backoff,
        "cancel-before-start" => Scenario::CancelBeforeStart,
        "chaos" => Scenario::Chaos,
        "concurrency-limit" => Scenario::ConcurrencyLimit,
        "graceful-drain" => Scenario::GracefulDrain,
        "perpetual" => Scenario::Perpetual,
        "perpetual-single-flight" => Scenario::PerpetualSingleFlight,
        "redelivery" => Scenario::Redelivery,
        "same-key" => Scenario::SameKey,
        "telemetry" => Scenario::Telemetry,
        _ => {
            eprintln!("no scenario is named {name}");
            std::process::exit(2);
        }
    }
}

/// A worker with the agent's defaults, the same as pydocket's `Worker.run`.
fn base(docket: &Docket) -> Worker {
    Worker::new(docket.clone()).minimum_check_interval(Duration::from_millis(100))
}

impl Scenario {
    pub async fn produce(&self, docket: &Docket, events: &Events) -> docket::Result<()> {
        match self {
            Self::AdmissionFailure => admission_failure::produce(docket).await,
            Self::Backoff => backoff::produce(docket).await,
            Self::CancelBeforeStart => cancel_before_start::produce(docket, events).await,
            Self::Chaos => chaos::produce(docket, events).await,
            Self::ConcurrencyLimit => concurrency_limit::produce(docket).await,
            Self::GracefulDrain => graceful_drain::produce(docket).await,
            Self::Perpetual | Self::PerpetualSingleFlight => Ok(()),
            Self::Redelivery => redelivery::produce(docket).await,
            Self::SameKey => same_key::produce(docket, events).await,
            Self::Telemetry => telemetry::produce(docket).await,
        }
    }

    pub fn worker(&self, docket: &Docket, events: &Events) -> Worker {
        let worker = base(docket);
        match self {
            Self::AdmissionFailure => admission_failure::worker(docket, events, worker),
            Self::Backoff => backoff::worker(docket, events, worker),
            Self::CancelBeforeStart => cancel_before_start::worker(docket, events, worker),
            Self::Chaos => chaos::worker(docket, events, worker),
            Self::ConcurrencyLimit => concurrency_limit::worker(docket, events, worker),
            Self::GracefulDrain => graceful_drain::worker(docket, events, worker),
            Self::Perpetual => perpetual::worker(docket, events, worker),
            Self::PerpetualSingleFlight => perpetual_single_flight::worker(docket, events, worker),
            Self::Redelivery => redelivery::worker(docket, events, worker),
            Self::SameKey => same_key::worker(docket, events, worker),
            Self::Telemetry => telemetry::worker(docket, events, worker),
        }
    }
}
