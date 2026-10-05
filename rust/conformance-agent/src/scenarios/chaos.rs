//! Load while the driver kills workers and restarts Redis.
//!
//! Each producer adds `TASKS_PER_PRODUCE` tasks with jitter of five seconds
//! either way, a toxic task now and then, and some strikes that never match,
//! so the workers evaluate strikes on every task.

use std::time::Duration;

use chrono::Utc;
use docket::{Docket, Operator, Retry, Strike, Task, Worker};
use rand::RngExt;
use serde::{Deserialize, Serialize};

use crate::events::{Event, Events};

const TASKS_PER_PRODUCE: usize = 4000;
const STRIKES_PER_PRODUCE: usize = 20;
const TOXIC_CHANCE: f64 = 0.01;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "hello")]
struct Hello;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "toxic")]
struct Toxic;

#[derive(Serialize, Deserialize, Task)]
#[task(name = "rando")]
struct Rando;

const OPERATORS: [Operator; 7] = [
    Operator::Equal,
    Operator::NotEqual,
    Operator::GreaterThan,
    Operator::GreaterOrEqual,
    Operator::LessThan,
    Operator::LessOrEqual,
    Operator::Between,
];

pub async fn produce(docket: &Docket, events: &Events) -> docket::Result<()> {
    let mut strikes = 0;
    let mut sent = 0;
    while sent < TASKS_PER_PRODUCE {
        let attempt = async {
            while strikes < STRIKES_PER_PRODUCE {
                let (parameter, operator, value) = {
                    let mut rng = rand::rng();
                    (
                        format!("param_{}", rng.random_range(1..=100)),
                        OPERATORS[rng.random_range(0..OPERATORS.len())],
                        format!("val_{}", rng.random_range(1..=1000)),
                    )
                };
                docket
                    .strike(
                        Strike::task::<Rando>()
                            .field(parameter)
                            .when(operator, value),
                    )
                    .await?;
                strikes += 1;
            }
            let jitter = rand::rng().random_range(-5000..=5000);
            let when = Utc::now() + chrono::Duration::milliseconds(jitter);
            let execution = docket.add(Hello).at(when).await?;
            events
                .record(Event {
                    event: "added",
                    task: "hello",
                    key: execution.key(),
                    attempt: 0,
                    worker: "",
                })
                .await?;
            sent += 1;
            if rand::rng().random_bool(TOXIC_CHANCE) {
                docket.add(Toxic).await?;
            }
            Ok(())
        };
        let result: docket::Result<()> = attempt.await;
        if let Err(error) = result {
            if !error.is_redis_unavailable() {
                return Err(error);
            }
            tracing::warn!(%error, sent, "Redis went away; retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

pub fn worker(docket: &Docket, events: &Events, worker: Worker) -> Worker {
    let events = events.clone();
    docket
        .register(move |ctx, _: Hello| {
            let events = events.clone();
            async move { events.ran(&ctx, "ran").await }
        })
        .with(Retry::forever());
    docket.register(|_ctx, _: Toxic| async {
        let roll: f64 = rand::rng().random();
        if roll < 0.25 {
            std::process::exit(42);
        }
        if roll < 0.625 {
            return Err(docket::Error::Invalid("Boom".into()));
        }
        let pause = rand::rng().random_range(10..50);
        tokio::time::sleep(Duration::from_millis(pause)).await;
        Ok(())
    });
    worker.redelivery_timeout(Duration::from_secs(5))
}
