//! The events a scenario's tasks write, in the shape every agent writes.

use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;

#[derive(Clone)]
pub struct Events {
    connection: MultiplexedConnection,
    stream: String,
}

/// What one event says.  The field names are the stream's field names.
#[expect(
    clippy::struct_field_names,
    reason = "the contract names this field event"
)]
pub struct Event<'a> {
    pub event: &'a str,
    pub task: &'a str,
    pub key: &'a str,
    /// The attempt number, or 0 for a producer.
    pub attempt: u32,
    /// The worker's name, or empty for a producer.
    pub worker: &'a str,
}

impl Events {
    pub async fn open(url: &str, scenario: &str) -> redis::RedisResult<Self> {
        Ok(Self {
            connection: redis::Client::open(url)?
                .get_multiplexed_async_connection()
                .await?,
            stream: format!("conformance:{scenario}:events"),
        })
    }

    pub async fn record(&self, event: Event<'_>) -> docket::Result<()> {
        let now = chrono::Utc::now();
        let time = format!("{}.{:06}", now.timestamp(), now.timestamp_subsec_micros());
        let mut connection = self.connection.clone();
        let _: String = connection
            .xadd(
                &self.stream,
                "*",
                &[
                    ("event", event.event.to_owned()),
                    ("task", event.task.to_owned()),
                    ("key", event.key.to_owned()),
                    ("attempt", event.attempt.to_string()),
                    ("worker", event.worker.to_owned()),
                    ("time", time),
                ],
            )
            .await?;
        Ok(())
    }

    /// Records what a running task is doing.
    pub async fn ran(&self, ctx: &docket::Context, event: &str) -> docket::Result<()> {
        self.record(Event {
            event,
            task: ctx.function(),
            key: ctx.key(),
            attempt: ctx.attempt(),
            worker: ctx.worker(),
        })
        .await
    }
}
