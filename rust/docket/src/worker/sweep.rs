//! Taking over deliveries that a dead worker stopped renewing, and renewing
//! this worker's own.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use redis::RedisResult;
use redis::streams::StreamAutoClaimReply;
use tokio::time::Instant;

use super::session::{Delivery, Shared, deliveries, regrouping};
use crate::error::Result;
use crate::keys::WORKER_GROUP;
use crate::scripts;

/// Lease renewal sends at most this many entries in one command.
const RENEWAL_BATCH: usize = 5000;

/// The sweep for lost deliveries.  One worker at a time holds the sweep's
/// lease, so the fleet sweeps about once per interval however many workers
/// it has.
pub(super) struct Sweep {
    worker: Arc<Shared>,
    cursor: String,
    next: Instant,
}

impl Sweep {
    pub fn new(worker: Arc<Shared>) -> Self {
        Self {
            worker,
            cursor: "0-0".to_owned(),
            next: Instant::now(),
        }
    }

    fn interval(&self) -> Duration {
        self.worker.settings.redelivery_timeout / 4
    }

    fn lease_millis(&self) -> i64 {
        i64::try_from(self.interval().as_millis())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    /// Whether this worker should sweep now.
    pub async fn due(&mut self) -> Result<bool> {
        let walking = self.cursor != "0-0";
        if walking && self.refresh().await? {
            return Ok(true);
        }
        if Instant::now() < self.next {
            return Ok(false);
        }
        let docket = &self.worker.docket;
        let mut connection = docket.handle();
        let taken: Option<String> = redis::cmd("SET")
            .arg(docket.keys().sweep_lease())
            .arg(&self.worker.settings.name)
            .arg("NX")
            .arg("PX")
            .arg(self.lease_millis())
            .query_async(&mut connection)
            .await?;
        if taken.is_some() || self.refresh().await? {
            return Ok(true);
        }
        self.rest();
        Ok(false)
    }

    async fn refresh(&self) -> Result<bool> {
        let docket = &self.worker.docket;
        let call = scripts::RefreshLease {
            lease_key: docket.keys().sweep_lease(),
            holder: self.worker.settings.name.clone(),
            duration_ms: self.lease_millis(),
        }
        .call();
        let mut connection = docket.handle();
        let held: i64 = call.run(&mut connection).await?;
        Ok(held == 1)
    }

    fn rest(&mut self) {
        let jitter = rand::rng().random_range(0.75..1.25);
        self.next = Instant::now() + self.interval().mul_f64(jitter);
    }

    /// Takes over up to `count` deliveries that went unrenewed for the
    /// redelivery timeout.
    pub async fn claim(&mut self, count: usize) -> Result<Vec<Delivery>> {
        let worker = Arc::clone(&self.worker);
        let docket = &worker.docket;
        let settings = &worker.settings;
        let mut connection = docket.handle();
        let idle = u64::try_from(settings.redelivery_timeout.as_millis()).unwrap_or(u64::MAX);
        let reply: RedisResult<StreamAutoClaimReply> = redis::cmd("XAUTOCLAIM")
            .arg(docket.keys().stream())
            .arg(WORKER_GROUP)
            .arg(&settings.name)
            .arg(idle)
            .arg(&self.cursor)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut connection)
            .await;
        // Without a group there is nothing to claim, so the walk starts over.
        let reply = regrouping(docket, reply)
            .await?
            .unwrap_or(StreamAutoClaimReply {
                next_stream_id: "0-0".to_owned(),
                ..StreamAutoClaimReply::default()
            });
        self.cursor = reply.next_stream_id;
        if self.cursor == "0-0" {
            self.rest();
        }
        Ok(deliveries(docket, reply.claimed.into_iter(), true).await)
    }
}

/// Renews the deliveries of this worker's running tasks, so that no other
/// worker takes them over.
pub(super) async fn renew_leases(worker: Arc<Shared>) {
    let docket = &worker.docket;
    let interval = worker.settings.redelivery_timeout / 4;
    loop {
        tokio::time::sleep(interval).await;
        let ids = worker.active_ids();
        let mut connection = docket.handle();
        for chunk in ids.chunks(RENEWAL_BATCH) {
            let renewed: RedisResult<redis::Value> = redis::cmd("XCLAIM")
                .arg(docket.keys().stream())
                .arg(WORKER_GROUP)
                .arg(&worker.settings.name)
                .arg(0)
                .arg(chunk)
                .arg("IDLE")
                .arg(0)
                .arg("JUSTID")
                .query_async(&mut connection)
                .await;
            if let Err(error) = renewed {
                tracing::warn!(%error, "renewing task leases failed");
            }
        }
    }
}
