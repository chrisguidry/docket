//! The Redis side of one delivery: its claim, its terminal state, and the
//! reschedules that take its place.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use opentelemetry::KeyValue;
use redis::Value;

use super::session::Delivery;
use crate::behaviors::{AdmissionBlocked, AfterCompletion};
use crate::docket::{Docket, Placement};
use crate::error::Result;
use crate::execution::{Disposition, Message, State};
use crate::keys::WORKER_GROUP;
use crate::scripts;
use crate::telemetry::{self, format_duration};
use crate::wire::iso;

/// How soon a blocked task tries again when its hook names no delay.
const ADMISSION_RETRY_DELAY: Duration = Duration::from_millis(100);

/// What a claim found.
pub(super) enum Claim {
    /// The task is this worker's to run, at this generation.
    Claimed(i64),
    /// Someone cancelled the task.
    Cancelled,
    /// A newer copy of the task took its key.
    Superseded,
}

/// One delivery, as the worker records what becomes of it.
pub(super) struct Run<'a> {
    pub docket: &'a Docket,
    pub delivery: &'a Delivery,
    pub worker: &'a str,
    /// The task's call, as its log lines show it.
    pub call: String,
}

impl Run<'_> {
    fn message(&self) -> &Message {
        &self.delivery.message
    }

    fn key(&self) -> &str {
        &self.delivery.message.key
    }

    /// The labels of this run's metrics.
    pub fn labels(&self) -> Vec<KeyValue> {
        telemetry::run_labels(self.docket.name(), self.worker, &self.message().function)
    }

    /// The labels of a metric that says where a task was turned away.
    pub fn labels_where(&self, place: &'static str) -> Vec<KeyValue> {
        let mut labels = self.labels();
        labels.push(KeyValue::new("docket.where", place));
        labels
    }

    /// Claims the delivery.  A claim refused for a superseded or cancelled
    /// task also acknowledges its message.
    pub async fn claim(&self) -> Result<Claim> {
        let keys = self.docket.keys();
        let key = self.key();
        let started_at = iso(self.docket.now());
        let payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": "running",
            "worker": self.worker,
            "started_at": started_at,
        });
        let call = scripts::Claim {
            runs_key: keys.runs(key),
            progress_key: keys.progress(key),
            known_key: keys.known(key),
            stream_id_key: keys.stream_id(key),
            state_channel: keys.state(key),
            stream_key: keys.stream(),
            worker: self.worker.to_owned(),
            started_at,
            generation: self.message().generation,
            state_payload: payload.to_string(),
            key_json: serde_json::Value::from(key).to_string(),
            worker_group_name: WORKER_GROUP.to_owned(),
            message_id: self.delivery.id.clone(),
        }
        .call();
        let mut connection = self.docket.handle();
        // The reply is the claim's status, the runs hash, and the progress
        // hash.
        let (status, runs, _): (String, HashMap<String, String>, Value) =
            call.run(&mut connection).await?;
        Ok(match status.as_str() {
            "OK" => Claim::Claimed(
                runs.get("generation")
                    .and_then(|generation| generation.parse().ok())
                    .unwrap_or(self.message().generation),
            ),
            "CANCELLED" => Claim::Cancelled,
            _ => Claim::Superseded,
        })
    }

    /// Ends a struck delivery as cancelled without running it.
    pub async fn strike(&self) -> Result<()> {
        let keys = self.docket.keys();
        let key = self.key();
        let mut connection = self.docket.handle();
        let () = redis::pipe()
            .hdel(keys.runs(key), &["known", "stream_id"])
            .ignore()
            .del(&[keys.known(key), keys.stream_id(key)])
            .ignore()
            .query_async(&mut connection)
            .await?;
        self.terminal(State::Cancelled, self.message().generation, Vec::new())
            .await
    }

    /// Records a terminal state and acknowledges the delivery.
    pub async fn terminal(
        &self,
        state: State,
        generation: i64,
        extra_fields: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let keys = self.docket.keys();
        let key = self.key();
        let completed_at = iso(self.docket.now());
        let mut payload = serde_json::json!({
            "type": "state",
            "key": key,
            "state": state.as_str(),
            "completed_at": completed_at,
        });
        if let Some((_, error)) = extra_fields.iter().find(|(field, _)| field == "error") {
            payload["error"] = String::from_utf8_lossy(error).into_owned().into();
        }
        let call = scripts::Terminal {
            runs_key: keys.runs(key),
            state_channel: keys.state(key),
            progress_key: keys.progress(key),
            stream_key: keys.stream(),
            generation,
            state: state.as_str().to_owned(),
            completed_at,
            ttl_seconds: self.docket.ttl_seconds(),
            state_payload: payload.to_string(),
            worker_group_name: WORKER_GROUP.to_owned(),
            message_id: self.delivery.id.clone(),
            extra_fields,
        }
        .call();
        let mut connection = self.docket.handle();
        call.run::<Value, _>(&mut connection).await?;
        Ok(())
    }

    /// Puts the delivery back in the docket at `when`, acknowledging it in
    /// the same script.  With a nonzero `expected_generation`, a newer
    /// generation on the key refuses the reschedule and leaves the delivery
    /// pending.
    async fn reschedule(
        &self,
        when: chrono::DateTime<Utc>,
        attempt: u32,
        generation: i64,
        expected_generation: i64,
    ) -> Result<Disposition> {
        let mut message = self.message().clone();
        message.when = when;
        message.attempt = attempt;
        message.generation = generation;
        self.docket
            .place(Placement {
                message,
                replace: true,
                reschedule_message_id: self.delivery.id.clone(),
                expected_generation,
            })
            .await
    }

    /// Schedules the next attempt of a failed run, unless a replace took the
    /// key while it ran.
    pub async fn retry(
        &self,
        when: chrono::DateTime<Utc>,
        generation: i64,
        duration: f64,
        error: &str,
    ) -> Result<()> {
        // The retry carries this run's generation, so a replace made while
        // it ran keeps its own time and arguments.
        let disposition = self
            .reschedule(when, self.message().attempt + 1, generation, generation)
            .await?;
        let metrics = &self.docket.telemetry().metrics;
        let took = format_duration(duration);
        tracing::error!(error, "↩ [{took}] {}", self.call);
        if disposition == Disposition::Superseded {
            metrics.tasks_superseded.add(1, &self.labels_where("retry"));
            tracing::info!("↬ [{took}] {} (superseded)", self.call);
            return self
                .terminal(
                    State::Failed,
                    generation,
                    vec![("error".into(), error.as_bytes().to_vec())],
                )
                .await;
        }
        metrics.tasks_retried.add(1, &self.labels());
        tracing::info!("↫ [{took}] {}", self.call);
        Ok(())
    }

    /// Acts on a blocked admission: the hook already parked the task, or it
    /// goes back in the docket, or it is dropped.
    pub async fn blocked(&self, blocked: &AdmissionBlocked, generation: i64) -> Result<()> {
        if blocked.handled {
            return Ok(());
        }
        if !blocked.reschedule {
            tracing::debug!(
                "⏭ Task {} blocked by admission control, dropping",
                self.key()
            );
            return self
                .terminal(State::Cancelled, generation, Vec::new())
                .await;
        }
        tracing::debug!(
            "⏳ Task {} blocked by admission control, rescheduling",
            self.key()
        );
        let delay = blocked
            .retry_delay
            .filter(|delay| !delay.is_zero())
            .unwrap_or(ADMISSION_RETRY_DELAY);
        let when =
            self.docket.now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX);
        self.reschedule(when, self.message().attempt, generation, 0)
            .await
            .map(drop)
    }

    /// Ends a completed task, after acting on its completion hook's decision.
    pub async fn succeed(
        &self,
        after: Option<&AfterCompletion>,
        output: serde_json::Value,
        generation: i64,
        duration: f64,
    ) -> Result<()> {
        let handled = match after {
            Some(after) => self.after_completion(after, generation, duration).await?,
            None => false,
        };
        if handled {
            return self
                .terminal(State::Completed, generation, Vec::new())
                .await;
        }
        self.store(output, generation).await?;
        self.terminal(State::Completed, generation, self.result_field())
            .await?;
        tracing::info!("↩ [{}] {}", format_duration(duration), self.call);
        Ok(())
    }

    fn result_field(&self) -> Vec<(String, Vec<u8>)> {
        vec![("result_key".to_owned(), self.key().as_bytes().to_vec())]
    }

    /// Acts on a completion hook's decision, and returns whether it took the
    /// place of the task's normal ending.
    pub async fn after_completion(
        &self,
        after: &AfterCompletion,
        generation: i64,
        duration: f64,
    ) -> Result<bool> {
        match after {
            AfterCompletion::Finish => Ok(false),
            AfterCompletion::Cancel => {
                // Cancel under this run's generation, so a replace made
                // while it ran survives the stop.
                if self.docket.cancel_quietly(self.key(), generation).await? {
                    return Ok(false);
                }
                self.superseded(duration);
                Ok(true)
            }
            AfterCompletion::Reschedule { when, args } => {
                let mut message = self.message().clone();
                message.when = *when;
                message.attempt = 1;
                if let Some(args) = args {
                    message.args = args.to_string();
                }
                // A perpetual task's next run is a replace, which checks
                // strikes and counts like any other.
                let disposition = self
                    .docket
                    .schedule(Placement {
                        message,
                        replace: true,
                        reschedule_message_id: String::new(),
                        expected_generation: generation,
                    })
                    .await?;
                if disposition == Disposition::Superseded {
                    self.superseded(duration);
                } else {
                    let metrics = &self.docket.telemetry().metrics;
                    metrics.tasks_perpetuated.add(1, &self.labels());
                    let took = format_duration(duration);
                    tracing::info!("↫ [{took}] {}", self.call);
                }
                Ok(true)
            }
        }
    }

    /// Records a completion hook's reschedule or cancel that a newer
    /// generation on the key refused.
    fn superseded(&self, duration: f64) {
        let metrics = &self.docket.telemetry().metrics;
        metrics
            .tasks_superseded
            .add(1, &self.labels_where("on_complete"));
        let took = format_duration(duration);
        tracing::info!("↬ [{took}] {} (superseded)", self.call);
    }

    /// Stores a completed task's output, or removes an earlier run's output
    /// when there is none to keep.  The script writes only while this run's
    /// generation is current, so a replaced run that finishes after its
    /// replacement cannot overwrite the replacement's output.
    async fn store(&self, output: serde_json::Value, generation: i64) -> Result<()> {
        let ttl = self.docket.ttl_seconds();
        let stored = if ttl == 0 || output.is_null() {
            String::new()
        } else {
            serde_json::json!({ "ok": output }).to_string()
        };
        let keys = self.docket.keys();
        let () = STORE_RESULT
            .key(keys.runs(self.key()))
            .key(keys.result(self.key()))
            .arg(generation)
            .arg(stored)
            .arg(ttl)
            .invoke_async(&mut self.docket.handle())
            .await?;
        Ok(())
    }
}

/// docket-rs's own result layout, so this script is not one of the shared
/// ones in protocol/.
static STORE_RESULT: std::sync::LazyLock<redis::Script> =
    std::sync::LazyLock::new(|| redis::Script::new(include_str!("store_result.lua")));
