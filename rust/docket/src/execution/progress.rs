//! A task's progress: how far it has come, of how far, and a message.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use crate::docket::Docket;
use crate::error::Result;
use crate::scripts;
use crate::wire::{iso, parse_iso};

/// The total a task's progress has until it sets its own.
const DEFAULT_TOTAL: i64 = 100;

/// Reports a running task's progress.  Every change is stored and published
/// to anyone following the task.
#[derive(Clone)]
pub struct Progress {
    docket: Docket,
    key: String,
    current: Arc<AtomicI64>,
}

impl Progress {
    pub(crate) fn new(docket: Docket, key: String) -> Self {
        Self {
            docket,
            key,
            current: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Sets the total that progress counts toward.
    pub async fn set_total(&self, total: i64) -> Result<()> {
        let now = iso(Utc::now());
        self.write(
            vec![("total", total.to_string()), ("updated_at", now.clone())],
            false,
            serde_json::json!({ "total": total, "updated_at": now }),
        )
        .await
    }

    /// Sets the message, or clears it with `None`.
    pub async fn set_message(&self, message: Option<&str>) -> Result<()> {
        let now = iso(Utc::now());
        let mut fields = vec![("updated_at", now.clone())];
        if let Some(message) = message {
            fields.push(("message", message.to_owned()));
        }
        self.write(
            fields,
            message.is_none(),
            serde_json::json!({ "message": message, "updated_at": now }),
        )
        .await
    }

    /// Adds `amount` to the current progress.
    pub async fn increment(&self, amount: i64) -> Result<()> {
        let keys = self.docket.keys();
        let now = iso(Utc::now());
        let mut connection = self.docket.connection().await?;
        let (current,): (i64,) = redis::pipe()
            .hincr(keys.progress(&self.key), "current", amount)
            .hset(keys.progress(&self.key), "updated_at", &now)
            .ignore()
            .query_async(&mut connection)
            .await?;
        self.current.store(current, Ordering::Relaxed);
        let snapshot = read(&self.docket, &self.key).await?;
        let payload = self.payload(serde_json::json!({
            "current": current,
            "total": snapshot.total,
            "message": snapshot.message,
            "updated_at": now,
        }));
        let _: i64 = connection
            .publish(keys.progress(&self.key), payload.to_string())
            .await?;
        Ok(())
    }

    async fn write(
        &self,
        fields: Vec<(&str, String)>,
        clear_message: bool,
        changes: serde_json::Value,
    ) -> Result<()> {
        let snapshot = read(&self.docket, &self.key).await?;
        let mut payload = serde_json::json!({
            "current": self.current.load(Ordering::Relaxed),
            "total": snapshot.total,
            "message": snapshot.message,
            "updated_at": snapshot.updated_at.map(iso),
        });
        if let (Some(payload), Some(changes)) = (payload.as_object_mut(), changes.as_object()) {
            payload.extend(changes.clone());
        }
        let call = scripts::ProgressWrite {
            progress_key: self.docket.keys().progress(&self.key),
            payload: self.payload(payload).to_string(),
            clear_message,
            fields: fields
                .into_iter()
                .map(|(field, value)| (field.to_owned(), value.into_bytes()))
                .collect(),
        }
        .call();
        let mut connection = self.docket.connection().await?;
        call.run::<redis::Value, _>(&mut connection).await?;
        Ok(())
    }

    fn payload(&self, mut fields: serde_json::Value) -> serde_json::Value {
        if let Some(object) = fields.as_object_mut() {
            object.insert("type".to_owned(), "progress".into());
            object.insert("key".to_owned(), self.key.clone().into());
        }
        fields
    }
}

/// A run's progress at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProgressSnapshot {
    /// How far the task has come, or `None` before it starts.
    pub current: Option<i64>,
    /// Where it is going.
    pub total: i64,
    /// What it says it is doing.
    pub message: Option<String>,
    /// When it last reported.
    pub updated_at: Option<DateTime<Utc>>,
}

pub(crate) async fn read(docket: &Docket, key: &str) -> Result<ProgressSnapshot> {
    let mut connection = docket.connection().await?;
    let hash: HashMap<String, String> = connection.hgetall(docket.keys().progress(key)).await?;
    if hash.is_empty() {
        return Ok(ProgressSnapshot {
            current: None,
            total: DEFAULT_TOTAL,
            message: None,
            updated_at: None,
        });
    }
    let number = |field: &str, default: i64| {
        hash.get(field)
            .and_then(|n| n.parse().ok())
            .unwrap_or(default)
    };
    Ok(ProgressSnapshot {
        current: Some(number("current", 0)),
        total: number("total", DEFAULT_TOTAL),
        message: hash.get("message").cloned(),
        updated_at: hash.get("updated_at").and_then(|text| parse_iso(text)),
    })
}
