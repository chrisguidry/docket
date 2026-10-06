//! docket's metrics, with the names, units, and descriptions pydocket gives
//! them, so a dashboard reads the same whichever language runs the worker.

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};

/// Every instrument docket records.
pub(crate) struct Metrics {
    pub tasks_added: Counter<u64>,
    pub tasks_replaced: Counter<u64>,
    pub tasks_scheduled: Counter<u64>,
    pub tasks_cancelled: Counter<u64>,
    pub tasks_started: Counter<u64>,
    pub tasks_redelivered: Counter<u64>,
    pub tasks_stricken: Counter<u64>,
    pub tasks_superseded: Counter<u64>,
    pub tasks_completed: Counter<u64>,
    pub tasks_failed: Counter<u64>,
    pub tasks_succeeded: Counter<u64>,
    pub tasks_retried: Counter<u64>,
    pub tasks_perpetuated: Counter<u64>,
    pub task_duration: Histogram<f64>,
    pub task_punctuality: Histogram<f64>,
    pub tasks_running: UpDownCounter<i64>,
    pub redis_disruptions: Counter<u64>,
    pub strikes_in_effect: UpDownCounter<i64>,
    pub queue_depth: Gauge<u64>,
    pub schedule_depth: Gauge<u64>,
}

impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_description(description)
                .with_unit("1")
                .build()
        };
        let histogram = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_description(description)
                .with_unit("s")
                .build()
        };
        let up_down = |name: &'static str, description: &'static str| {
            meter
                .i64_up_down_counter(name)
                .with_description(description)
                .with_unit("1")
                .build()
        };
        let gauge = |name: &'static str, description: &'static str| {
            meter
                .u64_gauge(name)
                .with_description(description)
                .with_unit("1")
                .build()
        };
        Self {
            tasks_added: counter("docket_tasks_added", "How many tasks added to the docket"),
            tasks_replaced: counter(
                "docket_tasks_replaced",
                "How many tasks replaced on the docket",
            ),
            tasks_scheduled: counter(
                "docket_tasks_scheduled",
                "How many tasks added or replaced on the docket",
            ),
            tasks_cancelled: counter(
                "docket_tasks_cancelled",
                "How many tasks cancelled from the docket",
            ),
            tasks_started: counter("docket_tasks_started", "How many tasks started"),
            tasks_redelivered: counter(
                "docket_tasks_redelivered",
                "How many tasks started that were redelivered from another worker",
            ),
            tasks_stricken: counter(
                "docket_tasks_stricken",
                "How many tasks have been stricken from executing",
            ),
            tasks_superseded: counter(
                "docket_tasks_superseded",
                "How many tasks were superseded by a newer schedule before execution",
            ),
            tasks_completed: counter(
                "docket_tasks_completed",
                "How many tasks that have completed in any state",
            ),
            tasks_failed: counter("docket_tasks_failed", "How many tasks that have failed"),
            tasks_succeeded: counter(
                "docket_tasks_succeeded",
                "How many tasks that have succeeded",
            ),
            tasks_retried: counter(
                "docket_tasks_retried",
                "How many tasks that have been retried",
            ),
            tasks_perpetuated: counter(
                "docket_tasks_perpetuated",
                "How many tasks that have been self-perpetuated",
            ),
            task_duration: histogram("docket_task_duration", "How long tasks take to complete"),
            task_punctuality: histogram(
                "docket_task_punctuality",
                "How close a task was to its scheduled time",
            ),
            tasks_running: up_down(
                "docket_tasks_running",
                "How many tasks that are currently running",
            ),
            redis_disruptions: counter(
                "docket_redis_disruptions",
                "How many times Redis dropped, timed out, or refused a command \
                 that the worker then retried",
            ),
            strikes_in_effect: up_down(
                "docket_strikes_in_effect",
                "How many strikes are currently in effect",
            ),
            queue_depth: gauge(
                "docket_queue_depth",
                "How many tasks are due to be executed now",
            ),
            schedule_depth: gauge(
                "docket_schedule_depth",
                "How many tasks are scheduled to be executed in the future",
            ),
        }
    }
}
