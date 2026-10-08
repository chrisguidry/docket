# Telemetry parity

docket-rs emits the same logs, metrics, and traces as pydocket, and the
conformance driver checks that every implementation's metrics and traces
agree.  pydocket is the spec.  This file lists what that spec is, where
docket-rs records each signal, and what the conformance scenario checks.

## Decisions

- docket-rs records metrics and spans through the `opentelemetry` API, the
  way pydocket uses `opentelemetry-api`.  Nothing is recorded until the
  application installs a meter provider or a tracer provider.
- The `opentelemetry` crate binds a meter or a tracer to the global provider
  when it is made, not when it is used.  So a docket makes its instruments
  and tracers when it connects, and an application installs its providers
  before it connects a docket.  pydocket's proxy objects do not have this
  rule.
- Task logs stay on `tracing`.  The OpenTelemetry spans and the `tracing`
  spans are separate: docket-rs does not depend on `tracing-opentelemetry`.
- The message carries the producer's trace context in the fields that the
  global text map propagator writes, such as `traceparent`.  The Rust
  default propagator writes nothing, so an application that wants the links
  installs `TraceContextPropagator`, as pydocket's default does.
- `docket::prometheus` (the `prometheus` feature) serves the metrics in the
  Prometheus text format, with the same names, labels, and series as
  pydocket's vendored exporter.  `WorkerArgs` gains `--metrics-port` and
  `--healthcheck-port`, with pydocket's environment variables.
- `docket_cache_size` measures two caches of Python function signatures.
  docket-rs has no such caches, so it does not record the metric, and the
  conformance scenario ignores it.
- A task with no handler runs a default fallback that logs a warning and
  completes, as pydocket's `default_fallback_task` does, so the run is
  counted and traced like any other.

## Metrics

The meter is named `docket`.  Every counter has the unit `1`.

| Name | Kind | Unit | Attributes | Recorded |
|---|---|---|---|---|
| `docket_tasks_added` | counter | `1` | docket, task | an add that was not struck, superseded, or failed |
| `docket_tasks_replaced` | counter | `1` | docket, task | a replace that was not struck, superseded, or failed |
| `docket_tasks_scheduled` | counter | `1` | docket, task | an add whose disposition is scheduled, and every counted replace |
| `docket_tasks_cancelled` | counter | `1` | docket, task | every counted replace |
| `docket_tasks_cancelled` | counter | `1` | docket | `Docket::cancel` |
| `docket_tasks_stricken` | counter | `1` | docket, task, where=`docket` | an add or replace that a strike blocked |
| `docket_tasks_stricken` | counter | `1` | docket, worker, task, where=`worker` | a delivery that a strike blocked |
| `docket_tasks_superseded` | counter | `1` | docket, worker, task, where=`worker` | a claim refused because a newer copy took the key |
| `docket_tasks_superseded` | counter | `1` | docket, worker, task, where=`on_complete` | a perpetual reschedule, or a perpetual task's cancel of itself, refused for the same reason |
| `docket_tasks_superseded` | counter | `1` | docket, worker, task, where=`retry` | a retry refused for the same reason |
| `docket_tasks_started` | counter | `1` | docket, worker, task | each claimed run, before admission |
| `docket_tasks_redelivered` | counter | `1` | docket, worker, task | a started run that came from the redelivery sweep |
| `docket_tasks_running` | up-down counter | `1` | docket, worker, task | +1 at start, -1 at the end of the run |
| `docket_task_punctuality` | histogram | `s` | docket, worker, task | start time minus the due time |
| `docket_tasks_succeeded` | counter | `1` | docket, worker, task | the handler returned |
| `docket_tasks_failed` | counter | `1` | docket, worker, task | the handler failed, retried or not |
| `docket_tasks_retried` | counter | `1` | docket, worker, task | a retry was scheduled |
| `docket_tasks_perpetuated` | counter | `1` | docket, worker, task | a perpetual task scheduled its next run |
| `docket_tasks_completed` | counter | `1` | docket, worker, task | every started run, at its end |
| `docket_task_duration` | histogram | `s` | docket, worker, task | every started run, at its end |
| `docket_redis_disruptions` | counter | `1` | docket, worker | the worker retried after Redis dropped, refused, or timed out |
| `docket_strikes_in_effect` | up-down counter | `1` | docket, strike labels | +1 for each strike the monitor reads, -1 for each restore |
| `docket_queue_depth` | gauge | `1` | docket | each heartbeat: stream length plus due queued tasks |
| `docket_schedule_depth` | gauge | `1` | docket | each heartbeat: queued tasks not yet due |

The attribute keys are `docket.name`, `docket.worker`, `docket.task`, and
`docket.where`.  A strike's labels are `docket.task` for a whole task, and
`docket.parameter`, `docket.operator`, and `docket.value` for a condition.

These follow pydocket exactly, including three details that read like
accidents:

- A run that admission blocks, such as a concurrency limit that parks it,
  still counts as started and completed, with a duration of 0.
- A perpetual task's reschedule is a replace, so it also counts as
  replaced, cancelled, and scheduled, with the docket and task attributes
  only.
- A perpetual task that stops itself cancels its key without counting a
  cancellation.

## Traces

| Span | Tracer | Kind | Attributes | Status |
|---|---|---|---|---|
| `docket.add`, `docket.replace` | `docket.docket` | internal | docket, task, key, when, attempt, `code.function.name`, disposition | unset |
| `docket.add_many`, `docket.replace_many` | `docket.docket` | internal | docket, `docket.batch.count`, `docket.batch.stricken` | unset |
| `docket.cancel` | `docket.docket` | internal | docket, key | unset |
| `docket.strike`, `docket.restore` | `docket.docket` | internal | docket, strike labels | unset |
| `docket.clear` | `docket.docket` | internal | docket | unset |
| the task's name | `docket.worker` | consumer | docket, worker, task, key, when, attempt, `code.function.name` | ok, or error with the failure's message and an `exception` event |

- The keys are `docket.name`, `docket.worker`, `docket.task`, `docket.key`,
  `docket.when` (ISO 8601), `docket.attempt` (an integer), and
  `docket.disposition` (`scheduled`, `already_scheduled`, `struck`, or
  `superseded`).
- Every message that goes into the docket carries the trace context that is
  current when it goes in: the `docket.add` or `docket.replace` span for a
  producer, and the run's consumer span for a retry or a parked task.
- `stream_due_tasks.lua` copies a fixed list of fields when a future task
  becomes due, and the trace context is not on it.  So a task scheduled for
  later runs with no link, in every language, until that script carries the
  context too.
- A consumer span starts a new trace.  It links to the span in its message's
  trace context, when there is one.
- A run that admission blocks ends its span with status ok.  A run that
  `Docket::cancel` stops ends its span with status ok.

## Prometheus

The exporter renders what the SDK collects the way pydocket's vendored
`PrometheusMetricReader` and `prometheus_client` do:

- A counter is `NAME_total`.  A histogram is `NAME_UNIT_bucket`, `_count`,
  and `_sum`, with `le` labels such as `5.0` and `+Inf`.  An up-down counter
  and a gauge are gauges.  The vendored exporter gives `prometheus_client`
  no creation times, so there are no `_created` series.
- Units map the way the vendored exporter maps them: `s` is `seconds` and
  `1` adds nothing.
- Label names replace each run of characters other than letters and digits
  with `_`, so `docket.name` is `docket_name`.  Labels sort by name.
- A `target_info` gauge carries the resource attributes.

## Logs

docket-rs logs the same events as pydocket, at the same levels, with the
same messages: `↪`/`↬` when a run starts, `↩` when it ends, `✗` for a
cancellation, `🗙` for a strike, `↫` for a perpetual reschedule, and the
worker's startup list.  A run's log fields are `docket.name`,
`docket.worker`, `docket.task`, `docket.key`, `docket.when`, and
`docket.attempt`, as in pydocket's `extra`.

A task's call in a log line shows only the arguments marked to be logged,
the way `Logged` marks them in pydocket.  In docket-rs, a field takes
`#[task(logged)]` or `#[task(logged(length_only))]`.

## The conformance scenario

The `telemetry` scenario runs one workload against each implementation it
is given, and each implementation's producer and worker export to the
driver:

- Traces and metrics go over OTLP/HTTP to a receiver in the driver.
- A second worker serves Prometheus metrics, which the driver scrapes.

The workload adds a task that succeeds, one that fails, one that fails once
and then succeeds on a retry, a perpetual task that stops itself after three
runs, a future task that is cancelled, a future task that is replaced to run
now, and a task that is struck and then restored.

The driver checks each implementation against the tables above: the metric
names, kinds, units, descriptions, attribute keys, and the counter values;
the span names, tracers, kinds, attributes, statuses, events, and links; and
the Prometheus families, label names, and sample values.  With more than one
implementation, it also checks that their normalized telemetry is equal.

## What the work found in pydocket

Each of these is a defect in pydocket, and each is a fix of its own:

- `stream_due_tasks.lua` drops the trace context of a task scheduled for
  later, as the Traces section says.  The script is shared, so docket-rs
  loses the context in the same way.
- The strike monitor counts `docket_redis_disruptions` with the attribute
  `docket`, where every other recording uses `docket.name`
  (`python/src/docket/strikelist.py`).  docket-rs uses `docket.name`; the
  conformance workload does not disrupt Redis, so it does not see this.
- `docs/production.md` names a run's span `docket.task.{function_name}`,
  but the span is named for the function alone.
