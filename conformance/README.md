# Conformance

The conformance driver runs the same scenarios against every docket
implementation, and makes the same assertions on each one.  It also holds
the chaos run, which kills workers and restarts Redis under load.

```bash
uv run python -m conformance backoff
uv run python -m conformance graceful-drain --implementation python@release
uv run python -m conformance chaos --implementation python@main --implementation python@release
```

## Implementations

An implementation is named `language@version`.

| Name | What runs |
|---|---|
| `python@main` | pydocket from the working tree.  This is the default. |
| `python@release` | The newest pydocket release, from its git tag. |
| `python@0.25.2` | That version of pydocket, from PyPI. |

Each implementation gets its own virtual environment, and every one of them
runs the agent from the working tree.  When you give more than one, the
driver chooses one at random for each agent it starts.

## Scenarios

| Scenario | What it shows |
|---|---|
| `backoff` | A failing task makes four attempts, with delays of 0.5 s, 1 s, and 2 s between them, then stops. |
| `perpetual` | An automatic perpetual task runs every 0.5 s, and a new worker takes it over after the first worker is killed. |
| `cancel-before-start` | A future task cancelled before it is due never runs. |
| `graceful-drain` | After SIGTERM, and again after SIGINT, workers finish their tasks and exit 0. |
| `concurrency-limit` | Three workers never run more than two tasks under one `ConcurrencyLimit(max_concurrent=2)` at once, and they do reach two. |
| `redelivery` | When a worker dies during a task, another worker runs the task again after the lease runs out.  The second run keeps attempt 1. |
| `perpetual-single-flight` | Three workers share an automatic perpetual task.  Its runs never overlap, even when all the workers die during a run and new ones start. |
| `same-key` | Two producers add and then replace the same key at about the same time, and the task runs once, at the time of the last replace. |
| `chaos` | Every task added runs while workers die and Redis restarts. |
| `telemetry` | The producer and the worker emit the metrics, spans, and Prometheus series in [the telemetry spec](../plans/telemetry-parity.md).  With more than one implementation, the driver runs each one in turn, and their telemetry must be equal. |

## The agent contract

Each language ships an agent program in its own tree, such as
`python/conformance-agent/`.  The driver starts it like this:

```
<agent> produce|worker --scenario NAME --url URL --docket NAME
```

- `produce` schedules the scenario's tasks and exits 0.
- `worker` runs a worker for the scenario's tasks.  SIGTERM and SIGINT stop
  it the way they stop a deployed worker.

The scenario's tasks add events to the stream `conformance:{scenario}:events`.
Each event has these fields:

| Field | Value |
|---|---|
| `event` | What happened, such as `attempt`, `ran`, `started`, or `added`. |
| `task` | The task's name. |
| `key` | The task's key. |
| `attempt` | The attempt number, from 1.  A producer writes 0. |
| `worker` | The worker's name.  A producer writes an empty string. |
| `time` | Seconds since the Unix epoch, as a decimal. |

The driver reads only these events and the run state that docket's shared
Lua scripts keep, never anything particular to one language.  A new
language passes when its agent writes the same events.

## The telemetry contract

The `telemetry` scenario runs its workload twice for each implementation,
each time on a new docket.  The workload is in the docstring of
`python/conformance-agent/scenarios/telemetry.py`.  Each agent gets
`CONFORMANCE_PHASE`, and its producer starts every key with that value and a
colon, such as `otlp:succeed`.

In the `otlp` phase, the driver sets these variables for the worker and the
producer:

| Variable | Value |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | The driver's receiver, such as `http://127.0.0.1:PORT` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` |
| `OTEL_METRIC_EXPORT_INTERVAL` | `500` |
| `OTEL_BSP_SCHEDULE_DELAY` | `200` |
| `OTEL_SERVICE_NAME` | `conformance-worker` or `conformance-producer` |

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the agent installs a tracer
provider and a meter provider before it connects to the docket.  They export
spans and metrics over OTLP/HTTP with protobuf bodies, to the paths
`/v1/traces` and `/v1/metrics`, with cumulative temporality.  The agent uses
the W3C trace context propagator.  It shuts both providers down before it
exits, so that the driver gets everything.  The driver tells the processes
apart by the resource's `service.name`.

In the `prometheus` phase, the driver sets `DOCKET_WORKER_METRICS_PORT` for
the worker, and no OTLP variables.  The worker serves its metrics in the
Prometheus text format at `http://127.0.0.1:PORT/metrics`, with a
`target_info` family.

The driver writes each implementation's normalized telemetry to
`telemetry-NAME.json`, beside the agents' logs.
