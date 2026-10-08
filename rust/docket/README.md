# docket-rs

docket-rs runs functions on other machines, now or later, with Redis keeping
the queue.  It is the Rust implementation of
[docket](https://github.com/chrisguidry/docket): the same concepts, the same
task behaviors, and the same Redis data model as pydocket, in Rust's idioms.

docket-rs is a library.  Your application opens a docket, registers its
tasks, and runs a worker.

```toml
[dependencies]
docket-rs = "0.2"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## A task, a worker, and a producer

```rust,no_run
use std::time::Duration;

use docket::{Docket, ExponentialRetry, Task, Worker};
use serde::{Deserialize, Serialize};

// The argument type names the task.  Producers need only this type.
#[derive(Serialize, Deserialize, Task)]
#[task(name = "charge", output = Receipt)]
pub struct Charge {
    pub customer: u64,
    pub cents: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Receipt {
    pub id: String,
}

#[tokio::main]
async fn main() -> docket::Result<()> {
    let docket = Docket::connect("orders", "redis://localhost:6379/0").await?;

    // Workers register a handler, with the behaviors that change how it runs.
    docket
        .register(|ctx, args: Charge| async move {
            println!("charging {} on attempt {}", args.customer, ctx.attempt());
            Ok::<_, std::io::Error>(Receipt { id: format!("r-{}", args.customer) })
        })
        .with(ExponentialRetry::attempts(5).minimum_delay(Duration::from_secs(1)));

    // Producers add tasks, now or later.  A key makes the add idempotent.
    let execution = docket.add(Charge { customer: 7, cents: 1999 }).key("order-9").await?;

    Worker::new(docket.clone()).run_until_finished().await?;
    let receipt = execution.result().await?;
    assert_eq!(receipt.id, "r-7");
    Ok(())
}
```

## Behaviors

| Behavior | What it does |
|---|---|
| `Retry`, `ExponentialRetry` | Runs a failed task again, after a fixed or a doubling delay.  A handler can return `ForcedRetry` to choose the delay. |
| `Timeout` | Stops a task that runs too long.  The task can extend its own deadline. |
| `Perpetual` | Runs a task again after each run.  `.automatic()` makes every worker add it at startup. |
| `Cron` | Runs a task on a cron schedule, in a time zone. |
| `ConcurrencyLimit` | Caps how many copies run at once, for the whole task or per value of an argument field. |
| `Debounce` | Runs a task once after its calls stop coming. |
| `Cooldown` | Drops calls that come too soon after the last run. |
| `RateLimit` | Caps runs per sliding window, waiting for room or dropping the excess. |

Your own behaviors plug into the same four hooks: admission, runtime,
failure, and completion.  See `docket::behaviors`.

## Redis

A docket URL is `redis://`, `rediss://`, `unix://`, `redis+cluster://`,
`redis+sentinel://host:port/service`, or `memory://`.  docket-rs is tested
with Redis 6.2 and 8.10, Redis 8.10 in cluster mode, with ACLs, and behind
Sentinel, and Valkey 8.0 and 9.1.

`rediss://`, `rediss+cluster://`, and `rediss+sentinel://` connect over TLS
through rustls, with its `ring` crypto backend.  The default `tls` feature
brings both.  If another crate in your build turns on rustls's `aws-lc-rs`
backend too, rustls cannot pick one, so call
`rustls::crypto::CryptoProvider::install_default` before connecting.  Without
TLS, turn off the default features:

```toml
[dependencies]
docket-rs = { version = "0.2", default-features = false }
```

Every connection docket-rs opens to Redis gives up on a connect after 10
seconds, and uses TCP keepalive to find a Redis that vanished without closing
the connection.  As in pydocket, a command waits for Redis as long as it takes,
because some of docket's Lua scripts, such as clearing a large docket, run for
a long time.  Commands to a cluster retry up to 10 times, waiting from 10
milliseconds to 1 second, so they survive the redirects of a slot migration.
`Docket::builder` changes each of these, and sets a response timeout:

```rust,no_run
use std::time::Duration;

use docket::Docket;

#[tokio::main]
async fn main() -> docket::Result<()> {
    let docket = Docket::builder("orders", "redis+cluster://node-1:6379")
        .connection_timeout(Duration::from_secs(5))
        .response_timeout(Duration::from_secs(10))
        .retries(10)
        .retry_backoff(Duration::from_millis(10), Duration::from_secs(1))
        .tcp_keepalive(Duration::from_secs(30), Duration::from_secs(5), 3)
        .connect()
        .await?;
    Ok(())
}
```

With a response timeout, blocking reads, such as a worker's wait for new
tasks, block for at most half of it, so a short response timeout makes them
poll more often.  A Lua script that runs longer than the response timeout
fails with a timeout, though Redis still finishes it.

On `redis+sentinel://`, the timeouts reach the master but not the Sentinels.
redis-rs asks the Sentinels for the master, and checks the master's role, on
connections of its own that give up after 1 second to connect and 500
milliseconds to answer, and docket-rs cannot change those.  Only TCP keepalive
reaches the Sentinels themselves.

`memory://`, behind the `memory` feature, runs an in-process Redis, so your
own tests need no server.  Each `memory://` URL keeps its data for the life of
the process, as in pydocket, so a docket opened again on a URL finds what the
last one left there:

```toml
[dev-dependencies]
docket-rs = { version = "0.2", features = ["memory"] }
```

An application that keeps keys of its own in the docket's Redis reaches them
with `Docket::redis`, which opens a redis-rs connection on any of these URLs,
`memory://` included.

A test can move a `memory://` docket's clock with `docket::testing::advance_time`,
or have idle workers skip ahead to the next scheduled task with
`docket::testing::skip_idle_time`, so that perpetual intervals and retry
delays take no real time.  Skipping assumes one worker per `memory://` URL,
because an idle worker moves the clock that busy workers share.

[`examples/testing.rs`](examples/testing.rs) is a small application with the
tests it writes for its tasks this way.

## Logs, metrics, and traces

docket-rs logs, counts, and traces the same things as pydocket, with the same
names, so one dashboard reads both.

- Logs go through `tracing`.  Each run logs when it starts and ends, inside a
  span with the docket, worker, task, key, and attempt.  A run's log lines
  show only the argument fields marked `#[task(logged)]`, or
  `#[task(logged(length_only))]` for a collection's length.
- Metrics and spans go through the `opentelemetry` API, and record nothing
  until you install a meter provider or a tracer provider.  A docket binds to
  the global providers when it connects, so install them first.
- Each message carries the trace context of the span that added it, and each
  run's span links back to that span.  Install a text map propagator, such as
  `TraceContextPropagator`, for the links.
- The `prometheus` feature serves the metrics in the Prometheus text format,
  the way pydocket's `--metrics-port` does: see `docket::prometheus`.

## Command line

docket-rs ships no worker binary.  The `cli` feature gives your binary the
worker options of pydocket's `docket worker`, as a clap struct to flatten into
your own command line: see `docket::cli::WorkerArgs`.  It includes
`--metrics-port` and `--healthcheck-port`.

## Working on docket-rs

Run the tests from `rust/`.  They use `memory://` unless `DOCKET_TEST_URL`
names another server, and `scripts/test-redis.sh` starts one in Docker:

```bash
cargo test --workspace --all-features
DOCKET_TEST_URL=$(scripts/test-redis.sh redis:8.10 cluster) cargo test --workspace --all-features
```

CI measures coverage on nightly across `memory://`, Redis, a cluster, and
Sentinel together, and requires 100% of lines, regions, and functions.
