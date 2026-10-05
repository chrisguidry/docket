# docket-rs

docket-rs runs functions on other machines, now or later, with Redis keeping
the queue.  It is the Rust implementation of
[docket](https://github.com/chrisguidry/docket): the same concepts, the same
task behaviors, and the same Redis data model as pydocket, in Rust's idioms.

docket-rs is a library.  Your application opens a docket, registers its
tasks, and runs a worker.

```toml
[dependencies]
docket-rs = "0.1"
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

`memory://`, behind the `memory` feature, runs an in-process Redis, so your
own tests need no server:

```toml
[dev-dependencies]
docket-rs = { version = "0.1", features = ["memory"] }
```

## Command line

docket-rs ships no worker binary.  The `cli` feature gives your binary the
worker options of pydocket's `docket worker`, as a clap struct to flatten into
your own command line: see `docket::cli::WorkerArgs`.

## Working on docket-rs

Run the tests from `rust/`.  They use `memory://` unless `DOCKET_TEST_URL`
names another server, and `scripts/test-redis.sh` starts one in Docker:

```bash
cargo test --workspace --all-features
DOCKET_TEST_URL=$(scripts/test-redis.sh redis:8.10 cluster) cargo test --workspace --all-features
```

CI measures coverage on nightly across `memory://`, Redis, a cluster, and
Sentinel together, and requires 100% of lines, regions, and functions.
