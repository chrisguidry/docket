# Developer experience across languages

This document proposes how docket feels to use in Rust, Go, and TypeScript,
next to the Python API that exists today.  It is a draft for review.  The
goal is an API that users of each language find idiomatic, and that still
reads as one system across all four.

The Python code is the real API, and this work does not change it.  Every
Python snippet uses only the API that exists today.  The Rust snippets use
the API of docket-rs as built in `rust/`.  The Go and TypeScript code is a
proposal.  Imports are left out, the Go snippets skip
error checks, and names such as `Payments` and `cleanup` are placeholders.

The questions where review helps most are in [Open questions](#open-questions).

## The model

These rules hold in every language.

- **A task is a named handler with typed arguments.**  The name is a stable
  string, because queued tasks refer to it.  Renaming a type or a function
  must not strand the tasks that are already queued.
- **The argument type carries the task's name.**  In Rust, the argument
  type derives the `Task` trait with `#[task(name = "...")]`.  In Go, it has a `TaskName()` method.
  In TypeScript, a declared type map connects each name to its argument and
  result types.  A producer needs only these definitions.  It does not
  compile or import the handler.
- **Arguments and results are JSON** in Rust, Go, and TypeScript.  Python
  keeps cloudpickle.
- **The worker registers each handler with its behaviors.**  Behaviors are
  retries, timeouts, perpetual scheduling, cron, and admission control.
  They all run on the worker, so they belong at registration.  Python
  declares them in the function signature.
- **Handlers get their dependencies from closures or method values.**  A
  database pool or an API client is created by the host application and
  captured by the handler.  The compiler checks it.
- **The handler receives a context.**  The context gives the task's key,
  its attempt number, a logger, and the handles for its behaviors.
- **Behaviors plug into four hooks.**  An admission hook runs before the
  task starts.  A runtime hook wraps the call.  A failure hook decides what
  a failure means.  A completion hook runs after the task is done.  Users
  write their own behaviors against the same hooks.
- **docket is a library inside a host application.**  The host opens the
  docket, registers tasks, and runs the worker.
- **`memory://` works in every language,** so tests need no Redis server.

## A complete example

A `charge` task returns a receipt.  It retries with exponential backoff, and
it uses a payments client that the worker creates.  A `nightly-cleanup` task
takes no arguments and runs every 24 hours as an automatic perpetual task.

**Python**

```python
# Task definitions.  In Python, one function is both the name and the handler.
@dataclass
class Receipt:
    id: str

async def open_payments() -> Payments:  # Shared runs this once per worker
    return Payments()

async def charge(
    customer: int,
    cents: int,
    payments: Payments = Shared(open_payments),
    logger: LoggerAdapter[Logger] = TaskLogger(),
    retry: ExponentialRetry = ExponentialRetry(attempts=5, minimum_delay=timedelta(seconds=1)),
) -> Receipt:
    logger.info("charging customer %s, attempt %d", customer, retry.attempt)
    return Receipt(id=await payments.charge(customer, cents))

async def nightly_cleanup(
    perpetual: Perpetual = Perpetual(every=timedelta(hours=24), automatic=True),
) -> None:
    await cleanup()

# Worker.
async with Docket(name="orders", url="redis://localhost:6379/0") as docket:
    docket.register(charge)
    docket.register(nightly_cleanup)
    async with Worker(docket, concurrency=20) as worker:
        await worker.run_forever()  # cancelling this drains in-flight tasks

# Producer.
execution = await docket.add(charge, key="order-9")(7, 1999)
receipt = await execution.get_result()
```

**Rust**

```rust
// Task definitions, shared by producers and workers.
#[derive(Serialize, Deserialize)]
pub struct Receipt { pub id: String }

#[derive(Serialize, Deserialize, Task)]
#[task(name = "charge", output = Receipt)]
pub struct Charge { pub customer: u64, pub cents: u64 }

#[derive(Serialize, Deserialize, Default, Task)]
#[task(name = "nightly-cleanup")]
pub struct NightlyCleanup;

// Worker.
async fn charge(ctx: Context, args: Charge, payments: Payments) -> anyhow::Result<Receipt> {
    tracing::info!(customer = args.customer, attempt = ctx.attempt(), "charging");
    Ok(Receipt { id: payments.charge(args.customer, args.cents).await? })
}

let docket = Docket::connect("orders", "redis://localhost:6379/0").await?;
let payments = Payments::new();
docket
    .register(move |ctx, args: Charge| charge(ctx, args, payments.clone()))
    .with(ExponentialRetry::attempts(5).minimum_delay(Duration::from_secs(1)));
docket
    .register(|_ctx, _: NightlyCleanup| cleanup())
    .with(Perpetual::every(Duration::from_secs(24 * 60 * 60)).automatic());
Worker::new(docket.clone()).concurrency(20).run_until(docket::cli::shutdown_signal()).await?;

// Producer.
let execution = docket.add(Charge { customer: 7, cents: 1999 }).key("order-9").await?;
let receipt = execution.result().await?;
```

**Go**

```go
// Task definitions, shared by producers and workers.
type Receipt struct {
    ID string `json:"id"`
}

type Charge struct {
    Customer int64 `json:"customer"`
    Cents    int64 `json:"cents"`
}

func (Charge) TaskName() string { return "charge" }

type NightlyCleanup struct{}

func (NightlyCleanup) TaskName() string { return "nightly-cleanup" }

// Worker.
func charge(ctx context.Context, args Charge, payments *Payments) (Receipt, error) {
    docket.Logger(ctx).Info("charging", "customer", args.Customer, "attempt", docket.ExecutionFrom(ctx).Attempt)
    id, err := payments.Charge(ctx, args.Customer, args.Cents)
    return Receipt{ID: id}, err
}

func cleanup(ctx context.Context, _ NightlyCleanup) error { return runCleanup(ctx) }

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
d, err := docket.Open(ctx, "orders", "redis://localhost:6379/0")
payments := NewPayments()
docket.RegisterResult(d, func(ctx context.Context, args Charge) (Receipt, error) {
    return charge(ctx, args, payments)
}, docket.ExponentialRetry{Attempts: 5, MinimumDelay: time.Second})
docket.Register(d, cleanup, docket.Perpetual{Every: 24 * time.Hour, Automatic: true})
err = docket.NewWorker(d, docket.Concurrency(20)).Run(ctx) // returns after in-flight tasks drain

// Producer.
exec, err := d.Add(ctx, Charge{Customer: 7, Cents: 1999}, docket.Key("order-9"))
receipt, err := docket.Result[Receipt](ctx, exec)
```

**TypeScript**

```ts
// Task definitions, shared by producers and workers.
export interface Receipt { id: string }

declare module "@chrisguidry/docket" {
  interface Tasks {
    charge: { args: { customer: number; cents: number }; result: Receipt };
    "nightly-cleanup": { args: void; result: void };
  }
}

// Worker.
const docket = await Docket.open({ name: "orders", url: "redis://localhost:6379/0" });
const payments = new Payments();
docket.register("charge", async ({ customer, cents }, ctx) => {
  ctx.logger.info("charging", { customer, attempt: ctx.attempt });
  return { id: await payments.charge(customer, cents) };
}, { behaviors: [exponentialRetry({ attempts: 5, minimumDelay: 1_000 })] });
docket.register("nightly-cleanup", () => cleanup(), {
  behaviors: [perpetual({ every: 24 * 60 * 60 * 1_000, automatic: true })],
});
const shutdown = new AbortController();
process.once("SIGTERM", () => shutdown.abort());
await new Worker(docket, { concurrency: 20 }).run({ signal: shutdown.signal });

// Producer.
const execution = await docket.add("charge", { customer: 7, cents: 1999 }, { key: "order-9" });
const receipt = await execution.result(); // typed as Receipt
```

In Go, the closure repeats the handler's signature, because Go does not infer
the types of a function literal.  When a task needs several dependencies, the
usual Go shape is a struct that holds them, registered by method value:
`docket.RegisterResult(d, app.Charge)`.

## Opening a docket

A docket is a name and a Redis URL.  The name prefixes every key, so several
dockets can share one Redis.

**Python**

```python
async with Docket(name="orders", url="redis://localhost:6379/0") as docket:
    ...
```

**Rust**

```rust
let docket = Docket::connect("orders", "redis://localhost:6379/0").await?;
```

**Go**

```go
d, err := docket.Open(ctx, "orders", "redis://localhost:6379/0")
defer d.Close()
```

**TypeScript**

```ts
await using docket = await Docket.open({ name: "orders", url: "redis://localhost:6379/0" });
```

Every language accepts the same URL forms: `redis://`, `rediss://`,
`unix://`, the cluster and sentinel forms, and `memory://`.

## Scheduling

A task runs now, or at a time.  A key makes the call idempotent: adding a
second task with the same key does nothing, and `replace` swaps it.  Batches
go to Redis in one round trip.

**Python**

```python
await docket.add(charge)(7, 1999)                                    # now
await docket.add(charge, when=tomorrow)(7, 1999)                     # later
await docket.add(charge, key="order-9")(7, 1999)                     # with a key
await docket.replace(charge, when=tomorrow, key="order-9")(7, 2499)  # replace
await docket.add_many(docket.call(charge)(o.customer, o.cents) for o in orders)
```

**Rust**

```rust
docket.add(Charge { customer: 7, cents: 1999 }).await?;
docket.add(Charge { customer: 7, cents: 1999 }).at(tomorrow).await?;
docket.add(Charge { customer: 7, cents: 1999 }).key("order-9").await?;
docket.replace(Charge { customer: 7, cents: 2499 }, "order-9", tomorrow).await?;
docket.add_many(orders.iter().map(|o| docket.call(Charge { customer: o.customer, cents: o.cents }))).await?;
```

**Go**

```go
d.Add(ctx, Charge{Customer: 7, Cents: 1999})
d.Add(ctx, Charge{Customer: 7, Cents: 1999}, docket.At(tomorrow))
d.Add(ctx, Charge{Customer: 7, Cents: 1999}, docket.Key("order-9"))
d.Replace(ctx, Charge{Customer: 7, Cents: 2499}, "order-9", tomorrow)
d.AddMany(ctx, docket.Call(Charge{Customer: 7, Cents: 1999}), docket.Call(Charge{Customer: 8, Cents: 500}))
```

**TypeScript**

```ts
await docket.add("charge", { customer: 7, cents: 1999 });
await docket.add("charge", { customer: 7, cents: 1999 }, { when: tomorrow });
await docket.add("charge", { customer: 7, cents: 1999 }, { key: "order-9" });
await docket.replace("charge", { customer: 7, cents: 2499 }, { key: "order-9", when: tomorrow });
await docket.addMany(orders.map((o) => docket.call("charge", { customer: o.customer, cents: o.cents })));
```

`replace` requires both a key and a time, as in Python.  Rust and Go take
them as positional arguments, so the compiler enforces them.  Every `add`
returns an execution whose disposition is scheduled, already scheduled, or
struck.

## Cancelling

A cancel removes a scheduled task, or stops a running one.

**Python**

```python
await docket.cancel("order-9")
```

**Rust**

```rust
docket.cancel("order-9").await?;
```

**Go**

```go
d.Cancel(ctx, "order-9")
```

**TypeScript**

```ts
await docket.cancel("order-9");
```

A running task learns about the cancel in its language's way.  See
[How cancellation reaches a running task](#how-cancellation-reaches-a-running-task).

## Strikes

A strike blocks a task, or only the calls whose arguments match a
condition.  It applies when a task is added and again before it runs.
`restore` removes the strike.

**Python**

```python
await docket.strike(charge)                         # every call
await docket.strike(charge, "customer", "==", 7)    # one customer
await docket.strike(None, "customer", ">=", 90000)  # any task with that argument
await docket.restore(charge, "customer", "==", 7)
```

**Rust**

```rust
docket.strike(Strike::task::<Charge>()).await?;
docket.strike(Strike::task::<Charge>().field("customer").eq(7)).await?;
docket.strike(Strike::any_task().field("customer").ge(90_000)).await?;
docket.restore(Strike::task::<Charge>().field("customer").eq(7)).await?;
```

**Go**

```go
d.Strike(ctx, docket.Strike{Task: "charge"})
d.Strike(ctx, docket.Strike{Task: "charge", Field: "customer", Op: docket.Equal, Value: 7})
d.Strike(ctx, docket.Strike{Field: "customer", Op: docket.AtLeast, Value: 90000})
d.Restore(ctx, docket.Strike{Task: "charge", Field: "customer", Op: docket.Equal, Value: 7})
```

**TypeScript**

```ts
await docket.strike({ task: "charge" });
await docket.strike({ task: "charge", field: "customer", op: "==", value: 7 });
await docket.strike({ field: "customer", op: ">=", value: 90000 });
await docket.restore({ task: "charge", field: "customer", op: "==", value: 7 });
```

A Python strike names a parameter of the function.  In the other languages
it names a field of the JSON arguments.  The operators are the same
everywhere: `==`, `!=`, `>`, `>=`, `<`, `<=`, and `between`.

## Retries

A task fails when its handler raises, returns an error, or rejects.  Its
retry behavior decides whether it runs again.  The attempt count includes the
first run.  A task can also ask for a retry at a time it chooses.

**Python**

```python
async def charge(
    customer: int,
    cents: int,
    retry: ExponentialRetry = ExponentialRetry(
        attempts=5, minimum_delay=timedelta(seconds=1), maximum_delay=timedelta(minutes=1)
    ),
) -> Receipt:
    if await payments_are_down():
        retry.after(timedelta(minutes=5))  # spends an attempt
    ...
```

**Rust**

```rust
async fn charge(ctx: Context, args: Charge) -> anyhow::Result<Receipt> {
    if payments_are_down().await {
        return Err(ForcedRetry::after(Duration::from_secs(5 * 60)).into()); // spends an attempt
    }
    ...
}

docket.register(charge).with(
    ExponentialRetry::attempts(5)
        .minimum_delay(Duration::from_secs(1))
        .maximum_delay(Duration::from_secs(60)),
);
```

**Go**

```go
func charge(ctx context.Context, args Charge) (Receipt, error) {
    if paymentsAreDown(ctx) {
        return Receipt{}, docket.RetryAfter(5 * time.Minute) // spends an attempt
    }
    ...
}

docket.RegisterResult(d, charge,
    docket.ExponentialRetry{Attempts: 5, MinimumDelay: time.Second, MaximumDelay: time.Minute})
```

**TypeScript**

```ts
docket.register("charge", async (args, ctx) => {
  if (await paymentsAreDown()) {
    ctx.retry.after(5 * 60_000); // throws, and spends an attempt
  }
  ...
}, {
  behaviors: [exponentialRetry({ attempts: 5, minimumDelay: 1_000, maximumDelay: 60_000 })],
});
```

The fixed-delay form is `Retry`, with `attempts` and `delay`.  Retrying
forever is `Retry.forever()` in Python, `Retry::forever()` in Rust,
`docket.Retry{Forever: true}` in Go, and `attempts: Infinity` in TypeScript.

## Timeouts

A timeout cancels a task that runs too long, and the task fails with a
timeout error that its retry behavior can retry.  A task can extend its own
deadline.

**Python**

```python
async def rebuild_index(timeout: Timeout = Timeout(timedelta(seconds=30))) -> None:
    await phase_one()
    timeout.extend(timedelta(seconds=30))
    await phase_two()
```

**Rust**

```rust
async fn rebuild_index(ctx: Context, _: RebuildIndex) -> anyhow::Result<()> {
    phase_one().await?;
    if let Some(timeout) = ctx.timeout() {
        timeout.extend(Duration::from_secs(30));
    }
    phase_two().await
}

docket.register(rebuild_index).with(Timeout::after(Duration::from_secs(30)));
```

**Go**

```go
func rebuildIndex(ctx context.Context, _ RebuildIndex) error {
    if err := phaseOne(ctx); err != nil {
        return err
    }
    docket.TimeoutFrom(ctx).Extend(30 * time.Second)
    return phaseTwo(ctx)
}

docket.Register(d, rebuildIndex, docket.Timeout{After: 30 * time.Second})
```

**TypeScript**

```ts
docket.register("rebuild-index", async (_, ctx) => {
  await phaseOne(ctx.signal);
  ctx.timeout.extend(30_000);
  await phaseTwo(ctx.signal);
}, { behaviors: [timeout({ after: 30_000 })] });
```

## Perpetual and cron tasks

A perpetual task schedules its next run when it finishes.  The task can stop
itself or change the delay to its next run.  An automatic perpetual task
starts when a worker starts, and takes no arguments.  A cron task is a
perpetual task on a cron schedule, and it is automatic by default.

**Python**

```python
async def watch_deploy(
    deploy_id: str,
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=30)),
) -> None:
    status = await check_status(deploy_id)
    if status == "done":
        perpetual.cancel()
    elif status == "stuck":
        perpetual.after(timedelta(minutes=5))

async def standup_reminder(
    cron: Cron = Cron("0 9 * * 1-5", tz=ZoneInfo("America/Los_Angeles")),
) -> None: ...
```

**Rust**

```rust
async fn watch_deploy(ctx: Context, args: WatchDeploy) -> anyhow::Result<()> {
    let next = ctx.perpetual().expect("the task is perpetual");
    match check_status(&args.deploy_id).await? {
        Status::Done => next.cancel(),
        Status::Stuck => next.after(Duration::from_secs(5 * 60)),
        _ => {}
    }
    Ok(())
}

docket.register(watch_deploy).with(Perpetual::every(Duration::from_secs(30)));
docket.register(standup_reminder).with(Cron::new("0 9 * * 1-5")?.timezone(chrono_tz::America::Los_Angeles));
```

**Go**

```go
func watchDeploy(ctx context.Context, args WatchDeploy) error {
    switch checkStatus(ctx, args.DeployID) {
    case StatusDone:
        docket.PerpetualFrom(ctx).Cancel()
    case StatusStuck:
        docket.PerpetualFrom(ctx).After(5 * time.Minute)
    }
    return nil
}

docket.Register(d, watchDeploy, docket.Perpetual{Every: 30 * time.Second})
la, _ := time.LoadLocation("America/Los_Angeles")
docket.Register(d, standupReminder, docket.Cron{Expression: "0 9 * * 1-5", Location: la})
```

**TypeScript**

```ts
docket.register("watch-deploy", async ({ deployId }, ctx) => {
  const status = await checkStatus(deployId);
  if (status === "done") ctx.perpetual.cancel();
  else if (status === "stuck") ctx.perpetual.after(5 * 60_000);
}, { behaviors: [perpetual({ every: 30_000 })] });

docket.register("standup-reminder", () => remind(), {
  behaviors: [cron({ expression: "0 9 * * 1-5", timezone: "America/Los_Angeles" })],
});
```

An automatic task needs arguments with a default.  Rust enforces this at
compile time: `Perpetual::automatic()` and `Cron` (automatic unless
`.manual()`) attach only to tasks whose arguments implement `Default`.  In Go, the default is the
zero value.  In TypeScript, the arguments are `void`.

## Admission control

Admission behaviors decide whether a task may start now.  A concurrency limit
caps how many run at once, for the whole task or for each value of one
argument.  A cooldown and a debounce drop calls that come too close together.
A rate limit caps runs per period.

**Python**

```python
async def charge(customer: Annotated[int, ConcurrencyLimit(1)], cents: int) -> Receipt: ...
async def render_report(limit: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=3)) -> None: ...
async def refresh_feed(feed: Annotated[int, Cooldown(timedelta(seconds=30))]) -> None: ...
async def reindex(debounce: Debounce = Debounce(timedelta(seconds=5))) -> None: ...
async def call_api(rate: RateLimit = RateLimit(10, per=timedelta(minutes=1))) -> None: ...
```

**Rust**

```rust
docket.register(charge).with(ConcurrencyLimit::per_field("customer", 1));
docket.register(render_report).with(ConcurrencyLimit::new(3));
docket.register(refresh_feed).with(Cooldown::per_field("feed", Duration::from_secs(30)));
docket.register(reindex).with(Debounce::new(Duration::from_secs(5)));
docket.register(call_api).with(RateLimit::new(10).per(Duration::from_secs(60)));
```

**Go**

```go
docket.RegisterResult(d, charge, docket.ConcurrencyLimit{Field: "customer", Max: 1})
docket.Register(d, renderReport, docket.ConcurrencyLimit{Max: 3})
docket.Register(d, refreshFeed, docket.Cooldown{Field: "feed", Window: 30 * time.Second})
docket.Register(d, reindex, docket.Debounce{Settle: 5 * time.Second})
docket.Register(d, callAPI, docket.RateLimit{Limit: 10, Per: time.Minute})
```

**TypeScript**

```ts
docket.register("charge", charge, { behaviors: [concurrencyLimit({ field: "customer", max: 1 })] });
docket.register("render-report", renderReport, { behaviors: [concurrencyLimit({ max: 3 })] });
docket.register("refresh-feed", refreshFeed, { behaviors: [cooldown({ field: "feed", window: 30_000 })] });
docket.register("reindex", reindex, { behaviors: [debounce({ settle: 5_000 })] });
docket.register("call-api", callApi, { behaviors: [rateLimit({ limit: 10, per: 60_000 })] });
```

In TypeScript, the type map lets `field` be typed as a key of the task's
arguments, so a misspelled field fails to compile.  In Go, registration can
check the name against the struct's JSON tags.  In Rust, a misspelled field
is found only when a task runs, which is one reason for open question 2.

## The context and dependencies

Python injects context values and dependencies through parameter defaults.
The other languages pass a context to the handler, and they take
dependencies from closures.  This table maps each Python injectable.

| Python | Rust | Go | TypeScript |
|---|---|---|---|
| `TaskKey()` | `ctx.key()` | `docket.ExecutionFrom(ctx).Key` | `ctx.key` |
| `CurrentExecution()` | `ctx.key()`, `ctx.function()`, `ctx.when()`, `ctx.args()` | `docket.ExecutionFrom(ctx)` | `ctx.execution` |
| `CurrentDocket()` | `ctx.docket()` | `docket.From(ctx)` | `ctx.docket` |
| `CurrentWorker()` | `ctx.worker()` | `docket.WorkerFrom(ctx)` | `ctx.worker` |
| `TaskLogger()` | the task's `tracing` span | `docket.Logger(ctx)` | `ctx.logger` |
| `retry.attempt` | `ctx.attempt()` | `docket.ExecutionFrom(ctx).Attempt` | `ctx.attempt` |
| `Shared(factory)` | a value captured by the closure | a value captured by the closure | a value captured by the closure |
| `Depends(fn)` | a call inside the handler | a call inside the handler | a call inside the handler |

A `Shared` resource is created once per worker and closed when the worker
exits.  In the other languages, the host application does both: it creates
the resource before it starts the worker, and it closes the resource after
the worker returns.  A `Depends` function with setup and cleanup becomes
code in the handler: a value with `Drop` in Rust, `defer` in Go, and
`try`/`finally` or `await using` in TypeScript.

In Rust, docket runs each task inside a `tracing` span that has the task
name, key, and attempt.  So a plain `tracing::info!` call in the handler
logs with those fields, the same way `TaskLogger` does in Python.

## Results

A caller can wait for a task's result.  If the task failed, the wait fails.
If the task was cancelled, the wait fails with a cancellation error.

**Python**

```python
execution = await docket.add(charge)(7, 1999)
receipt = await execution.get_result(timeout=timedelta(seconds=30))
```

**Rust**

```rust
let execution = docket.add(Charge { customer: 7, cents: 1999 }).await?;
let receipt = tokio::time::timeout(Duration::from_secs(30), execution.result()).await??;
```

**Go**

```go
exec, err := d.Add(ctx, Charge{Customer: 7, Cents: 1999})
waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
receipt, err := docket.Result[Receipt](waitCtx, exec)
```

**TypeScript**

```ts
const execution = await docket.add("charge", { customer: 7, cents: 1999 });
const receipt = await execution.result({ signal: AbortSignal.timeout(30_000) });
```

Each language bounds the wait with its own tool: a Tokio timeout, a context
deadline, or an `AbortSignal`.  Python raises the task's original exception.
The other languages return an error that has the failed task's error type and
message.

## Progress

A task can report progress, and a caller can read it or follow it live.

**Python**

```python
async def import_rows(rows: int, progress: Progress = Progress()) -> None:
    await progress.set_total(rows)
    for _ in range(rows):
        await progress.increment()
    await progress.set_message("done")

async for event in execution.subscribe():
    print(event)
```

**Rust**

```rust
async fn import_rows(ctx: Context, args: ImportRows) -> anyhow::Result<()> {
    let progress = ctx.progress();
    progress.set_total(args.rows).await?;
    for _ in 0..args.rows {
        progress.increment(1).await?;
    }
    progress.set_message(Some("done")).await?;
    Ok(())
}

let mut events = execution.subscribe().await?;
while let Some(event) = events.next().await {
    println!("{:?}", event?);
}
```

**Go**

```go
func importRows(ctx context.Context, args ImportRows) error {
    progress := docket.ProgressFrom(ctx)
    progress.SetTotal(ctx, args.Rows)
    for range args.Rows {
        progress.Increment(ctx, 1)
    }
    return progress.SetMessage(ctx, "done")
}

events, err := exec.Subscribe(ctx) // closed when the task ends or ctx is done
for event := range events {
    fmt.Println(event)
}
```

**TypeScript**

```ts
docket.register("import-rows", async ({ rows }, ctx) => {
  await ctx.progress.setTotal(rows);
  for (let i = 0; i < rows; i++) await ctx.progress.increment();
  await ctx.progress.setMessage("done");
});

for await (const event of execution.subscribe()) {
  console.log(event);
}
```

## Testing

With `memory://`, a test needs no Redis server.  `run_until_finished` runs
the worker until nothing is left to run.  Because the handler is registered
separately from the task definition, a test in Rust, Go, or TypeScript can
register a fake handler for a real task.

**Python**

```python
async with Docket(name="test", url="memory://test") as docket:
    docket.register(charge)
    await docket.add(charge, key="order-9")(7, 1999)
    await testing.assert_task_scheduled(docket, charge, key="order-9")
    async with Worker(docket) as worker:
        await worker.run_until_finished()
    await testing.assert_no_tasks(docket)
```

**Rust**

```rust
#[tokio::test]
async fn charges_the_customer() -> anyhow::Result<()> {
    let docket = Docket::connect("test", "memory://").await?;
    docket.register(|_ctx, args: Charge| async move { Ok(Receipt { id: format!("r-{}", args.customer) }) });
    docket.add(Charge { customer: 7, cents: 1999 }).key("order-9").await?;
    testing::assert_task_scheduled(&docket, Charge::NAME, "order-9").await;
    Worker::new(docket.clone()).run_until_finished().await?;
    testing::assert_no_tasks(&docket).await;
    Ok(())
}
```

**Go**

```go
func TestCharge(t *testing.T) {
    ctx := t.Context()
    d, _ := docket.Open(ctx, "test", "memory://")
    docket.RegisterResult(d, func(ctx context.Context, args Charge) (Receipt, error) {
        return Receipt{ID: fmt.Sprintf("r-%d", args.Customer)}, nil
    })
    d.Add(ctx, Charge{Customer: 7, Cents: 1999}, docket.Key("order-9"))
    dockettest.AssertTaskScheduled(t, d, "charge", "order-9")
    docket.NewWorker(d).RunUntilFinished(ctx)
    dockettest.AssertNoTasks(t, d)
}
```

**TypeScript**

```ts
test("charges the customer", async () => {
  await using docket = await Docket.open({ name: "test", url: "memory://" });
  docket.register("charge", async ({ customer }) => ({ id: `r-${customer}` }));
  await docket.add("charge", { customer: 7, cents: 1999 }, { key: "order-9" });
  await expectTaskScheduled(docket, "charge", { key: "order-9" });
  await new Worker(docket).runUntilFinished();
  await expectNoTasks(docket);
});
```

## Custom behaviors

A custom behavior implements one or more of the four hooks.  This example
is an admission hook that lets a task start only during business hours.  A
blocked task goes back on the queue and tries again in 15 minutes.

**Python**

```python
class BusinessHoursOnly(Dependency):
    async def __aenter__(self) -> None:
        if not 9 <= datetime.now().hour < 17:
            raise AdmissionBlocked(
                current_execution.get(), reason="after hours", retry_delay=timedelta(minutes=15)
            )

async def payroll(gate: None = BusinessHoursOnly()) -> None: ...
```

**Rust**

```rust
struct BusinessHoursOnly;

impl Admission for BusinessHoursOnly {
    async fn admit(&self, _ctx: &Context) -> Result<Admitted, NotAdmitted> {
        match Local::now().hour() {
            9..17 => Ok(Admitted::now()),
            _ => Err(AdmissionBlocked::new("after hours")
                .retry_delay(Duration::from_secs(15 * 60))
                .into()),
        }
    }
}

impl<T: Task> Behavior<T> for BusinessHoursOnly {
    fn attach(self, hooks: &mut Hooks<'_, T>) { hooks.admission(self); }
}

docket.register(payroll).with(BusinessHoursOnly);
```

**Go**

```go
type BusinessHoursOnly struct{}

func (BusinessHoursOnly) Hooks() docket.Hooks {
    return docket.Hooks{Admit: func(ctx context.Context) error {
        if hour := time.Now().Hour(); hour < 9 || hour >= 17 {
            return &docket.AdmissionBlocked{Reason: "after hours", RetryDelay: 15 * time.Minute}
        }
        return nil
    }}
}

docket.Register(d, payroll, BusinessHoursOnly{})
```

**TypeScript**

```ts
const businessHoursOnly: Behavior = {
  async admit() {
    const hour = new Date().getHours();
    if (hour < 9 || hour >= 17) {
      throw new AdmissionBlocked({ reason: "after hours", retryDelay: 15 * 60_000 });
    }
  },
};

docket.register("payroll", payroll, { behaviors: [businessHoursOnly] });
```

Each behavior states which hooks it uses.  In Rust, `attach` puts the
behavior into its hooks, and `Behavior` is generic over the task, so a
behavior can require more of the task's arguments.  An admission hook
returns `Admitted::with_release(...)` when it holds something, such as a
concurrency slot, that must be given back after the task.  It refuses a
task with `NotAdmitted`: `Blocked` holds the task back, the way Python's
`AdmissionBlocked` does, and `Failed` fails the task when the check cannot
run, the way any other error from a Python dependency does.  A failure hook
returns `AfterFailure::RetryAt(when)` or `Fail`, and a completion hook
returns `AfterCompletion`; docket, not the behavior, calls the Lua scripts.  In Go, `Hooks()` returns a struct of hook
functions.  In TypeScript, the behavior is an object with optional hook
methods.  A behavior can use several hooks: a concurrency limit admits a
task and also releases its slot when the task completes.  The runtime,
failure, and completion hooks allow one behavior per task, as in Python.

## Differences that come from the languages

### How cancellation reaches a running task

A timeout and a cancel both stop a running task, and the languages deliver
that stop in different ways.

- **Python** raises `CancelledError` at the task's next `await`.
- **Rust** drops the task's future, so the task stops at its next `.await`.
- **Go** closes `ctx.Done()`.  The task must check its context, or pass it
  to calls that do.
- **TypeScript** aborts `ctx.signal`.  The task must check the signal, or
  pass it to calls that do, such as `fetch`.

In Go and TypeScript, a task that ignores its context or signal keeps
running after docket records the timeout or cancel.

### Arguments

A Python task takes positional and keyword arguments.  A task in the other
languages takes one value, a struct or an object.  Python strikes and
admission limits name a parameter, and the other languages name a field of
that value.

### Results in Go

Go does not allow a generic method, so the result type of `docket.Result`
is written at the call site.  The compiler does not check it against the
handler.  Rust and TypeScript check it.  For the same reason, Go has two
registration functions: `Register` for a handler that returns only an
error, and `RegisterResult` for a handler that also returns a value.

### Durations

Rust and Go use their standard duration types.  TypeScript uses
milliseconds, the same unit as `setTimeout`.

## Open questions

1. **Go registration.**  Is `Register` plus `RegisterResult` the right
   shape, or is one function with a result type of `struct{}` better?
2. **Fields by name or by function.**  Strikes and admission limits name a
   JSON field as a string.  Rust and Go could take a typed function
   instead, such as `ConcurrencyLimit::per(|c: &Charge| c.customer, 1)`.
   That catches mistakes at compile time, but a strike from an admin tool
   still needs a name.  Decided for Rust: JSON field names, the same as
   strikes.
3. **Behavior handles.**  The context has one accessor for each built-in
   behavior, such as `ctx.perpetual()` and `ctx.timeout()`.  A custom
   behavior needs a general lookup, such as `ctx.behavior::<MyBehavior>()`.
   What should a lookup do when the task does not have that behavior?
   Decided for Rust: every accessor returns an `Option`, and
   `ctx.behavior::<T>()` returns `None` when the task lacks the behavior.
4. **Rust task definitions.**  A hand-written `impl Task` is three lines.  A
   `#[derive(Task)]` is shorter, but a derive macro must ship as a second
   crate on crates.io.  Decided: `#[derive(Task)]` in docket-rs-macros,
   re-exported by docket-rs the way serde re-exports its derives, with
   `name` required so that renaming a type never strands queued tasks.
5. **TypeScript durations.**  Milliseconds, strings such as `"5m"`, or
   `Temporal.Duration`?
6. **TypeScript runtimes.**  Is Node enough, or do Bun and Deno need
   support?
7. **A CLI helper for workers.**  Should Rust and Go offer a helper that
   gives a user's binary docket's standard worker flags and signal handling?
   Decided for Rust: `docket::cli::WorkerArgs` behind the `cli` feature, a
   clap struct to flatten, with pydocket's option names and environment
   variables, and `docket::cli::shutdown_signal()`.
8. **Words.**  This document keeps docket's words, such as `add`, `replace`,
   and `strike`, in every language.  Some libraries say `enqueue`.  Keeping
   docket's words makes the four APIs easier to compare.
