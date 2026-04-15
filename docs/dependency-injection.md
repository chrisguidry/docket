# Dependency Injection

Docket includes a dependency injection system that provides access to context, configuration, and custom resources. It's similar to FastAPI's dependency injection but tailored for background task patterns.

As of version 0.18.0, Docket's dependency injection is built on the
[`uncalled-for`](https://github.com/chrisguidry/uncalled-for) package
([PyPI](https://pypi.org/project/uncalled-for/)), which provides the core
resolution engine, `Depends`, `Shared`, and `Dependency` base class.  Docket
layers on task-specific context (`CurrentDocket`, `CurrentWorker`, etc.) and
behavioral dependencies (`Retry`, `Perpetual`, `Timeout`, etc.).

## Contextual Dependencies

### Accessing the Current Docket

Tasks often need to schedule more work. The `CurrentDocket` dependency gives you access to the same docket the worker is processing:

```python
from pathlib import Path
from datetime import datetime, timedelta, timezone
from docket import Docket, CurrentDocket

def now() -> datetime:
    return datetime.now(timezone.utc)

async def poll_for_file(
    file_path: str,
    docket: Docket = CurrentDocket()
) -> None:
    path = Path(file_path)
    if path.exists():
        print(f"File {file_path} found!")
        return

    # Schedule another check in 30 seconds
    await docket.add(
        poll_for_file,
        when=now() + timedelta(seconds=30)
    )(file_path)
```

This is especially useful for self-perpetuating tasks that create chains of future work.

### Getting Your Task Key

Use `TaskKey` to access the current task's key, which is helpful for creating related work or maintaining task chains:

```python
from docket import CurrentDocket, TaskKey

async def process_data_chunk(
    dataset_id: int,
    chunk: int,
    total_chunks: int,
    key: str = TaskKey(),
    docket: Docket = CurrentDocket()
) -> None:
    print(f"Processing chunk {chunk}/{total_chunks} for dataset {dataset_id}")

    # Process this chunk...
    await process_chunk_data(dataset_id, chunk)

    if chunk < total_chunks:
        # Schedule next chunk with a related key
        next_key = f"dataset-{dataset_id}-chunk-{chunk + 1}"
        await docket.add(
            process_data_chunk,
            key=next_key
        )(dataset_id, chunk + 1, total_chunks)
```

### Worker and Execution Context

Access the current worker and execution details when needed:

```python
from docket import CurrentWorker, CurrentExecution, Worker, Execution

async def diagnostic_task(
    worker: Worker = CurrentWorker(),
    execution: Execution = CurrentExecution()
) -> None:
    print(f"Running on worker: {worker.name}")
    print(f"Task key: {execution.key}")
    print(f"Scheduled at: {execution.when}")
    print(f"Worker concurrency: {worker.concurrency}")
```

### TaskArgument

Dependencies can access the task's input arguments using `TaskArgument`. This lets a dependency function pull values from the task's call site without the task having to pass them explicitly:

```python
from docket import TaskArgument

async def get_user_context(user_id: int = TaskArgument()) -> dict:
    """Dependency that fetches user context based on task argument."""
    user = await fetch_user(user_id)
    return {
        'user': user,
        'permissions': await fetch_user_permissions(user_id),
        'preferences': await fetch_user_preferences(user_id)
    }

async def send_personalized_email(
    user_id: int,
    message: str,
    user_context=Depends(get_user_context)
) -> None:
    # user_context is populated based on the user_id argument
    email = personalize_email(message, user_context['preferences'])
    await send_email(user_context['user'].email, email)
```

You can access arguments by name or make them optional:

```python
async def get_optional_config(
    config_name: str | None = TaskArgument("config", optional=True)
) -> dict:
    """Get configuration if provided, otherwise use defaults."""
    if config_name:
        return await load_config(config_name)
    return DEFAULT_CONFIG

async def flexible_task(
    data: dict,
    config: str | None = None,  # Optional argument
    resolved_config=Depends(get_optional_config)
) -> None:
    # resolved_config will be loaded config or defaults
    await process_data(data, resolved_config)
```

## Using Functions as Dependencies

### Depends

`Depends()` wraps any callable (sync or async, plain or context manager) as a dependency that's resolved fresh for each task execution.

#### Async Dependencies

```python
from contextlib import asynccontextmanager
from docket import Depends

@asynccontextmanager
async def get_database_connection():
    """Async dependency that returns a database connection."""
    conn = await database.connect()
    try:
        yield conn
    finally:
        await conn.close()

async def process_user_data(
    user_id: int,
    db=Depends(get_database_connection)
) -> None:
    # Database connection is automatically provided and cleaned up
    user = await db.fetch_user(user_id)
    await db.update_user(user_id, {"last_seen": datetime.now()})
```

#### Sync Dependencies

Use sync dependencies for pure computations and in-memory operations. Synchronous dependencies should **not** include blocking I/O (file access, network calls, database queries) as that blocks the event loop and prevents other tasks from running. Use async dependencies for any I/O.

```python
from docket import Depends

# In-memory config lookup - no I/O
def get_config() -> dict:
    """Access configuration from memory."""
    return {"api_url": "https://api.example.com", "timeout": 30}

# Pure computation - no I/O
def build_request_headers(config: dict = Depends(get_config)) -> dict:
    """Construct headers from config."""
    return {
        "User-Agent": "MyApp/1.0",
        "Timeout": str(config["timeout"])
    }

async def call_api(
    headers: dict = Depends(build_request_headers)
) -> None:
    # Headers are computed without blocking
    # Network I/O happens here (async)
    response = await http_client.get(url, headers=headers)
```

#### Context Managers

Dependencies that are async context managers get automatic cleanup:

```python
@asynccontextmanager
async def get_database_connection():
    conn = await database.connect()
    try:
        yield conn
    finally:
        await conn.close()
```

The connection is created before your task runs and closed after it finishes, even if the task raises an exception.

#### Nesting and Caching

Dependency functions can themselves declare dependencies as parameters — including other `Depends()` values and built-in context like `CurrentExecution()`. Docket resolves the full graph in the right order:

```python
async def get_auth_service(db=Depends(get_database_connection)):
    """A service that depends on the database connection."""
    return AuthService(db)

async def get_user_service(
    db=Depends(get_database_connection),
    auth=Depends(get_auth_service)
):
    """A service that depends on both database and auth service."""
    return UserService(db, auth)

async def update_user_profile(
    user_id: int,
    profile_data: dict,
    user_service=Depends(get_user_service)
) -> None:
    # All dependencies are resolved automatically:
    # db -> auth_service -> user_service -> this task
    await user_service.update_profile(user_id, profile_data)
```

Dependencies are resolved once per task execution and cached, so if multiple parameters depend on the same resource, only one instance is created.

```python
async def get_task_logger(
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker()
) -> LoggerAdapter:
    logger = logging.getLogger(f"worker.{worker.name}")
    return LoggerAdapter(logger, {
        'task_key': execution.key,
        'worker_name': worker.name
    })

async def important_task(
    data: dict,
    logger=Depends(get_task_logger)
) -> None:
    logger.info("Starting important task")
    await process_important_data(data)
    logger.info("Important task completed")
```

### Shared

While `Depends` resolves a fresh instance for each task, `Shared` resolves once at worker startup and provides the same instance to all tasks for the worker's lifetime. This is useful for expensive resources like connection pools, loaded configuration, or shared clients.

#### Async Context Manager (with cleanup)

Use an async context manager when the resource needs cleanup at worker shutdown:

```python
from contextlib import asynccontextmanager
from docket import Shared

@asynccontextmanager
async def create_db_pool():
    pool = await AsyncConnectionPool.create(conninfo="postgresql://...")
    try:
        yield pool
    finally:
        await pool.close()

async def query_users(
    pool: AsyncConnectionPool = Shared(create_db_pool)
) -> None:
    async with pool.connection() as conn:
        await conn.execute("SELECT ...")
```

The pool is created once on first use and closed when the worker shuts down.

#### Simple Async Function

For shared values that don't need cleanup, a plain async function works:

```python
from docket import Shared

async def load_config() -> Config:
    return await fetch_config_from_remote()

async def process_order(
    config: Config = Shared(load_config)
) -> None:
    # Same config instance across all tasks on this worker
    print(config.api_url)
```

#### Identity

Shared dependencies are keyed by the factory function itself. Multiple `Shared(same_factory)` calls anywhere in the codebase resolve to the same cached value:

```python
async def task_a(pool: AsyncConnectionPool = Shared(create_db_pool)) -> None:
    ...

async def task_b(pool: AsyncConnectionPool = Shared(create_db_pool)) -> None:
    # Same pool instance as task_a
    ...
```

### Error Handling

When dependencies fail, the entire task fails with detailed error information:

```python
async def unreliable_dependency():
    if random.random() < 0.5:
        raise ValueError("Service unavailable")
    return "success"

async def dependent_task(
    value=Depends(unreliable_dependency)
) -> None:
    print(f"Got value: {value}")
```

If `unreliable_dependency` fails, the task won't execute and the error will be logged with context about which dependency failed. This prevents tasks from running with incomplete or invalid dependencies.

## Subclassing Dependency

For full control, subclass `Dependency` directly. This is how all of Docket's built-in dependencies (`Progress`, `ConcurrencyLimit`, `Timeout`, etc.) are implemented. A `Dependency` subclass is an async context manager — `__aenter__` sets up the resource and returns the value injected into the task, and `__aexit__` handles cleanup.

```python
from docket.dependencies import Dependency

class RateLimitedClient(Dependency):
    """Injects a rate-limited HTTP client scoped to this task execution."""

    def __init__(self, requests_per_second: int = 10) -> None:
        self.requests_per_second = requests_per_second

    async def __aenter__(self) -> httpx.AsyncClient:
        self._client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=self.requests_per_second)
        )
        return self._client

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self._client.aclose()

async def fetch_pages(
    urls: list[str],
    client: httpx.AsyncClient = RateLimitedClient(requests_per_second=5)
) -> None:
    for url in urls:
        response = await client.get(url)
        await process_response(response)
```

Inside `__aenter__`, you can access the current execution context through the
module-level context variables `current_docket`, `current_worker`, and
`current_execution`:

```python
from docket.dependencies import Dependency, current_execution, current_worker

class AuditedDependency(Dependency):
    async def __aenter__(self) -> AuditLog:
        execution = current_execution.get()
        worker = current_worker.get()
        return AuditLog(task_key=execution.key, worker_name=worker.name)
```

Or use the higher-level contextual dependencies for cleaner code:

```python
from docket import CurrentExecution, CurrentWorker, Depends, Execution, Worker

async def create_audit_log(
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> AuditLog:
    return AuditLog(task_key=execution.key, worker_name=worker.name)

async def audited_task(
    audit_log: AuditLog = Depends(create_audit_log),
) -> None:
    ...
```


## Worker-level dependencies

Sometimes you want a dependency to run around **every** task a worker
executes — tracing, a database transaction, an audit log, a feature-flag
context. Instead of repeating the same `Depends(...)` on every task,
register them on the worker itself:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def trace_task(key: str = TaskKey()):
    with tracer.start_as_current_span(f"task:{key}"):
        yield

async def get_db_pool() -> Pool:
    return await create_pool()

async with Worker(
    docket,
    dependencies={
        "trace": Depends(trace_task),
        "db":    Depends(get_db_pool),
    },
) as worker:
    await worker.run_forever()
```

Each value is a `Dependency` instance: wrap factories with `Depends(...)`
(sync functions, async functions, `@contextmanager`, `@asynccontextmanager`
all work there), or pass a built-in dependency like `Retry(attempts=3)`
directly. Worker dependencies may declare their own parameters
(`TaskKey`, `TaskArgument("customer_id")`, `CurrentWorker`, nested
`Depends(...)`), which resolve recursively — exactly like task-level
dependencies.

### Lifecycle

Worker dependencies enter **before** each task body runs and exit
**after** it finishes, in LIFO order. Teardown runs even when the task
raises or a `Timeout` cancels it.

```
enter:  trace → db → task body
exit:   task body → db → trace
```

### Cache sharing

Worker dependencies share the per-task resolution cache with task-level
`Depends(...)`. If a worker dependency calls `Depends(get_db_pool)` and a
task also calls `Depends(get_db_pool)`, both receive the same instance
for that task — no duplicate setup.

### Failures

If a worker dependency fails during setup, the task fails with an
`ExceptionGroup` naming the faulty dependency (the name you gave it in
the dict), and any task-level `Retry` policy applies normally. Raising
`AdmissionBlocked` from a worker dependency reschedules the task, just
like task-level admission controls.

Names starting with `__` are reserved for internal use and rejected at
`Worker` construction.

### Worker-level `single=True` dependencies

`single=True` dependencies — `Timeout`, `Retry`, `Perpetual`,
`ConcurrencyLimit`, `Debounce` — act as **defaults** for every task the
worker runs. Pass bare instances directly:

```python
async with Worker(
    docket,
    dependencies={
        "timeout": Timeout(timedelta(seconds=30)),
        "retry":   Retry(attempts=3),
    },
) as worker:
    await worker.run_forever()
```

Bare instances are safe to reuse across tasks: stateful ones like `Retry`
and `Perpetual` construct a fresh internal instance on each execution,
so a single `Retry(attempts=3)` at worker construction gives every task
its own attempt counter.

Declaring the same `single=True` type in both task and worker is an
error: the task fails with a `ValueError` naming both sources. There can
be only one.
