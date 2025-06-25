# Dependencies Guide

Docket tasks include a dependency injection system that provides access to context, configuration, and custom resources. This system is similar to FastAPI's dependency injection but tailored for background task patterns.

## Built-in Context Dependencies

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

## Advanced Retry Patterns

### Exponential Backoff

For services that might be overloaded, exponential backoff gives them time to recover:

```python
from docket import ExponentialRetry

async def call_external_api(
    url: str,
    retry: ExponentialRetry = ExponentialRetry(
        attempts=5,
        minimum_delay=timedelta(seconds=1),
        maximum_delay=timedelta(minutes=5)
    )
) -> None:
    # Retries with delays: 1s, 2s, 4s, 8s, 16s (but capped at 5 minutes)
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        print(f"API call succeeded on attempt {retry.attempt}")
    except Exception as e:
        print(f"Attempt {retry.attempt} failed: {e}")
        raise
```

### Unlimited Retries

For critical tasks that must eventually succeed, use `attempts=None`:

```python
from docket import Retry

async def critical_data_sync(
    source_url: str,
    retry: Retry = Retry(attempts=None, delay=timedelta(minutes=5))
) -> None:
    # This will retry forever with 5-minute delays until it succeeds
    await sync_critical_data(source_url)
    print(f"Critical sync completed after {retry.attempt} attempts")
```

Both `Retry` and `ExponentialRetry` support unlimited retries this way.

## Task Timeouts

Prevent tasks from running too long with the `Timeout` dependency:

```python
from docket import Timeout

async def data_processing_task(
    large_dataset: dict,
    timeout: Timeout = Timeout(timedelta(minutes=10))
) -> None:
    # This task will be cancelled if it runs longer than 10 minutes
    await process_dataset_phase_one(large_dataset)

    # Extend timeout if we need more time for phase two
    timeout.extend(timedelta(minutes=5))
    await process_dataset_phase_two(large_dataset)
```

The `extend()` method can take a specific duration or default to the original timeout duration:

```python
async def adaptive_timeout_task(
    timeout: Timeout = Timeout(timedelta(minutes=2))
) -> None:
    await quick_check()

    # Extend by the base timeout (another 2 minutes)
    timeout.extend()
    await longer_operation()
```

Timeouts work alongside retries. If a task times out, it can be retried according to its retry policy.

## Custom Dependencies

Create your own dependencies using `Depends()` for reusable resources and patterns:

```python
from contextlib import asynccontextmanager
from docket import Depends

@asynccontextmanager
async def get_database_connection():
    """Simple dependency that returns a database connection."""
    conn = await database.connect()
    try:
        yield conn
    finally:
        await conn.close()

@asynccontextmanager
async def get_redis_client():
    """Another dependency for Redis operations."""
    client = redis.Redis(host='localhost', port=6379)
    try:
        yield client
    finally:
        client.close()

async def process_user_data(
    user_id: int,
    db=Depends(get_database_connection),
    cache=Depends(get_redis_client)
) -> None:
    # Both dependencies are automatically provided and cleaned up
    user = await db.fetch_user(user_id)
    await cache.set(f"user:{user_id}", user.to_json())
```

### Nested Dependencies

Dependencies can depend on other dependencies, and Docket resolves them in the correct order:

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

### Context Manager Dependencies

Dependencies can be async context managers for automatic resource cleanup:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def get_file_lock(filename: str):
    """A dependency that provides file locking."""
    lock = await acquire_file_lock(filename)
    try:
        yield lock
    finally:
        await release_file_lock(filename)

async def process_shared_file(
    filename: str,
    file_lock=Depends(lambda: get_file_lock("shared.txt"))
) -> None:
    # File is locked before task starts, unlocked after task completes
    await process_file_safely(filename)
```

### Dependencies with Built-in Context

Dependencies can access Docket's built-in context dependencies:

```python
async def get_task_logger(
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker()
) -> LoggerAdapter:
    """Create a logger with task and worker context."""
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

## TaskArgument: Accessing Task Parameters

Dependencies can access the task's input arguments using `TaskArgument`:

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

## Dependency Error Handling

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

## Dependency Guidelines

### Design for Reusability

Create dependencies that can be used across multiple tasks:

```python
# Good: Reusable across many tasks
async def get_api_client():
    return APIClient(api_key=os.getenv("API_KEY"))

# Less ideal: Too specific to one task
async def get_user_api_client_for_profile_updates():
    return APIClient(api_key=os.getenv("API_KEY"), timeout=30)
```

### Keep Dependencies Focused

Each dependency should have a single responsibility:

```python
# Good: Focused dependencies
async def get_database():
    return await database.connect()

async def get_cache():
    return redis.Redis()

# Less ideal: Too many responsibilities
async def get_all_services():
    return {
        'db': await database.connect(),
        'cache': redis.Redis(),
        'api': APIClient(),
        'metrics': MetricsClient()
    }
```

### Handle Resource Cleanup

Always use context managers or try/finally for resource cleanup:

```python
# Good: Automatic cleanup
async def get_database():
    conn = await database.connect()
    try:
        yield conn
    finally:
        await conn.close()

# Risky: Manual cleanup required
async def get_database_no_cleanup():
    return await database.connect()  # Who closes this?
```

The dependency injection system supports flexible task design while maintaining clear separation of concerns. Dependencies can be simple values, complex services, or entire subsystems that your tasks need to operate effectively.
