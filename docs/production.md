# Docket in Production

This page covers configuring workers, connecting to Redis, managing state and results, monitoring, and operational tools for running Docket in production.

## Worker Configuration

### Core Settings

Workers have several configuration knobs for different environments:

```python
async with Worker(
    docket,
    name="worker-1",                                    # Unique worker identifier
    concurrency=20,                                     # Parallel task limit
    redelivery_timeout=timedelta(minutes=5),           # When to redeliver tasks
    reconnection_delay=timedelta(seconds=5),           # Redis reconnection backoff
    minimum_check_interval=timedelta(milliseconds=100), # Polling frequency
    scheduling_resolution=timedelta(milliseconds=250),  # Future task check frequency
    schedule_automatic_tasks=True                       # Enable perpetual task startup
) as worker:
    await worker.run_forever()
```

### Environment Variable Configuration

All settings can be configured via environment variables:

```bash
# Core docket settings
export DOCKET_NAME=orders
export DOCKET_URL=redis://redis.production.com:6379/0

# Worker settings
export DOCKET_WORKER_NAME=orders-worker-1
export DOCKET_WORKER_CONCURRENCY=50
export DOCKET_WORKER_REDELIVERY_TIMEOUT=10m
export DOCKET_WORKER_RECONNECTION_DELAY=5s
export DOCKET_WORKER_MINIMUM_CHECK_INTERVAL=100ms
export DOCKET_WORKER_SCHEDULING_RESOLUTION=250ms

# Monitoring
export DOCKET_WORKER_HEALTHCHECK_PORT=8080
export DOCKET_WORKER_METRICS_PORT=9090

# Logging
export DOCKET_LOGGING_LEVEL=INFO
export DOCKET_LOGGING_FORMAT=json

# Task modules
export DOCKET_TASKS=myapp.tasks:production_tasks
```

### CLI Usage

Run workers in production using the CLI:

```bash
# Basic worker
docket worker --tasks myapp.tasks:all_tasks

# Production worker with full configuration
docket worker \
  --docket orders \
  --url redis://redis.prod.com:6379/0 \
  --name orders-worker-1 \
  --concurrency 50 \
  --redelivery-timeout 10m \
  --healthcheck-port 8080 \
  --metrics-port 9090 \
  --logging-format json \
  --tasks myapp.tasks:production_tasks
```

### Signal Handling

Workers catch `SIGTERM` and `SIGINT` and shut down gracefully — they stop accepting new tasks and wait for in-flight tasks to finish before exiting. On container orchestrators like Kubernetes, set `terminationGracePeriodSeconds` to be longer than your slowest expected task so the worker has time to drain. Tasks that don't finish before the grace period expires will be redelivered to other workers based on `redelivery_timeout`.

## Connection Management

### Redis Connection Pools

Docket automatically manages Redis connection pools. To use a custom pool:

```python
from redis.asyncio import ConnectionPool

pool = ConnectionPool.from_url(
    "redis://redis.prod.com:6379/0",
    max_connections=50,  # Match or exceed worker concurrency
    retry_on_timeout=True
)

async with Docket(name="orders", connection_pool=pool) as docket:
    pass
```

### Redis Cluster Support

Docket supports Redis Cluster using the `redis+cluster://` URL scheme:

```python
# Connect to Redis Cluster
async with Docket(
    name="orders",
    url="redis+cluster://cluster-node-1:6379/0"
) as docket:
    pass

# With authentication
async with Docket(
    name="orders",
    url="redis+cluster://user:password@cluster-node-1:6379/0"
) as docket:
    pass

# TLS connection to cluster
async with Docket(
    name="orders",
    url="rediss+cluster://cluster-node-1:6379/0"
) as docket:
    pass
```

When using cluster mode, Docket automatically:

- Uses hash-tagged keys (`{docket_name}:*`) to ensure all keys hash to the same slot
- Manages cluster client lifecycle and connection distribution
- Handles pub/sub through a dedicated node connection (cluster pub/sub limitation)

**Note:** All Docket data for a single docket name will be stored on the same cluster shard. This ensures atomicity for Lua scripts and simplifies data management, but means individual dockets don't benefit from cluster data distribution.

### Authentication

Docket supports Redis authentication via URL credentials:

```python
# Password-only authentication (Redis default user)
docket_url = "redis://:password@redis.prod.com:6379/0"

# Username and password authentication (Redis 6.0+ ACL)
docket_url = "redis://myuser:mypassword@redis.prod.com:6379/0"
```

### ACL Configuration

When using Redis ACLs with a restricted user, grant access to the key pattern matching your docket name.

**Standalone Redis:** Keys follow the pattern `{docket_name}:*`

```bash
# Create a user with restricted permissions for a docket named "orders"
ACL SETUSER docket-user on >secure-password ~orders:* &orders:* +@all
```

**Redis Cluster:** Keys are hash-tagged with curly braces `{docket_name}:*`

```bash
# For cluster mode, the pattern includes the hash tag braces
ACL SETUSER docket-user on >secure-password ~{orders}:* &{orders}:* +@all
```

The required permissions are:

- **Key pattern**: `~{docket_name}:*` (standalone) or `~\{docket_name\}:*` (cluster) - matches all Redis keys used by Docket
- **Channel pattern**: `&{docket_name}:*` (standalone) or `&\{docket_name\}:*` (cluster) - required for task cancellation pub/sub
- **Commands**: `+@all` or the specific command categories Docket uses

For production deployments, you may restrict to only the required command categories:

```bash
# More restrictive command permissions (standalone)
ACL SETUSER docket-user on >secure-password \
  ~orders:* &orders:* \
  +@read +@write +@set +@sortedset +@hash +@stream +@pubsub +@scripting +@connection

# More restrictive command permissions (cluster)
ACL SETUSER docket-user on >secure-password \
  ~{orders}:* &{orders}:* \
  +@read +@write +@set +@sortedset +@hash +@stream +@pubsub +@scripting +@connection
```

### Valkey Support

Docket also works with Valkey (Redis fork):

```bash
export DOCKET_URL=valkey://valkey.prod.com:6379/0
```

## State and Result Storage

Docket tracks execution state and stores task results by default. You can configure or disable this behavior based on your observability and throughput requirements.

### Execution State Tracking

By default, Docket stores execution state (SCHEDULED, QUEUED, RUNNING, COMPLETED, FAILED) in Redis with a 15-minute TTL:

```python
async with Docket(
    name="orders",
    url="redis://localhost:6379/0",
    execution_ttl=timedelta(minutes=15)  # Default
) as docket:
    # State records expire 15 minutes after task completion
    pass
```

The `execution_ttl` controls:

- How long state records persist in Redis after task completion
- How long result data is retained (see Result Storage below)
- How long progress information remains available

### Fire-and-Forget Mode

For maximum throughput in high-volume scenarios, disable state tracking entirely:

```python
async with Docket(
    name="high-throughput",
    url="redis://localhost:6379/0",
    execution_ttl=timedelta(0)  # Disable state persistence
) as docket:
    # No state records, no result storage, no progress tracking
    for event in events:
        await docket.add(process_event)(event)
```

With `execution_ttl=0`:

- **No state records**: State transitions are not written to Redis
- **No result storage**: Task return values are not persisted
- **No progress tracking**: Progress updates are not recorded
- **Maximum throughput**: Minimizes Redis operations per task
- **get_result() unavailable**: Cannot retrieve task results

### Result Storage Configuration

Task results are stored using the `py-key-value-aio` library. By default, Docket uses `RedisStore` but you can provide a custom storage backend:

```python
from key_value.stores.redis import RedisStore
from key_value.stores.memory import MemoryStore

# Default: Redis-backed result storage
async with Docket(
    name="production",
    url="redis://localhost:6379/0",
    result_storage=RedisStore(url="redis://localhost:6379/1")  # Separate DB
) as docket:
    pass

# Alternative: In-memory storage for testing
async with Docket(
    name="test",
    url="redis://localhost:6379/0",
    result_storage=MemoryStore()
) as docket:
    pass
```

Custom storage backends must implement the `KeyValueStore` protocol from `py-key-value-aio`.

### Result Storage Tips

**Separate Redis Database**: Store results in a different Redis database than task queues:

```python
result_storage = RedisStore(url="redis://localhost:6379/1")  # DB 1 for results
docket = Docket(
    name="orders",
    url="redis://localhost:6379/0",  # DB 0 for queues
    result_storage=result_storage
)
```

**TTL Management**: Results inherit the `execution_ttl` setting. Adjust based on how long you need results:

```python
# Keep results for 1 hour
async with Docket(execution_ttl=timedelta(hours=1)) as docket:
    execution = await docket.add(generate_report)()
    # Result available for 1 hour after completion
```

**None Returns**: Tasks that return `None` don't write to result storage, saving space:

```python
async def send_notification(user_id: str) -> None:
    await send_email(user_id)
    # No result stored - task returns None
```

**Large Results**: For large result data, consider storing references instead:

```python
# Instead of returning large data
async def process_large_dataset(dataset_id: str) -> dict:
    data = await expensive_computation()
    return data  # Large object stored in Redis

# Store a reference
async def process_large_dataset(dataset_id: str) -> str:
    data = await expensive_computation()
    s3_key = await store_in_s3(data)
    return s3_key  # Small string stored in Redis
```

### Monitoring State and Result Storage

Track Redis memory usage for state and result data:

```bash
# Check memory usage
redis-cli info memory

# Count state records
redis-cli --scan --pattern "docket:runs:*" | wc -l

# Count result keys
redis-cli --scan --pattern "docket:results:*" | wc -l
```

If memory usage is high:

- Reduce `execution_ttl` to expire data sooner
- Use `execution_ttl=0` for fire-and-forget tasks
- Store large results externally (S3, database) and return references
- Separate result storage to a different Redis instance

## Monitoring and Observability

### Prometheus Metrics

Enable Prometheus metrics with the `--metrics-port` option:

```bash
docket worker --metrics-port 9090
```

Available metrics include:

#### Task Counters

- `docket_tasks_added` - Tasks scheduled
- `docket_tasks_started` - Tasks begun execution
- `docket_tasks_succeeded` - Successfully completed tasks
- `docket_tasks_failed` - Failed tasks
- `docket_tasks_retried` - Retry attempts
- `docket_tasks_stricken` - Tasks blocked by strikes

#### Task Timing

- `docket_task_duration` - Histogram of task execution times
- `docket_task_punctuality` - How close tasks run to their scheduled time

#### System Health

- `docket_queue_depth` - Tasks ready for immediate execution
- `docket_schedule_depth` - Tasks scheduled for future execution
- `docket_tasks_running` - Currently executing tasks
- `docket_redis_disruptions` - Redis connection failures
- `docket_strikes_in_effect` - Active strike rules

All metrics include labels for docket name, worker name, and task function name.

### Health Checks

Enable health check endpoints:

```bash
docket worker --healthcheck-port 8080
```

The health check endpoint (`/`) returns 200 OK when the worker is healthy and able to process tasks.

### OpenTelemetry Traces

Docket automatically creates OpenTelemetry spans for task execution:

- **Span name**: `docket.task.{function_name}`
- **Attributes**: docket name, worker name, task key, attempt number
- **Status**: Success/failure with error details
- **Duration**: Complete task execution time

Configure your OpenTelemetry exporter to send traces to your observability platform. See the [OpenTelemetry Python documentation](https://opentelemetry.io/docs/languages/python/) for configuration examples with various backends like Jaeger, Zipkin, or cloud providers.

### Structured Logging

Configure structured logging for production:

```bash
# JSON logs for log aggregation
docket worker --logging-format json --logging-level info

# Plain logs for simple deployments
docket worker --logging-format plain --logging-level warning
```

Log entries include:

- Task execution start/completion
- Error details with stack traces
- Worker lifecycle events
- Redis connection status
- Strike/restore operations

## Striking and Restoring Tasks

Striking temporarily disables tasks without redeploying code.

### Striking Entire Task Types

Disable all instances of a specific task:

```python
# Disable all order processing during maintenance
await docket.strike(process_order)

# Orders added during this time won't be processed
await docket.add(process_order)(order_id=12345)  # Won't run
await docket.add(process_order)(order_id=67890)  # Won't run

# Re-enable when ready
await docket.restore(process_order)
```

### Striking by Parameter Values

Disable tasks based on their arguments using comparison operators:

```python
# Block all tasks for a problematic customer
await docket.strike(None, "customer_id", "==", "12345")

# Block low-priority work during high load
await docket.strike(process_order, "priority", "<=", "low")

# Block all orders above a certain value during fraud investigation
await docket.strike(process_payment, "amount", ">", 10000)

# Later, restore them
await docket.restore(None, "customer_id", "==", "12345")
await docket.restore(process_order, "priority", "<=", "low")
```

Supported operators include `==`, `!=`, `<`, `<=`, `>`, `>=`.

### Striking Specific Task-Parameter Combinations

Target very specific scenarios:

```python
# Block only high-value orders for a specific customer
await docket.strike(process_order, "customer_id", "==", "12345")
await docket.strike(process_order, "amount", ">", 1000)

# This order won't run (blocked customer)
await docket.add(process_order)(customer_id="12345", amount=500)

# This order won't run (blocked customer AND high amount)
await docket.add(process_order)(customer_id="12345", amount=2000)

# This order WILL run (different customer)
await docket.add(process_order)(customer_id="67890", amount=2000)
```

### Striking from the CLI

You can also strike and restore tasks from the command line:

```bash
# Block problematic tasks immediately
docket strike problematic_function

# Block tasks for specific customers
docket strike process_order customer_id == "problematic-customer"

# Restore when issues are resolved
docket restore problematic_function
```

## Built-in Utility Tasks

Docket provides utility tasks for smoke-testing and debugging:

```python
from docket import tasks

# Simple trace logging
await docket.add(tasks.trace)("System startup completed")
await docket.add(tasks.trace)("Processing batch 123")

# Intentional failures for testing error handling
await docket.add(tasks.fail)("Testing error notification system")
```
