# Docket in Production

Running Docket at scale requires understanding its Redis-based architecture, configuring workers appropriately, and monitoring system health. This guide covers everything you need for reliable production deployments.

## Redis Streams Architecture

Docket uses Redis streams and sorted sets to provide reliable task delivery with at-least-once semantics. Note that Docket requires a single Redis instance and does not support Redis Cluster.

### Task Lifecycle

Understanding how tasks flow through the system helps with monitoring and troubleshooting:

1. **Immediate tasks** go directly to the Redis stream and are available to any worker in the consumer group
2. **Future tasks** are stored in the sorted set with their execution time as the score
3. **Workers continuously move** due tasks from the sorted set to the stream
4. **Consumer groups** ensure each task is delivered to exactly one worker
5. **Acknowledgment** removes completed tasks; unacknowledged tasks are redelivered

### Redelivery Behavior

When a worker crashes or fails to acknowledge a task within `redelivery_timeout`, Redis automatically makes the task available to other workers. This ensures reliability but means tasks may execute more than once.

```python
# Configure redelivery timeout based on your longest-running tasks
async with Worker(
    docket,
    redelivery_timeout=timedelta(minutes=10)  # Adjust for your workload
) as worker:
    await worker.run_forever()
```

Set redelivery timeout to be longer than your 99th percentile task duration to minimize duplicate executions.

### Redis Data Structures

Docket creates several Redis data structures for each docket:

- **Stream (`{docket}:stream`)**: Ready-to-execute tasks using Redis consumer groups
- **Sorted Set (`{docket}:queue`)**: Future tasks ordered by scheduled execution time
- **Hashes (`{docket}:{key}`)**: Serialized task data for scheduled tasks
- **Set (`{docket}:workers`)**: Active worker heartbeats with timestamps
- **Set (`{docket}:worker-tasks:{worker}`)**: Tasks each worker can execute
- **Stream (`{docket}:strikes`)**: Strike/restore commands for operational control

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

All settings can be configured via environment variables for production deployments:

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

### Tuning for Different Workloads

**High-throughput, fast tasks:**

```bash
docket worker \
  --concurrency 100 \
  --redelivery-timeout 30s \
  --minimum-check-interval 50ms \
  --scheduling-resolution 100ms
```

**Long-running, resource-intensive tasks:**

```bash
docket worker \
  --concurrency 5 \
  --redelivery-timeout 1h \
  --minimum-check-interval 1s \
  --scheduling-resolution 5s
```

**Mixed workload with perpetual tasks:**

```bash
docket worker \
  --concurrency 25 \
  --redelivery-timeout 5m \
  --schedule-automatic-tasks \
  --tasks myapp.tasks:all_tasks,myapp.monitoring:health_checks
```

## Connection Management

### Redis Connection Pools

Docket automatically manages Redis connection pools, but you can tune them for your environment:

```python
from redis.asyncio import ConnectionPool

# Custom connection pool for high-concurrency workers
pool = ConnectionPool.from_url(
    "redis://redis.prod.com:6379/0",
    max_connections=50,  # Match or exceed worker concurrency
    retry_on_timeout=True
)

async with Docket(name="orders", connection_pool=pool) as docket:
    # Use the custom pool
    pass
```

### Redis Requirements

Docket requires a single Redis instance and does not currently support Redis Cluster. For high availability, consider:

- **Managed Redis services** like AWS ElastiCache, Google Cloud Memorystore, or Redis Cloud
- **Redis replicas** with manual failover procedures

```python
# With authentication
docket_url = "redis://:password@redis.prod.com:6379/0"
```

### Valkey Support

Docket also works with Valkey (Redis fork):

```bash
export DOCKET_URL=valkey://valkey.prod.com:6379/0
```

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

### Example Grafana Dashboard

Monitor Docket health with queries like:

```promql
# Task throughput
rate(docket_tasks_completed[5m])

# Error rate
rate(docket_tasks_failed[5m]) / rate(docket_tasks_started[5m])

# Queue depth trending
docket_queue_depth

# P95 task duration
histogram_quantile(0.95, rate(docket_task_duration_bucket[5m]))

# Worker availability
up{job="docket-workers"}
```

## Production Guidelines

### Capacity Planning

**Estimate concurrent tasks:**

```
concurrent_tasks = avg_task_duration * tasks_per_second
worker_concurrency = concurrent_tasks * 1.2  # 20% buffer
```

**Size worker pools:**

- Start with 1-2 workers per CPU core
- Monitor CPU and memory usage
- Scale horizontally rather than increasing concurrency indefinitely

### Deployment Strategies

**Blue-green deployments:**

```bash
# Deploy new workers with different name
docket worker --name orders-worker-v2 --tasks myapp.tasks:v2_tasks

# Gradually strike old task versions
docket strike old_task_function

# Scale down old workers after tasks drain
```

### Error Handling

**Configure appropriate retries:**

```python
# Transient failures - short delays
async def api_call(
    retry: Retry = Retry(attempts=3, delay=timedelta(seconds=5))
): ...

# Infrastructure issues - exponential backoff
async def database_sync(
    retry: ExponentialRetry = ExponentialRetry(
        attempts=5,
        minimum_delay=timedelta(seconds=30),
        maximum_delay=timedelta(minutes=10)
    )
): ...

# Critical operations - unlimited retries
async def financial_transaction(
    retry: Retry = Retry(attempts=None, delay=timedelta(minutes=1))
): ...
```

**Dead letter handling:**

```python
async def process_order(order_id: str) -> None:
    try:
        await handle_order(order_id)
    except CriticalError as e:
        # Send to dead letter queue for manual investigation
        await send_to_dead_letter_queue(order_id, str(e))
        raise
```

### Operational Procedures

**Graceful shutdown:**

```bash
# Workers handle SIGTERM gracefully
kill -TERM $WORKER_PID

# Or use container orchestration stop signals
docker stop docket-worker
```

**Emergency task blocking:**

```bash
# Block problematic tasks immediately
docket strike problematic_function

# Block tasks for specific customers
docket strike process_order customer_id == "problematic-customer"

# Restore when issues are resolved
docket restore problematic_function
```

**Monitoring checklist:**

- Queue depth alerts (tasks backing up)
- Error rate alerts (> 5% failure rate)
- Task duration alerts (P95 > expected)
- Worker availability alerts
- Redis connection health

### Scaling Considerations

**Horizontal scaling:**

- Add workers across multiple machines
- Use consistent worker naming for monitoring
- Monitor Redis memory usage as task volume grows

**Vertical scaling:**

- Increase worker concurrency for I/O bound tasks
- Increase memory limits for large task payloads
- Monitor CPU usage to avoid oversubscription

**Redis scaling:**

- Use managed Redis services for high availability (Redis Cluster is not supported)
- Monitor memory usage and eviction policies
- Scale vertically for larger workloads

Running Docket in production requires attention to these operational details, but the Redis-based architecture and monitoring support can help with demanding production workloads.
