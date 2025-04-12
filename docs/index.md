# Welcome to docket

docket is a distributed background task system for Python functions that makes scheduling future work as seamless as immediate work. Built with modern Python features and async/await support, it provides a robust foundation for distributed task processing.

## Key Features

- **Async First**: Built for modern Python with full async/await support
- **Immediate and Scheduled Tasks**: Execute tasks right away or schedule them for the future
- **Redis-Powered**: Built on Redis for reliable message brokering and storage
- **Self-Perpetuating Tasks**: First-class support for recurring tasks with the `@Perpetual` decorator
- **Dependency Injection**: Rich dependency injection system for tasks with `Depends`
- **Task Strikes**: Temporarily disable tasks based on conditions
- **Idempotency**: Guarantee task uniqueness with caller-defined keys
- **Developer-Friendly**: Easy to use in both production code and test suites
- **Observability**: Built-in OpenTelemetry instrumentation and structured logging

## How It Works

docket integrates two powerful modes of task execution:

1. **Immediate Tasks**: Tasks are pushed onto a Redis stream and picked up by available workers
2. **Scheduled Tasks**: Tasks are stored in a Redis sorted set with their schedule time and automatically moved to the immediate queue when their time arrives

## Quick Start

```python
from datetime import timedelta
from docket import Docket, Perpetual, Depends, TaskLogger

# Create a docket instance
async with Docket() as docket:
    # Define a task with dependencies
    @docket.add
    @Perpetual(interval=timedelta(minutes=5))
    async def monitor_service(
        service_id: str,
        logger: TaskLogger = Depends(TaskLogger)
    ):
        status = await check_service(service_id)
        logger.info("Service status", service_id=service_id, status=status)
        if status.needs_attention:
            await notify_admin(service_id, status)
        # Task will automatically reschedule itself for 5 minutes later

    # Schedule the task
    await monitor_service.schedule(
        key="monitor-service-main",  # Unique key for idempotency
        service_id="main-service"
    )
```

## Why docket?

- **Modern Python**: Built for Python 3.12+ with type hints and async/await
- **Reliability**: Built on Redis for robust message handling and persistence
- **Flexibility**: Rich feature set for both immediate and scheduled tasks
- **Testability**: First-class support for testing with async test suites
- **Observability**: OpenTelemetry integration for monitoring and tracing
- **Efficiency**: Optimized for both immediate execution and future scheduling

## Installation

```bash
pip install pydocket
```

For more detailed information, check out our [Getting Started](getting-started.md) guide or dive into the [API Reference](api-reference.md).
