# Welcome to docket

Docket is a distributed background task system for Python functions with a focus
on the scheduling of future work as seamlessly and efficiently as immediate work.

[![PyPI - Version](https://img.shields.io/pypi/v/pydocket)](https://pypi.org/project/pydocket/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydocket)](https://pypi.org/project/pydocket/)
[![GitHub main checks](https://img.shields.io/github/check-runs/chrisguidry/docket/main)](https://github.com/chrisguidry/docket/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/chrisguidry/docket)](https://app.codecov.io/gh/chrisguidry/docket)
[![PyPI - License](https://img.shields.io/pypi/l/pydocket)](https://github.com/chrisguidry/docket/blob/main/LICENSE)

## At a glance

```python
from datetime import datetime, timedelta, timezone

from docket import Docket


async def greet(name: str, greeting="Hello") -> None:
    print(f"{greeting}, {name} at {datetime.now()}!")


async with Docket() as docket:
    await docket.add(greet)("Jane")

    now = datetime.now(timezone.utc)
    soon = now + timedelta(seconds=3)
    await docket.add(greet, when=soon)("John", greeting="Howdy")
```

And in another process, run a worker:

```python
from docket import Docket, Worker

async with Docket() as docket:
    async with Worker(docket) as worker:
        await worker.run_until_finished()
```

Which produces:

```
Hello, Jane at 2025-03-05 13:58:21.552644!
Howdy, John at 2025-03-05 13:58:24.550773!
```

## Why docket?

⚡️ Snappy one-way background task processing without any bloat

📅 Schedule immediate or future work seamlessly with the same interface

⏭️ Skip problematic tasks or parameters without redeploying

🌊 Purpose-built for Redis streams

🧩 Fully type-complete and type-aware for your background task functions

## How It Works

docket integrates two modes of task execution:

1. **Immediate tasks** are pushed onto a Redis stream and are available to be picked up by any worker.
2. **Scheduled tasks** are pushed onto a Redis sorted set with a schedule time. A loop within each worker moves scheduled tasks onto the stream when their schedule time has arrived. This move is performed as a Lua script to ensure atomicity.

Docket requires a [Redis](http://redis.io/) server with Streams support (which was
introduced in Redis 5.0.0). Docket is tested with Redis 6, 7, and 8.

For more detailed information, check out our [Getting Started](getting-started.md) guide or dive into the [API Reference](api-reference.md).
