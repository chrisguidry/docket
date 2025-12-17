"""
Example: Using fallback_task for dynamic task dispatch

This example demonstrates how to use fallback_task to implement a generic
arithmetic system. A "scheduler" service registers stub functions and schedules
tasks, while a separate "worker" service handles them via a fallback without
needing the original function definitions.

This pattern is useful for:
- Loose coupling between schedulers and workers
- Avoiding heavy imports in scheduling code
- Dynamic dispatch based on function names

Run with:
    python examples/fallback_arithmetic.py
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from docket import CurrentExecution, Docket, TaskLogger, Worker
from docket.execution import Execution

logging.basicConfig(level=logging.INFO)


# --- Scheduler side: stub functions just for registration ---


async def add(a: float, b: float) -> float:
    """Stub - actual implementation is in the fallback."""
    raise NotImplementedError


async def subtract(a: float, b: float) -> float:
    """Stub - actual implementation is in the fallback."""
    raise NotImplementedError


async def multiply(a: float, b: float) -> float:
    """Stub - actual implementation is in the fallback."""
    raise NotImplementedError


async def divide(a: float, b: float) -> float:
    """Stub - actual implementation is in the fallback."""
    raise NotImplementedError


# --- Worker side: fallback handles all arithmetic ---


async def arithmetic_fallback(
    a: float,
    b: float,
    execution: Execution = CurrentExecution(),
    logger: logging.LoggerAdapter[logging.Logger] = TaskLogger(),
    **kwargs: Any,
) -> float | None:
    """A fallback that handles arithmetic operations dynamically."""
    match execution.function_name:
        case "add":
            result = a + b
        case "subtract":
            result = a - b
        case "multiply":
            result = a * b
        case "divide":
            if b == 0:
                logger.error("Division by zero!")
                return None
            result = a / b
        case _:
            logger.warning("Unknown operation: %r", execution.function_name)
            return None

    logger.info("%s(%s, %s) = %s", execution.function_name, a, b, result)
    return result


async def main() -> None:
    # Scheduler docket: registers stubs and schedules tasks
    async with Docket(name="arithmetic-example") as scheduler_docket:
        scheduler_docket.register(add)
        scheduler_docket.register(subtract)
        scheduler_docket.register(multiply)
        scheduler_docket.register(divide)

        # Schedule some arithmetic operations
        await scheduler_docket.add(add)(10, 5)
        await scheduler_docket.add(subtract)(10, 5)
        await scheduler_docket.add(multiply)(10, 5)
        await scheduler_docket.add(divide)(10, 5)
        await scheduler_docket.add(divide)(10, 0)  # Division by zero

    # Worker docket: does NOT register the stubs, uses fallback instead
    async with Docket(name="arithmetic-example") as worker_docket:
        async with Worker(
            worker_docket,
            fallback_task=arithmetic_fallback,
        ) as worker:
            await worker.run_until_finished()


if __name__ == "__main__":
    asyncio.run(main())
