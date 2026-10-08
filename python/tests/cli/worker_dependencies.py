"""Importable collections for the worker CLI dependency integration tests."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator

from docket import CurrentExecution, Depends, Execution


task_context: ContextVar[str] = ContextVar("task_context")


@asynccontextmanager
async def task_logging_context(
    execution: Execution = CurrentExecution(),
) -> AsyncGenerator[None, None]:
    token = task_context.set(execution.key)
    print(f"enter {execution.key}")
    try:
        yield
    finally:
        print(f"exit {execution.key}")
        task_context.reset(token)


async def task_with_context() -> None:
    print(f"task {task_context.get()}")


dependencies = [Depends(task_logging_context)]
tasks = [task_with_context]
