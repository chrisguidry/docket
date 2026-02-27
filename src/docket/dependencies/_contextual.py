"""Contextual dependencies for accessing current Docket, Worker, Execution, etc."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from ._base import Dependency, current_docket, current_execution, current_worker

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker


class _CurrentWorker(Dependency["Worker"]):
    async def __aenter__(self) -> Worker:
        return current_worker.get()


def CurrentWorker() -> Worker:
    """A dependency to access the current Worker.

    Example:

    ```python
    @task
    async def my_task(worker: Worker = CurrentWorker()) -> None:
        assert isinstance(worker, Worker)
    ```
    """
    return cast("Worker", _CurrentWorker())


class _CurrentDocket(Dependency["Docket"]):
    async def __aenter__(self) -> Docket:
        return current_docket.get()


def CurrentDocket() -> Docket:
    """A dependency to access the current Docket.

    Example:

    ```python
    @task
    async def my_task(docket: Docket = CurrentDocket()) -> None:
        assert isinstance(docket, Docket)
    ```
    """
    return cast("Docket", _CurrentDocket())


class _CurrentExecution(Dependency["Execution"]):
    async def __aenter__(self) -> Execution:
        return current_execution.get()


def CurrentExecution() -> Execution:
    """A dependency to access the current Execution.

    Example:

    ```python
    @task
    async def my_task(execution: Execution = CurrentExecution()) -> None:
        assert isinstance(execution, Execution)
    ```
    """
    return cast("Execution", _CurrentExecution())


class _TaskKey(Dependency[str]):
    async def __aenter__(self) -> str:
        return current_execution.get().key


def TaskKey() -> str:
    """A dependency to access the key of the currently executing task.

    Example:

    ```python
    @task
    async def my_task(key: str = TaskKey()) -> None:
        assert isinstance(key, str)
    ```
    """
    return cast(str, _TaskKey())


class _TaskArgument(Dependency[Any]):
    parameter: str | None
    optional: bool

    def __init__(self, parameter: str | None = None, optional: bool = False) -> None:
        self.parameter = parameter
        self.optional = optional

    async def __aenter__(self) -> Any:
        assert self.parameter is not None
        execution = current_execution.get()
        try:
            return execution.get_argument(self.parameter)
        except KeyError:
            if self.optional:
                return None
            raise


def TaskArgument(parameter: str | None = None, optional: bool = False) -> Any:
    """A dependency to access a argument of the currently executing task.  This is
    often useful in dependency functions so they can access the arguments of the
    task they are injected into.

    Example:

    ```python
    async def customer_name(customer_id: int = TaskArgument()) -> str:
        ...look up the customer's name by ID...
        return "John Doe"

    @task
    async def greet_customer(customer_id: int, name: str = Depends(customer_name)) -> None:
        print(f"Hello, {name}!")
    ```
    """
    return cast(Any, _TaskArgument(parameter, optional))


class _TaskLogger(Dependency["logging.LoggerAdapter[logging.Logger]"]):
    async def __aenter__(self) -> logging.LoggerAdapter[logging.Logger]:
        execution = current_execution.get()
        logger = logging.getLogger(f"docket.task.{execution.function_name}")
        return logging.LoggerAdapter(
            logger,
            {
                **current_docket.get().labels(),
                **current_worker.get().labels(),
                **execution.specific_labels(),
            },
        )


def TaskLogger() -> logging.LoggerAdapter[logging.Logger]:
    """A dependency to access a logger for the currently executing task.  The logger
    will automatically inject contextual information such as the worker and docket
    name, the task key, and the current execution attempt number.

    Example:

    ```python
    @task
    async def my_task(logger: "LoggerAdapter[Logger]" = TaskLogger()) -> None:
        logger.info("Hello, world!")
    ```
    """
    return cast("logging.LoggerAdapter[logging.Logger]", _TaskLogger())
