import abc
import inspect
import logging
from datetime import timedelta
from typing import Any, Awaitable, Callable, Counter, cast

from .docket import Docket
from .execution import Execution
from .worker import Worker


class Dependency(abc.ABC):
    single: bool = False

    @abc.abstractmethod
    def __call__(
        self, docket: Docket, worker: Worker, execution: Execution
    ) -> Any: ...  # pragma: no cover


class _CurrentWorker(Dependency):
    def __call__(self, docket: Docket, worker: Worker, execution: Execution) -> Worker:
        return worker


def CurrentWorker() -> Worker:
    return cast(Worker, _CurrentWorker())


class _CurrentDocket(Dependency):
    def __call__(self, docket: Docket, worker: Worker, execution: Execution) -> Docket:
        return docket


def CurrentDocket() -> Docket:
    return cast(Docket, _CurrentDocket())


class _CurrentExecution(Dependency):
    def __call__(
        self, docket: Docket, worker: Worker, execution: Execution
    ) -> Execution:
        return execution


def CurrentExecution() -> Execution:
    return cast(Execution, _CurrentExecution())


class _TaskKey(Dependency):
    def __call__(self, docket: Docket, worker: Worker, execution: Execution) -> str:
        return execution.key


def TaskKey() -> str:
    return cast(str, _TaskKey())


class _TaskLogger(Dependency):
    def __call__(
        self, docket: Docket, worker: Worker, execution: Execution
    ) -> logging.LoggerAdapter:
        logger = logging.getLogger(f"docket.task.{execution.key}")

        extra = {
            "task.key": execution.key,
            "task.attempt": execution.attempt,
            "worker.name": worker.name,
            "docket.name": docket.name,
        }

        return logging.LoggerAdapter(logger, extra)


def TaskLogger() -> logging.Logger:
    return cast(logging.Logger, _TaskLogger())


class Retry(Dependency):
    single: bool = True

    def __init__(
        self, attempts: int | None = 1, delay: timedelta = timedelta(0)
    ) -> None:
        self.attempts = attempts
        self.delay = delay
        self.attempt = 1

    def __call__(self, docket: Docket, worker: Worker, execution: Execution) -> "Retry":
        retry = Retry(attempts=self.attempts, delay=self.delay)
        retry.attempt = execution.attempt
        return retry


def get_dependency_parameters(
    function: Callable[..., Awaitable[Any]],
) -> dict[str, Dependency]:
    dependencies: dict[str, Any] = {}

    signature = inspect.signature(function)

    for param_name, param in signature.parameters.items():
        if not isinstance(param.default, Dependency):
            continue

        dependencies[param_name] = param.default

    return dependencies


def validate_dependencies(function: Callable[..., Awaitable[Any]]) -> None:
    parameters = get_dependency_parameters(function)

    counts = Counter(type(dependency) for dependency in parameters.values())

    for dependency_type, count in counts.items():
        if dependency_type.single and count > 1:
            raise ValueError(
                f"Only one {dependency_type.__name__} dependency is allowed per task"
            )
