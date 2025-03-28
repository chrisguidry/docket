import abc
import logging
import time
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Counter,
    Generic,
    TypeVar,
    cast,
)

from .docket import Docket
from .execution import Execution, TaskFunction, get_signature

if TYPE_CHECKING:  # pragma: no cover
    from .worker import Worker


class Dependency(abc.ABC):
    single: bool = False

    docket: ContextVar[Docket] = ContextVar("docket")
    worker: ContextVar["Worker"] = ContextVar("worker")
    execution: ContextVar[Execution] = ContextVar("execution")

    @abc.abstractmethod
    async def __aenter__(self) -> Any: ...  # pragma: no cover

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool: ...  # pragma: no cover


class _CurrentWorker(Dependency):
    async def __aenter__(self) -> "Worker":
        return self.worker.get()


def CurrentWorker() -> "Worker":
    return cast("Worker", _CurrentWorker())


class _CurrentDocket(Dependency):
    async def __aenter__(self) -> Docket:
        return self.docket.get()


def CurrentDocket() -> Docket:
    return cast(Docket, _CurrentDocket())


class _CurrentExecution(Dependency):
    async def __aenter__(self) -> Execution:
        return self.execution.get()


def CurrentExecution() -> Execution:
    return cast(Execution, _CurrentExecution())


class _TaskKey(Dependency):
    async def __aenter__(self) -> str:
        return self.execution.get().key


def TaskKey() -> str:
    return cast(str, _TaskKey())


class _TaskArgument(Dependency):
    parameter: str | None

    def __init__(self, parameter: str | None = None) -> None:
        self.parameter = parameter

    async def __aenter__(self) -> Any:
        assert self.parameter is not None
        execution = self.execution.get()
        return execution.get_argument(self.parameter)


def TaskArgument(parameter: str | None = None) -> Any:
    return cast(Any, _TaskArgument(parameter))


class _TaskLogger(Dependency):
    async def __aenter__(self) -> logging.LoggerAdapter[logging.Logger]:
        execution = self.execution.get()
        logger = logging.getLogger(f"docket.task.{execution.function.__name__}")
        return logging.LoggerAdapter(
            logger,
            {
                **self.docket.get().labels(),
                **self.worker.get().labels(),
                **execution.specific_labels(),
            },
        )


def TaskLogger() -> logging.LoggerAdapter[logging.Logger]:
    return cast(logging.LoggerAdapter[logging.Logger], _TaskLogger())


class Retry(Dependency):
    single: bool = True

    def __init__(
        self, attempts: int | None = 1, delay: timedelta = timedelta(0)
    ) -> None:
        self.attempts = attempts
        self.delay = delay
        self.attempt = 1

    async def __aenter__(self) -> "Retry":
        execution = self.execution.get()
        retry = Retry(attempts=self.attempts, delay=self.delay)
        retry.attempt = execution.attempt
        return retry


class ExponentialRetry(Retry):
    attempts: int

    def __init__(
        self,
        attempts: int = 1,
        minimum_delay: timedelta = timedelta(seconds=1),
        maximum_delay: timedelta = timedelta(seconds=64),
    ) -> None:
        super().__init__(attempts=attempts, delay=minimum_delay)
        self.minimum_delay = minimum_delay
        self.maximum_delay = maximum_delay

    async def __aenter__(self) -> "ExponentialRetry":
        execution = self.execution.get()

        retry = ExponentialRetry(
            attempts=self.attempts,
            minimum_delay=self.minimum_delay,
            maximum_delay=self.maximum_delay,
        )
        retry.attempt = execution.attempt

        if execution.attempt > 1:
            backoff_factor = 2 ** (execution.attempt - 1)
            calculated_delay = self.minimum_delay * backoff_factor

            if calculated_delay > self.maximum_delay:
                retry.delay = self.maximum_delay
            else:
                retry.delay = calculated_delay

        return retry


class Perpetual(Dependency):
    single = True

    every: timedelta
    automatic: bool

    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    cancelled: bool

    def __init__(
        self,
        every: timedelta = timedelta(0),
        automatic: bool = False,
    ) -> None:
        """Declare a task that should be run perpetually.

        Args:
            every: The target interval between task executions.
            automatic: If set, this task will be automatically scheduled during worker
                startup and continually through the worker's lifespan.  This ensures
                that the task will always be scheduled despite crashes and other
                adverse conditions.  Automatic tasks must not require any arguments.
        """
        self.every = every
        self.automatic = automatic
        self.cancelled = False

    async def __aenter__(self) -> "Perpetual":
        execution = self.execution.get()
        perpetual = Perpetual(every=self.every)
        perpetual.args = execution.args
        perpetual.kwargs = execution.kwargs
        return perpetual

    def cancel(self) -> None:
        self.cancelled = True

    def perpetuate(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


class Timeout(Dependency):
    single = True

    base: timedelta

    _deadline: float

    def __init__(self, base: timedelta) -> None:
        self.base = base

    async def __aenter__(self) -> "Timeout":
        timeout = Timeout(base=self.base)
        timeout.start()
        return timeout

    def start(self) -> None:
        self._deadline = time.monotonic() + self.base.total_seconds()

    def expired(self) -> bool:
        return time.monotonic() >= self._deadline

    def remaining(self) -> timedelta:
        return timedelta(seconds=self._deadline - time.monotonic())

    def extend(self, by: timedelta | None = None) -> None:
        if by is None:
            by = self.base
        self._deadline += by.total_seconds()


R = TypeVar("R")

DependencyFunction = Callable[..., Awaitable[R] | AsyncContextManager[R]]


_parameter_cache: dict[
    TaskFunction | DependencyFunction[Any],
    dict[str, Dependency],
] = {}


def get_dependency_parameters(
    function: TaskFunction | DependencyFunction[Any],
) -> dict[str, Dependency]:
    if function in _parameter_cache:
        return _parameter_cache[function]

    dependencies: dict[str, Dependency] = {}

    signature = get_signature(function)

    for parameter, param in signature.parameters.items():
        if not isinstance(param.default, Dependency):
            continue

        dependencies[parameter] = param.default

    _parameter_cache[function] = dependencies
    return dependencies


class _Depends(Dependency, Generic[R]):
    dependency: DependencyFunction[R]

    cache: ContextVar[dict[DependencyFunction[Any], Any]] = ContextVar("cache")
    stack: ContextVar[AsyncExitStack] = ContextVar("stack")

    def __init__(
        self, dependency: Callable[[], Awaitable[R] | AsyncContextManager[R]]
    ) -> None:
        self.dependency = dependency

    async def _resolve_parameters(
        self,
        function: TaskFunction | DependencyFunction[Any],
    ) -> dict[str, Any]:
        stack = self.stack.get()

        arguments: dict[str, Any] = {}
        parameters = get_dependency_parameters(function)

        for parameter, dependency in parameters.items():
            # Special case for TaskArguments, they are "magical" and infer the parameter
            # they refer to from the parameter name (unless otherwise specified)
            if isinstance(dependency, _TaskArgument) and not dependency.parameter:
                dependency.parameter = parameter

            arguments[parameter] = await stack.enter_async_context(dependency)

        return arguments

    async def __aenter__(self) -> R:
        cache = self.cache.get()

        if self.dependency in cache:
            return cache[self.dependency]

        stack = self.stack.get()
        arguments = await self._resolve_parameters(self.dependency)

        value = self.dependency(**arguments)

        if isinstance(value, AsyncContextManager):
            value = await stack.enter_async_context(value)
        else:
            value = await value

        cache[self.dependency] = value
        return value


def Depends(dependency: DependencyFunction[R]) -> R:
    return cast(R, _Depends(dependency))


D = TypeVar("D", bound=Dependency)


def get_single_dependency_parameter_of_type(
    function: TaskFunction, dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in get_dependency_parameters(function).items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


def get_single_dependency_of_type(
    dependencies: dict[str, Dependency], dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in dependencies.items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


def validate_dependencies(function: TaskFunction) -> None:
    parameters = get_dependency_parameters(function)

    counts = Counter(type(dependency) for dependency in parameters.values())

    for dependency_type, count in counts.items():
        if dependency_type.single and count > 1:
            raise ValueError(
                f"Only one {dependency_type.__name__} dependency is allowed per task"
            )


class FailedDependency:
    def __init__(self, parameter: str, error: Exception) -> None:
        self.parameter = parameter
        self.error = error


@asynccontextmanager
async def resolved_dependencies(
    worker: "Worker", execution: Execution
) -> AsyncGenerator[dict[str, Any], None]:
    # Set context variables once at the beginning
    Dependency.docket.set(worker.docket)
    Dependency.worker.set(worker)
    Dependency.execution.set(execution)

    _Depends.cache.set({})

    async with AsyncExitStack() as stack:
        _Depends.stack.set(stack)

        arguments: dict[str, Any] = {}

        parameters = get_dependency_parameters(execution.function)
        for parameter, dependency in parameters.items():
            kwargs = execution.kwargs
            if parameter in kwargs:
                arguments[parameter] = kwargs[parameter]
                continue

            # Special case for TaskArguments, they are "magical" and infer the parameter
            # they refer to from the parameter name (unless otherwise specified).  At
            # the top-level task function call, it doesn't make sense to specify one
            # _without_ a parameter name, so we'll call that a failed dependency.
            if isinstance(dependency, _TaskArgument) and not dependency.parameter:
                arguments[parameter] = FailedDependency(
                    parameter, ValueError("No parameter name specified")
                )
                continue

            try:
                arguments[parameter] = await stack.enter_async_context(dependency)
            except Exception as error:
                arguments[parameter] = FailedDependency(parameter, error)

        yield arguments
