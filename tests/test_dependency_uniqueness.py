"""Tests for dependency uniqueness constraints (single=True behavior)."""

from datetime import timedelta
from typing import Any, Awaitable, Callable

import pytest

from docket import Docket, Worker
from docket.dependencies import (
    CompletionHandler,
    FailureHandler,
    Perpetual,
    Retry,
    Runtime,
    TaskOutcome,
    Timeout,
)
from docket.execution import Execution


async def test_retries_must_be_unique(docket: Docket, worker: Worker):
    async def the_task(
        a: str,
        retryA: Retry = Retry(attempts=3),
        retryB: Retry = Retry(attempts=5),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match="Only one Retry dependency is allowed per task",
    ):
        await docket.add(the_task)("a")


async def test_runtime_subclasses_must_be_unique(docket: Docket, worker: Worker):
    """Two different Runtime subclasses should conflict since Runtime.single=True."""

    class CustomRuntime(Runtime):
        async def __aenter__(self) -> "CustomRuntime":
            return self  # pragma: no cover

        async def run(
            self,
            execution: Execution,
            function: Callable[..., Awaitable[Any]],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> Any:
            return await function(*args, **kwargs)  # pragma: no cover

    async def the_task(
        a: str,
        timeout: Timeout = Timeout(timedelta(seconds=10)),
        custom: CustomRuntime = CustomRuntime(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one Runtime dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")


async def test_failure_handler_subclasses_must_be_unique(
    docket: Docket, worker: Worker
):
    """Two different FailureHandler subclasses should conflict since FailureHandler.single=True."""

    class CustomFailureHandler(FailureHandler):
        async def __aenter__(self) -> "CustomFailureHandler":
            return self  # pragma: no cover

        async def handle_failure(
            self, execution: Execution, outcome: TaskOutcome
        ) -> bool:
            return False  # pragma: no cover

    async def the_task(
        a: str,
        retry: Retry = Retry(attempts=3),
        custom: CustomFailureHandler = CustomFailureHandler(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one FailureHandler dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")


async def test_completion_handler_subclasses_must_be_unique(
    docket: Docket, worker: Worker
):
    """Two different CompletionHandler subclasses should conflict since CompletionHandler.single=True."""

    class CustomCompletionHandler(CompletionHandler):
        async def __aenter__(self) -> "CustomCompletionHandler":
            return self  # pragma: no cover

        async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
            return False  # pragma: no cover

    async def the_task(
        a: str,
        perpetual: Perpetual = Perpetual(),
        custom: CustomCompletionHandler = CustomCompletionHandler(),
    ):
        pass  # pragma: no cover

    with pytest.raises(
        ValueError,
        match=r"Only one CompletionHandler dependency is allowed per task, but found: .+",
    ):
        await docket.add(the_task)("a")
