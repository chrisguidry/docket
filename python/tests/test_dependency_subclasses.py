"""Subclasses of docket's behavioral dependencies reach the task and the
worker as the subclass, with the state their constructors set."""

from datetime import timedelta
from typing import Any, Awaitable, Callable

import pytest

from docket import (
    ConcurrencyLimit,
    Cooldown,
    Cron,
    Debounce,
    Docket,
    ExponentialRetry,
    Perpetual,
    RateLimit,
    Retry,
    Timeout,
    Worker,
)
from docket.dependencies import Dependency, TaskOutcome
from docket.execution import Execution


class LabeledPerpetual(Perpetual):
    def __init__(self, label: str) -> None:
        super().__init__(every=timedelta(hours=1))
        self.label = label


class LabeledCron(Cron):
    def __init__(self, label: str) -> None:
        super().__init__("@yearly", automatic=False)
        self.label = label


class LabeledTimeout(Timeout):
    def __init__(self, label: str) -> None:
        super().__init__(timedelta(seconds=10))
        self.label = label


class LabeledConcurrencyLimit(ConcurrencyLimit):
    def __init__(self, label: str) -> None:
        super().__init__(max_concurrent=1)
        self.label = label


@pytest.mark.parametrize(
    "declared",
    [
        LabeledPerpetual("billing"),
        LabeledCron("billing"),
        LabeledTimeout("billing"),
        LabeledConcurrencyLimit("billing"),
    ],
)
async def test_task_receives_subclass_with_its_own_arguments(
    docket: Docket, worker: Worker, declared: Any
):
    """A task receives the subclass it declared, including state from a
    constructor that takes different arguments than the base class."""

    received: list[tuple[bool, str]] = []

    async def the_task(dependency: Any = declared) -> None:
        received.append((type(dependency) is type(declared), dependency.label))

    execution = await docket.add(the_task)()

    await worker.run_at_most({execution.key: 1})

    assert received == [(True, "billing")]


class PerpetualOnce(Perpetual):
    async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


class CronOnce(Cron):
    async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


@pytest.mark.parametrize(
    "declared",
    [
        PerpetualOnce(every=timedelta(milliseconds=10)),
        CronOnce("* * * * * *", automatic=False),
    ],
)
async def test_subclass_decides_whether_to_run_again(
    docket: Docket, worker: Worker, declared: Perpetual
):
    """A subclass's on_complete decides whether the task is rescheduled."""

    calls = 0

    async def the_task(perpetual: Perpetual = declared) -> None:
        nonlocal calls
        calls += 1

    execution = await docket.add(the_task)()

    await worker.run_at_most({execution.key: 2})

    assert calls == 1


class RecordingTimeout(Timeout):
    def __init__(self, base: timedelta, ran: list[str]) -> None:
        super().__init__(base)
        self.ran = ran

    async def run(
        self,
        execution: Execution,
        function: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        self.ran.append(execution.key)
        return await super().run(execution, function, args, kwargs)


async def test_timeout_subclass_runs_the_task(docket: Docket, worker: Worker):
    """A Timeout subclass's run is what executes the task."""

    ran: list[str] = []

    async def the_task(
        timeout: Timeout = RecordingTimeout(timedelta(seconds=10), ran),
    ) -> None: ...

    execution = await docket.add(the_task)()

    await worker.run_until_finished()

    assert ran == [execution.key]


class LabeledCooldown(Cooldown):
    def __init__(self, label: str) -> None:
        super().__init__(timedelta(seconds=30))
        self.label = label


class LabeledRateLimit(RateLimit):
    def __init__(self, label: str) -> None:
        super().__init__(5, per=timedelta(minutes=1))
        self.label = label


class LabeledDebounce(Debounce):
    def __init__(self, label: str) -> None:
        super().__init__(timedelta(seconds=5))
        self.label = label


@pytest.mark.parametrize(
    "declared",
    [
        LabeledConcurrencyLimit("billing"),
        LabeledCooldown("billing"),
        LabeledRateLimit("billing"),
        LabeledDebounce("billing"),
    ],
)
def test_annotated_dependency_binds_as_its_subclass(declared: Dependency[Any]):
    """A dependency in Annotated metadata is bound to its parameter as the
    subclass that was declared, with its state."""

    bound: Any = declared.bind_to_parameter("customer_id", 1)

    assert (type(bound), bound.label) == (type(declared), "billing")


class NeverRetry(Retry):
    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


class NeverExponentialRetry(ExponentialRetry):
    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        return False


@pytest.mark.parametrize(
    "retry",
    [NeverRetry(attempts=3), NeverExponentialRetry(attempts=3)],
)
async def test_retry_subclass_decides_whether_to_retry(
    docket: Docket, worker: Worker, retry: Retry
):
    """A subclass's handle_failure decides whether a failed task is retried."""

    calls = 0

    async def the_task(retry: Retry = retry) -> None:
        nonlocal calls
        calls += 1
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert calls == 1


class LabeledRetry(Retry):
    def __init__(self, label: str, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts)
        self.label = label


class LabeledExponentialRetry(ExponentialRetry):
    def __init__(self, label: str, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts, minimum_delay=timedelta(0))
        self.label = label


@pytest.mark.parametrize(
    "retry",
    [
        LabeledRetry("billing", attempts=2),
        LabeledExponentialRetry("billing", attempts=2),
    ],
)
async def test_task_receives_retry_subclass_with_its_own_arguments(
    docket: Docket, worker: Worker, retry: LabeledRetry | LabeledExponentialRetry
):
    """A task receives its retry subclass, including state from a constructor
    that takes different arguments than the base class."""

    received: list[tuple[type[Retry], str, int]] = []

    async def the_task(
        retry: LabeledRetry | LabeledExponentialRetry = retry,
    ) -> None:
        received.append((type(retry), retry.label, retry.attempt))
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert received == [(type(retry), "billing", 1), (type(retry), "billing", 2)]


def test_forever_returns_the_subclass():
    """forever() on a subclass is typed as, and returns, that subclass."""
    retry: NeverRetry = NeverRetry.forever()
    exponential: NeverExponentialRetry = NeverExponentialRetry.forever()

    assert (type(retry), type(exponential)) == (NeverRetry, NeverExponentialRetry)
