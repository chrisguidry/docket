"""Each attempt of a task works with its own copy of the retry the task
declared."""

from datetime import timedelta

from docket import Docket, Retry, Worker
from docket.dependencies import TaskOutcome
from docket.execution import Execution


async def test_forced_retry_delay_applies_to_one_attempt_only(
    docket: Docket, worker: Worker
):
    """The delay a task passes to after() does not change the delay the task
    declared for its later attempts."""

    declared = Retry(attempts=3, delay=timedelta(milliseconds=5))
    delays: list[timedelta] = []

    async def the_task(retry: Retry = declared) -> None:
        delays.append(retry.delay)
        if retry.attempt == 1:
            retry.after(timedelta(milliseconds=10))
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert delays == [timedelta(milliseconds=5)] * 3
    assert declared.delay == timedelta(milliseconds=5)


class CountingRetry(Retry):
    def __init__(self, attempts: int | None = 1) -> None:
        super().__init__(attempts=attempts)
        self.failures = 0

    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        self.failures += 1
        return await super().handle_failure(execution, outcome)


async def test_each_attempt_starts_from_the_declared_state(
    docket: Docket, worker: Worker
):
    """What a subclass records on one attempt's copy is not carried to the next
    attempt, which may run on another worker."""

    failures: list[int] = []

    async def the_task(retry: CountingRetry = CountingRetry(attempts=3)) -> None:
        failures.append(retry.failures)
        raise Exception("Failed")

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert failures == [0, 0, 0]
