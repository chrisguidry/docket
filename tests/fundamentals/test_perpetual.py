"""Tests for Perpetual dependency (automatically rescheduling tasks)."""

from datetime import datetime, timedelta, timezone

from docket import Docket, Perpetual, Worker


async def test_perpetual_tasks(docket: Docket, worker: Worker):
    """Perpetual tasks should reschedule themselves forever"""

    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        assert a == "a"
        assert b == 2

        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

    execution = await docket.add(perpetual_task)(a="a", b=2)

    await worker.run_at_most({execution.key: 3})

    assert calls == 3


async def test_perpetual_tasks_can_cancel_themselves(docket: Docket, worker: Worker):
    """A perpetual task can request its own cancellation"""
    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        assert a == "a"
        assert b == 2

        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

        if calls == 3:
            perpetual.cancel()

    await docket.add(perpetual_task)(a="a", b=2)

    await worker.run_until_finished()

    assert calls == 3


async def test_perpetual_tasks_can_change_their_parameters(
    docket: Docket, worker: Worker
):
    """Perpetual tasks may change their parameters each time"""
    arguments: list[tuple[str, int]] = []

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        arguments.append((a, b))
        perpetual.perpetuate(a + "a", b=b + 1)

    execution = await docket.add(perpetual_task)(a="a", b=1)

    await worker.run_at_most({execution.key: 3})

    assert len(arguments) == 3
    assert arguments == [("a", 1), ("aa", 2), ("aaa", 3)]


async def test_perpetual_tasks_perpetuate_even_after_errors(
    docket: Docket, worker: Worker
):
    """Perpetual tasks keep rescheduling even when they raise exceptions."""
    calls = 0

    async def perpetual_task(
        a: str,
        b: int,
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=50)),
    ):
        nonlocal calls
        calls += 1

        raise ValueError("woops!")

    execution = await docket.add(perpetual_task)(a="a", b=1)

    await worker.run_at_most({execution.key: 3})

    assert calls == 3


async def test_perpetual_tasks_can_be_automatically_scheduled(
    docket: Docket, worker: Worker
):
    """Perpetual tasks can be automatically scheduled"""

    calls = 0

    async def my_automatic_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=50), automatic=True
        ),
    ):
        assert isinstance(perpetual, Perpetual)

        assert perpetual.every == timedelta(milliseconds=50)

        nonlocal calls
        calls += 1

    # Note we never add this task to the docket, we just register it.
    docket.register(my_automatic_task)

    # The automatic key will be the task function's name
    await worker.run_at_most({"my_automatic_task": 3})

    assert calls == 3


async def test_perpetual_tasks_can_schedule_next_run_after_delay(
    docket: Docket, worker: Worker
):
    """Perpetual.after() lets tasks control when the next run happens."""
    run_times: list[datetime] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(),
    ):
        run_times.append(datetime.now(timezone.utc))
        perpetual.after(timedelta(milliseconds=100))

    execution = await docket.add(perpetual_task)()

    await worker.run_at_most({execution.key: 2})

    assert len(run_times) == 2
    delay = run_times[1] - run_times[0]
    assert delay >= timedelta(milliseconds=50)


async def test_perpetual_tasks_can_schedule_next_run_at_specific_time(
    docket: Docket, worker: Worker
):
    """Perpetual.at() lets tasks schedule the next run at an absolute time."""
    run_times: list[datetime] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(),
    ):
        run_times.append(datetime.now(timezone.utc))
        perpetual.at(datetime.now(timezone.utc) + timedelta(milliseconds=100))

    execution = await docket.add(perpetual_task)()

    await worker.run_at_most({execution.key: 2})

    assert len(run_times) == 2
    delay = run_times[1] - run_times[0]
    assert delay >= timedelta(milliseconds=50)
