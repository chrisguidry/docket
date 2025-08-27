from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from docket import Docket
from docket.agenda import Agenda


@pytest.fixture
def agenda() -> Agenda:
    """Create a fresh agenda for testing."""
    return Agenda()


async def test_agenda_creation(agenda: Agenda):
    """Agenda should be created empty."""
    assert len(agenda) == 0
    assert list(agenda) == []


async def test_agenda_add_single_task(agenda: Agenda, the_task: AsyncMock):
    """Should add a single task to the agenda."""
    agenda.add(the_task)("arg1", kwarg1="value1")

    assert len(agenda) == 1
    tasks = list(agenda)
    assert tasks[0][0] == the_task
    assert tasks[0][1] == ("arg1",)
    assert tasks[0][2] == {"kwarg1": "value1"}


async def test_agenda_add_multiple_tasks(
    agenda: Agenda, the_task: AsyncMock, another_task: AsyncMock
):
    """Should add multiple tasks to the agenda."""
    agenda.add(the_task)("arg1")
    agenda.add(another_task)("arg2", key="value")
    agenda.add(the_task)("arg3")

    assert len(agenda) == 3
    tasks = list(agenda)
    assert tasks[0][0] == the_task
    assert tasks[1][0] == another_task
    assert tasks[2][0] == the_task


async def test_agenda_scatter_basic(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should scatter tasks evenly over the specified timeframe."""
    docket.register(the_task)

    # Add 3 tasks to scatter over 60 seconds
    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")
    agenda.add(the_task)("task3")

    start_time = datetime.now(timezone.utc)
    executions = await agenda.scatter(docket, over=timedelta(seconds=60))

    assert len(executions) == 3

    # Tasks should be scheduled at 0, 30, and 60 seconds
    expected_times = [
        start_time,
        start_time + timedelta(seconds=30),
        start_time + timedelta(seconds=60),
    ]

    for execution, expected_time in zip(executions, expected_times):
        # Allow 1 second tolerance for test execution time
        assert abs((execution.when - expected_time).total_seconds()) < 1


async def test_agenda_scatter_with_start_time(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should scatter tasks starting from a future time."""
    docket.register(the_task)

    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    start_time = datetime.now(timezone.utc) + timedelta(minutes=10)
    executions = await agenda.scatter(
        docket, start=start_time, over=timedelta(minutes=20)
    )

    assert len(executions) == 2

    # Tasks should be scheduled at start and start+20min
    assert abs((executions[0].when - start_time).total_seconds()) < 1
    assert (
        abs((executions[1].when - (start_time + timedelta(minutes=20))).total_seconds())
        < 1
    )


async def test_agenda_scatter_with_jitter(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should add random jitter to scheduled times."""
    docket.register(the_task)

    # Add many tasks to verify jitter is applied
    for i in range(5):
        agenda.add(the_task)(f"task{i}")

    start_time = datetime.now(timezone.utc)
    executions = await agenda.scatter(
        docket, over=timedelta(minutes=10), jitter=timedelta(seconds=30)
    )

    assert len(executions) == 5

    # Calculate expected base times (without jitter)
    base_times = [start_time + timedelta(minutes=i * 2.5) for i in range(5)]

    # Each task should be within ±30 seconds of its base time
    for execution, base_time in zip(executions, base_times):
        diff = abs((execution.when - base_time).total_seconds())
        assert diff <= 30


async def test_agenda_scatter_with_large_jitter(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should ensure jittered times never go before start even with large jitter."""
    docket.register(the_task)

    # Add tasks that will be scheduled close to start
    for i in range(3):
        agenda.add(the_task)(f"task{i}")

    start_time = datetime.now(timezone.utc)

    # Use a very large jitter (5 minutes) on a short window (1 minute)
    # This could potentially push times before start without our safety check
    executions = await agenda.scatter(
        docket, start=start_time, over=timedelta(minutes=1), jitter=timedelta(minutes=5)
    )

    assert len(executions) == 3

    # All scheduled times should be at or after start_time
    for execution in executions:
        assert execution.when >= start_time, (
            f"Task scheduled at {execution.when} is before start {start_time}"
        )


async def test_agenda_scatter_single_task(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should handle scattering a single task."""
    docket.register(the_task)

    agenda.add(the_task)("single")

    start_time = datetime.now(timezone.utc)
    executions = await agenda.scatter(docket, over=timedelta(minutes=10))

    assert len(executions) == 1
    # Single task should be scheduled in the middle of the window
    expected_time = start_time + timedelta(minutes=5)
    assert abs((executions[0].when - expected_time).total_seconds()) < 1


async def test_agenda_scatter_empty(docket: Docket, agenda: Agenda):
    """Should handle scattering an empty agenda."""
    executions = await agenda.scatter(docket, over=timedelta(minutes=10))
    assert executions == []


async def test_agenda_scatter_heterogeneous_tasks(
    docket: Docket, agenda: Agenda, the_task: AsyncMock, another_task: AsyncMock
):
    """Should scatter different types of tasks."""
    docket.register(the_task)
    docket.register(another_task)

    agenda.add(the_task)("task1", key="value1")
    agenda.add(another_task)(42, flag=True)
    agenda.add(the_task)("task2")
    agenda.add(another_task)(99)

    executions = await agenda.scatter(docket, over=timedelta(seconds=90))

    assert len(executions) == 4

    # Verify task types are preserved
    assert executions[0].function == the_task
    assert executions[1].function == another_task
    assert executions[2].function == the_task
    assert executions[3].function == another_task

    # Verify arguments are preserved
    assert executions[0].args == ("task1",)
    assert executions[0].kwargs == {"key": "value1"}
    assert executions[1].args == (42,)
    assert executions[1].kwargs == {"flag": True}


async def test_agenda_scatter_preserves_order(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should preserve task order when scattering."""
    docket.register(the_task)

    for i in range(10):
        agenda.add(the_task)(f"task{i}")

    executions = await agenda.scatter(docket, over=timedelta(minutes=10))

    assert len(executions) == 10

    # Tasks should be scheduled in the same order they were added
    for i, execution in enumerate(executions):
        assert execution.args == (f"task{i}",)

    # And times should be monotonically increasing
    for i in range(1, len(executions)):
        assert executions[i].when >= executions[i - 1].when


async def test_agenda_reusability(docket: Docket, agenda: Agenda, the_task: AsyncMock):
    """Agenda should be reusable for multiple scatter operations."""
    docket.register(the_task)

    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    # First scatter
    executions1 = await agenda.scatter(docket, over=timedelta(seconds=60))
    assert len(executions1) == 2

    # Second scatter with different timing
    start_time = datetime.now(timezone.utc) + timedelta(hours=1)
    executions2 = await agenda.scatter(
        docket, start=start_time, over=timedelta(minutes=30)
    )
    assert len(executions2) == 2

    # Executions should be different instances with different times
    assert executions1[0].when != executions2[0].when
    assert executions1[1].when != executions2[1].when


async def test_agenda_scatter_requires_over_parameter(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should raise error if 'over' parameter is not provided."""
    docket.register(the_task)

    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'over'"
    ):
        await agenda.scatter(docket)  # type: ignore[call-arg]


async def test_agenda_scatter_with_task_by_name(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should support adding tasks by name."""
    docket.register(the_task)

    # Add task by its registered name
    agenda.add("the_task")("arg1", key="value")

    executions = await agenda.scatter(docket, over=timedelta(seconds=60))

    assert len(executions) == 1
    assert executions[0].function == the_task
    assert executions[0].args == ("arg1",)
    assert executions[0].kwargs == {"key": "value"}


async def test_agenda_scatter_with_non_positive_over_parameter(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should raise ValueError if 'over' parameter is not positive."""
    docket.register(the_task)

    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    # Test with zero duration
    with pytest.raises(
        ValueError, match="'over' parameter must be a positive duration"
    ):
        await agenda.scatter(docket, over=timedelta(seconds=0))

    # Test with negative duration
    with pytest.raises(
        ValueError, match="'over' parameter must be a positive duration"
    ):
        await agenda.scatter(docket, over=timedelta(seconds=-60))


async def test_agenda_scatter_partial_scheduling_behavior(
    docket: Docket, agenda: Agenda, the_task: AsyncMock, another_task: AsyncMock
):
    """Documents the partial scheduling behavior when failures occur."""
    docket.register(the_task)
    # Don't register another_task initially

    # Test validation failure - unregistered task fails fast before any scheduling
    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")
    agenda.add("unregistered_task")("will_fail")  # This will fail validation
    agenda.add(the_task)("task3")

    # The scatter should fail during validation before scheduling anything
    with pytest.raises(KeyError, match="Task 'unregistered_task' is not registered"):
        await agenda.scatter(docket, over=timedelta(seconds=60))

    # Verify no tasks were scheduled (failed during validation)
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0

    # Test successful case with all registered tasks
    agenda2 = Agenda()
    docket.register(another_task)

    agenda2.add(the_task)("task1")
    agenda2.add(the_task)("task2")
    agenda2.add(another_task)("task3")
    agenda2.add(the_task)("task4")

    # All tasks should be scheduled successfully
    executions = await agenda2.scatter(docket, over=timedelta(seconds=60))
    assert len(executions) == 4

    # Verify all tasks are in the docket
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 4

    # Clear for next test
    await docket.clear()

    # Test partial failure during scheduling - earlier tasks remain scheduled
    agenda3 = Agenda()
    agenda3.add(the_task)("task1")
    agenda3.add(the_task)("task2")
    agenda3.add(the_task)("task3")

    call_count = 0
    original_add = docket.add

    def failing_add(*args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            # Fail on the second task
            raise RuntimeError("Simulated scheduling failure")
        return original_add(*args, **kwargs)

    with patch.object(docket, "add", side_effect=failing_add):
        with pytest.raises(RuntimeError, match="Simulated scheduling failure"):
            await agenda3.scatter(docket, over=timedelta(seconds=60))

    # The first task should have been scheduled successfully before the failure
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 1  # First task was scheduled


async def test_agenda_scatter_auto_registers_unregistered_functions(
    docket: Docket, agenda: Agenda, the_task: AsyncMock
):
    """Should automatically register task functions that aren't already registered."""
    # the_task is NOT registered yet
    assert the_task not in docket.tasks.values()

    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    # scatter should auto-register the task
    executions = await agenda.scatter(docket, over=timedelta(seconds=30))
    assert len(executions) == 2

    # Now the task should be registered
    assert the_task in docket.tasks.values()

    # Verify tasks were scheduled
    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 2


async def test_agenda_clear(agenda: Agenda, the_task: AsyncMock):
    """Should support clearing all tasks from agenda."""
    agenda.add(the_task)("task1")
    agenda.add(the_task)("task2")

    assert len(agenda) == 2

    agenda.clear()

    assert len(agenda) == 0
    assert list(agenda) == []
