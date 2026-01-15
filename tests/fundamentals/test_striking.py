"""Tests for strike/restore functionality."""

from unittest.mock import AsyncMock, call

from docket import Docket, Worker
from tests._key_leak_checker import KeyCountChecker


async def test_striking_entire_tasks(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring entire tasks"""

    execution1 = await docket.add(the_task)("a", b="c")
    await docket.add(another_task)("d", e="f")

    # Struck tasks leave runs hash behind (intentional - task might be restored)
    key_leak_checker.add_exemption(docket.runs_key(execution1.key))

    await docket.strike(the_task)

    await worker.run_until_finished()

    the_task.assert_not_called()
    the_task.reset_mock()

    another_task.assert_awaited_once_with("d", e="f")
    another_task.reset_mock()

    await docket.restore(the_task)

    await docket.add(the_task)("g", h="i")
    await docket.add(another_task)("j", k="l")

    await worker.run_until_finished()

    the_task.assert_awaited_once_with("g", h="i")
    another_task.assert_awaited_once_with("j", k="l")


async def test_striking_entire_parameters(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring entire parameters"""

    # Struck tasks remain without TTL so they can be restored
    key_leak_checker.add_pattern_exemption(f"{docket.prefix}:runs:*")

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await docket.strike(None, "customer_id", "==", "789")

    await worker.run_until_finished()

    assert the_task.call_count == 2
    the_task.assert_has_awaits(
        [
            call(customer_id="123", order_id="456"),
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
    )
    another_task.reset_mock()

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await docket.strike(None, "customer_id", "==", "123")

    await worker.run_until_finished()

    assert the_task.call_count == 1
    the_task.assert_has_awaits(
        [
            # customer_id == 123 is stricken
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is stricken
        ],
        any_order=True,
    )
    another_task.reset_mock()

    await docket.restore(None, "customer_id", "==", "123")

    await docket.add(the_task)(customer_id="123", order_id="456")
    await docket.add(the_task)(customer_id="456", order_id="789")
    await docket.add(the_task)(customer_id="789", order_id="012")
    await docket.add(another_task)(customer_id="456", order_id="012")
    await docket.add(another_task)(customer_id="789", order_id="456")

    await worker.run_until_finished()

    assert the_task.call_count == 2
    the_task.assert_has_awaits(
        [
            call(customer_id="123", order_id="456"),
            call(customer_id="456", order_id="789"),
            # customer_id == 789 is still stricken
        ],
        any_order=True,
    )

    assert another_task.call_count == 1
    another_task.assert_has_awaits(
        [
            call(customer_id="456", order_id="012"),
            # customer_id == 789 is still stricken
        ],
        any_order=True,
    )


async def test_striking_tasks_for_specific_parameters(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    another_task: AsyncMock,
    key_leak_checker: KeyCountChecker,
):
    """docket should support striking and restoring tasks for specific parameters"""

    # Struck tasks remain without TTL so they can be restored
    key_leak_checker.add_pattern_exemption(f"{docket.prefix}:runs:*")

    await docket.add(the_task)("a", b=1)
    await docket.add(the_task)("a", b=2)
    await docket.add(the_task)("a", b=3)
    await docket.add(another_task)("d", b=1)
    await docket.add(another_task)("d", b=2)
    await docket.add(another_task)("d", b=3)

    await docket.strike(the_task, "b", "<=", 2)

    await worker.run_until_finished()

    assert the_task.call_count == 1
    the_task.assert_has_awaits(
        [
            # b <= 2 is stricken, so b=1 is out
            # b <= 2 is stricken, so b=2 is out
            call("a", b=3),
        ],
        any_order=True,
    )
    the_task.reset_mock()

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ],
        any_order=True,
    )
    another_task.reset_mock()

    await docket.restore(the_task, "b", "<=", 2)

    await docket.add(the_task)("a", b=1)
    await docket.add(the_task)("a", b=2)
    await docket.add(the_task)("a", b=3)
    await docket.add(another_task)("d", b=1)
    await docket.add(another_task)("d", b=2)
    await docket.add(another_task)("d", b=3)

    await worker.run_until_finished()

    assert the_task.call_count == 3
    the_task.assert_has_awaits(
        [
            call("a", b=1),
            call("a", b=2),
            call("a", b=3),
        ],
        any_order=True,
    )

    assert another_task.call_count == 3
    another_task.assert_has_awaits(
        [
            call("d", b=1),
            call("d", b=2),
            call("d", b=3),
        ],
        any_order=True,
    )
