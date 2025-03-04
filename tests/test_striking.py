import asyncio
from datetime import datetime
from typing import Any, Callable

import pytest

from docket import Docket
from docket.execution import Execution, Operator, Strike, StrikeList


async def test_all_dockets_see_all_strikes(docket: Docket):
    async with (
        Docket(docket.name, docket.url) as docket_a,
        Docket(docket.name, docket.url) as docket_b,
        Docket(docket.name, docket.url) as docket_c,
    ):
        await docket.strike("test_task")

        await asyncio.sleep(0.25)

        assert "test_task" in docket_a.strike_list.task_strikes
        assert "test_task" in docket_b.strike_list.task_strikes
        assert "test_task" in docket_c.strike_list.task_strikes

        await docket_a.restore("test_task")

        await asyncio.sleep(0.25)

        assert "test_task" not in docket_a.strike_list.task_strikes
        assert "test_task" not in docket_b.strike_list.task_strikes
        assert "test_task" not in docket_c.strike_list.task_strikes


async def test_striking_is_idempotent(docket: Docket):
    for _ in range(3):
        await docket.strike("test_task")
        await docket.strike("test_task", "customer", "==", "123")
        await docket.strike("test_task", "customer", "<=", "234")
        await docket.strike(None, "customer", "==", "345")
        await docket.strike(None, "customer", "<=", "456")

    assert docket.strike_list.task_strikes["test_task"]["customer"] == {
        ("==", "123"),
        ("<=", "234"),
    }
    assert docket.strike_list.parameter_strikes["customer"] == {
        ("==", "345"),
        ("<=", "456"),
    }


async def test_restoring_is_idempotent(docket: Docket):
    """Covers all of the ways that we can restore a strike and all the possible
    code paths to show that these are all idempotent."""

    await docket.strike("test_task")
    await docket.strike("test_task", "customer", "==", "123")
    await docket.strike("test_task", "customer", "<=", "234")
    await docket.strike("another_task", "customer", "==", "123")
    await docket.strike("another_task", "order", "==", "987")
    await docket.strike("yet_another", "order", "==", "987")
    await docket.strike("yet_another", "order", "==", "keep-me")
    await docket.strike(None, "customer", "==", "345")
    await docket.strike(None, "customer", "<=", "456")
    await docket.strike(None, "order", "==", "789")

    assert docket.strike_list.task_strikes["test_task"]["customer"] == {
        ("==", "123"),
        ("<=", "234"),
    }
    assert docket.strike_list.task_strikes["another_task"]["customer"] == {
        ("==", "123"),
    }
    assert docket.strike_list.task_strikes["another_task"]["order"] == {
        ("==", "987"),
    }
    assert docket.strike_list.task_strikes["yet_another"]["order"] == {
        ("==", "987"),
        ("==", "keep-me"),
    }
    assert docket.strike_list.parameter_strikes["customer"] == {
        ("==", "345"),
        ("<=", "456"),
    }
    assert docket.strike_list.parameter_strikes["order"] == {
        ("==", "789"),
    }

    for _ in range(3):
        await docket.restore("test_task")
        await docket.restore("test_task", "customer", "==", "123")
        await docket.restore("another_task", "customer", "==", "123")
        await docket.restore("another_task", "order", "==", "987")
        await docket.restore("yet_another", "order", "==", "987")
        await docket.restore(None, "customer", "==", "345")
        await docket.restore(None, "order", "==", "789")
        await docket.restore("test_task", "nonexistent_param", "==", "value")
        await docket.restore("another_task", "nonexistent_param", "==", "value")
        await docket.restore(None, "nonexistent_param", "==", "value")

    assert docket.strike_list.task_strikes["test_task"]["customer"] == {
        ("<=", "234"),
    }
    assert "another_task" not in docket.strike_list.task_strikes
    assert docket.strike_list.parameter_strikes["customer"] == {
        ("<=", "456"),
    }
    assert docket.strike_list.task_strikes["yet_another"]["order"] == {
        ("==", "keep-me"),
    }
    assert "order" not in docket.strike_list.parameter_strikes


@pytest.mark.parametrize(
    "operator,value,test_value,expected_result",
    [
        ("==", 42, 42, True),
        ("==", 42, 43, False),
        ("!=", 42, 43, True),
        ("!=", 42, 42, False),
        (">", 42, 43, True),
        (">", 42, 42, False),
        (">", 42, 41, False),
        (">=", 42, 43, True),
        (">=", 42, 42, True),
        (">=", 42, 41, False),
        ("<", 42, 41, True),
        ("<", 42, 42, False),
        ("<", 42, 43, False),
        ("<=", 42, 41, True),
        ("<=", 42, 42, True),
        ("<=", 42, 43, False),
        ("between", (10, 50), 30, True),
        ("between", (10, 50), 10, True),
        ("between", (10, 50), 50, True),
        ("between", (10, 50), 5, False),
        ("between", (10, 50), 55, False),
        ("between", "not a tuple", 30, False),
        ("between", (10, 20, 100), 30, False),  # too many values
    ],
)
def test_strike_operators(
    operator: Operator,
    value: Any,
    test_value: Any,
    expected_result: bool,
    now: Callable[[], datetime],
) -> None:
    """Should correctly evaluate all supported strike operators."""
    strike_list = StrikeList()

    strike = Strike(None, "the_parameter", operator, value)
    strike_list.update(strike)

    async def test_function(the_parameter: Any) -> None:
        pass  # pragma: no cover

    execution = Execution(
        function=test_function,
        args=(),
        kwargs={"the_parameter": test_value},
        when=now(),
        key="test-key",
        attempt=1,
    )

    assert strike_list.is_stricken(execution) == expected_result


@pytest.mark.parametrize(
    "operator,value,test_value",
    [
        (">", 42, "string"),  # comparing int with string
        ("<", "string", 42),  # comparing string with int
        (">=", None, 42),  # comparing None with int
        ("<=", 42, None),  # comparing int with None
        (">", {}, 42),  # comparing dict with int
        ("<", 42, {}),  # comparing int with dict
        (">=", [], 42),  # comparing list with int
        ("<=", 42, []),  # comparing int with list
    ],
)
async def test_strike_incomparable_values(
    operator: Operator,
    value: Any,
    test_value: Any,
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
):
    """should handle incomparable values gracefully in strikes"""

    # Register a test task
    async def test_task(parameter: Any) -> None:
        pass  # pragma: no cover

    docket.register(test_task)

    # Create a strike with potentially incomparable values
    await docket.strike("test_task", "parameter", operator, value)

    # We should be able to add the task without errors, even if the strike would be
    # comparing incomparable values
    execution = await docket.add(test_task)(test_value)

    # The task might or might not be stricken depending on the implementation's
    # handling of incomparable values, but the operation shouldn't raise exceptions
    assert execution is not None  # Simply access the variable to satisfy the linter

    assert "Incompatible type for strike condition" in caplog.text
