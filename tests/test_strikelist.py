"""Tests for StrikeList."""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import asyncio
from typing import TYPE_CHECKING, Any, Callable

import pytest

from docket import StrikeList
from docket.strikelist import Operator, Restore, Strike

if TYPE_CHECKING:
    from docket.execution import Execution


@pytest.fixture
def strike_name(make_docket_name: Callable[[], str]) -> str:
    """Unique name for each test to avoid collisions.

    Uses make_docket_name to ensure ACL compatibility when running
    with Redis ACL enabled.
    """
    return make_docket_name()


async def send_strike(
    strikes: StrikeList,
    parameter: str,
    operator: str,
    value: Any,
) -> None:
    """Send a strike instruction directly to Redis."""
    instruction = Strike(None, parameter, Operator(operator), value)
    assert strikes._redis is not None
    async with strikes._redis.client() as r:
        await r.xadd(strikes.strike_key, instruction.as_message())  # type: ignore[arg-type]


async def send_restore(
    strikes: StrikeList,
    parameter: str,
    operator: str,
    value: Any,
) -> None:
    """Send a restore instruction directly to Redis."""
    instruction = Restore(None, parameter, Operator(operator), value)
    assert strikes._redis is not None
    async with strikes._redis.client() as r:
        await r.xadd(strikes.strike_key, instruction.as_message())  # type: ignore[arg-type]


# Basic functionality tests


async def test_context_manager(redis_url: str, strike_name: str):
    """Test async context manager works correctly."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        # Should be connected
        assert strikes._redis is not None
        assert strikes._redis.is_connected
        assert strikes._monitor_task is not None

    # After exit, resources should be cleaned up (connection closed, not deleted)
    assert strikes._redis is not None
    assert not strikes._redis.is_connected
    assert strikes._monitor_task is None


async def test_explicit_aenter_aexit(redis_url: str, strike_name: str):
    """Test explicit __aenter__/__aexit__ lifecycle."""
    strikes = StrikeList(url=redis_url, name=strike_name)

    # _redis is created in __init__, but not yet connected
    assert strikes._redis is not None
    assert not strikes._redis.is_connected
    assert strikes._monitor_task is None

    # Connect via __aenter__
    await strikes.__aenter__()
    assert strikes._redis is not None
    assert strikes._redis.is_connected
    assert strikes._monitor_task is not None

    # Close via __aexit__
    await strikes.__aexit__(None, None, None)
    assert strikes._redis is not None
    assert not strikes._redis.is_connected
    assert strikes._monitor_task is None


async def test_aenter_is_not_reentrant(redis_url: str, strike_name: str):
    """Test that calling __aenter__ twice raises an assertion error."""
    strikes = StrikeList(url=redis_url, name=strike_name)

    await strikes.__aenter__()

    # Second __aenter__ should raise
    with pytest.raises(AssertionError, match="not reentrant"):
        await strikes.__aenter__()

    await strikes.__aexit__(None, None, None)


async def test_context_manager_reuse(redis_url: str, strike_name: str):
    """Test that a StrikeList can be entered and exited multiple times."""
    strikes = StrikeList(url=redis_url, name=strike_name)

    await strikes.__aenter__()
    await strikes.__aexit__(None, None, None)

    # Can enter again after proper exit
    await strikes.__aenter__()
    await strikes.__aexit__(None, None, None)


async def test_prefix_property(redis_url: str, strike_name: str):
    """Test the prefix property returns the name (hash-tagged for cluster mode)."""
    from docket._redis import RedisConnection

    strikes = StrikeList(url=redis_url, name=strike_name)
    if RedisConnection(redis_url).is_cluster:  # pragma: no cover
        assert strikes.prefix == f"{{{strike_name}}}"
    else:  # pragma: no cover
        assert strikes.prefix == strike_name


async def test_strike_key_property(redis_url: str, strike_name: str):
    """Test the strike_key property uses prefix."""
    strikes = StrikeList(url=redis_url, name=strike_name)
    assert strikes.strike_key == f"{strikes.prefix}:strikes"


async def test_local_only_mode(strike_name: str):
    """Test StrikeList works without Redis (local-only mode)."""
    # No URL = local-only mode
    strikes = StrikeList(name=strike_name)
    await strikes.__aenter__()  # Should be a no-op

    assert strikes._redis is None
    assert strikes._monitor_task is None
    assert strikes._strikes_loaded is None

    # wait_for_strikes_loaded returns immediately in local-only mode
    await strikes.wait_for_strikes_loaded()

    # Can still use locally
    strikes.update(Strike(None, "customer_id", Operator.EQUAL, "blocked"))
    assert strikes.is_stricken({"customer_id": "blocked"})
    assert not strikes.is_stricken({"customer_id": "allowed"})

    await strikes.__aexit__(None, None, None)


async def test_memory_url_without_fakeredis(
    strike_name: str, monkeypatch: pytest.MonkeyPatch
):
    """Test error when memory:// used without fakeredis."""
    import sys

    # Temporarily make fakeredis unimportable
    monkeypatch.setitem(sys.modules, "fakeredis", None)
    monkeypatch.setitem(sys.modules, "fakeredis.aioredis", None)

    strikes = StrikeList(url="memory://", name=strike_name)
    with pytest.raises((ImportError, ModuleNotFoundError), match="fakeredis"):
        await strikes.__aenter__()


async def test_send_instruction_requires_connection(strike_name: str):
    """Test that send_instruction raises error when not connected."""
    strikes = StrikeList(url="memory://", name=strike_name)
    instruction = Strike(None, "customer_id", Operator.EQUAL, "blocked")

    with pytest.raises(RuntimeError, match="not connected to Redis"):
        await strikes.send_instruction(instruction)


# Tests for strike/restore methods


async def test_strike_method(redis_url: str, strike_name: str):
    """Test the strike() convenience method."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await strikes.strike(parameter="customer_id", operator="==", value="blocked")

        assert strikes.is_stricken({"customer_id": "blocked"})
        assert not strikes.is_stricken({"customer_id": "allowed"})


async def test_restore_method(redis_url: str, strike_name: str):
    """Test the restore() convenience method."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await strikes.strike(parameter="customer_id", operator="==", value="blocked")
        assert strikes.is_stricken({"customer_id": "blocked"})

        await strikes.restore(parameter="customer_id", operator="==", value="blocked")
        assert not strikes.is_stricken({"customer_id": "blocked"})


# Tests for receiving strikes from Redis stream


async def test_receives_strikes(redis_url: str, strike_name: str):
    """Test that StrikeList receives strikes from the stream."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await send_strike(strikes, "customer_id", "==", "blocked")
        await asyncio.sleep(0.1)

        assert strikes.is_stricken({"customer_id": "blocked"})
        assert not strikes.is_stricken({"customer_id": "allowed"})


async def test_receives_restore(redis_url: str, strike_name: str):
    """Test that StrikeList receives restores from the stream."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await send_strike(strikes, "customer_id", "==", "blocked")
        await asyncio.sleep(0.1)
        assert strikes.is_stricken({"customer_id": "blocked"})

        await send_restore(strikes, "customer_id", "==", "blocked")
        await asyncio.sleep(0.1)
        assert not strikes.is_stricken({"customer_id": "blocked"})


async def test_receives_multiple_strikes(redis_url: str, strike_name: str):
    """Test receiving multiple different strikes."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await send_strike(strikes, "region", "==", "us-west")
        await send_strike(strikes, "priority", ">=", 5)
        await asyncio.sleep(0.1)

        # Either condition triggers strike
        assert strikes.is_stricken({"region": "us-west", "priority": 1})
        assert strikes.is_stricken({"region": "us-east", "priority": 10})
        assert not strikes.is_stricken({"region": "us-east", "priority": 1})


async def test_new_instance_receives_existing_strikes(redis_url: str, strike_name: str):
    """Test that a new instance receives strikes from the stream history."""
    # Create first instance and send a strike
    async with StrikeList(url=redis_url, name=strike_name) as strikes1:
        await send_strike(strikes1, "customer_id", "==", "blocked")
        await asyncio.sleep(0.1)

    # Start a new StrikeList instance - should read existing strikes
    async with StrikeList(url=redis_url, name=strike_name) as strikes2:
        await strikes2.wait_for_strikes_loaded()
        assert strikes2.is_stricken({"customer_id": "blocked"})


# Tests for is_stricken matching logic


async def test_all_operators(redis_url: str, strike_name: str):
    """Test all comparison operators."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        # Test each operator
        await send_strike(strikes, "eq_param", "==", 42)
        await send_strike(strikes, "ne_param", "!=", 42)
        await send_strike(strikes, "gt_param", ">", 100)
        await send_strike(strikes, "gte_param", ">=", 100)
        await send_strike(strikes, "lt_param", "<", 10)
        await send_strike(strikes, "lte_param", "<=", 10)
        await send_strike(strikes, "between_param", "between", (20, 30))
        await asyncio.sleep(0.1)

        # ==
        assert strikes.is_stricken({"eq_param": 42})
        assert not strikes.is_stricken({"eq_param": 43})

        # !=
        assert strikes.is_stricken({"ne_param": 43})
        assert not strikes.is_stricken({"ne_param": 42})

        # >
        assert strikes.is_stricken({"gt_param": 101})
        assert not strikes.is_stricken({"gt_param": 100})

        # >=
        assert strikes.is_stricken({"gte_param": 100})
        assert not strikes.is_stricken({"gte_param": 99})

        # <
        assert strikes.is_stricken({"lt_param": 9})
        assert not strikes.is_stricken({"lt_param": 10})

        # <=
        assert strikes.is_stricken({"lte_param": 10})
        assert not strikes.is_stricken({"lte_param": 11})

        # between
        assert strikes.is_stricken({"between_param": 25})
        assert not strikes.is_stricken({"between_param": 19})


async def test_empty_dict_not_stricken(redis_url: str, strike_name: str):
    """Test that an empty dict is never stricken."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await send_strike(strikes, "customer_id", "==", "blocked")
        await asyncio.sleep(0.1)

        assert not strikes.is_stricken({})


async def test_type_mismatch_handled_gracefully(
    redis_url: str, strike_name: str, caplog: pytest.LogCaptureFixture
):
    """Test that type mismatches don't raise exceptions."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        await send_strike(strikes, "amount", ">", 100)
        await asyncio.sleep(0.1)

        # Comparing string to int should not raise
        result = strikes.is_stricken({"amount": "not a number"})
        assert result is False
        assert "Incompatible type" in caplog.text


# Internal state invariant tests


async def test_invariant_conditions_only_default_after_remove(
    redis_url: str, strike_name: str
):
    """After removing a temporary condition, only the default condition should remain."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        # Initially only default condition
        assert len(strikes._conditions) == 1
        default_condition = strikes._conditions[0]

        # Add a temporary condition (lambda to avoid coverage gap on unused function)
        temp_condition: Callable[[Execution], bool] = lambda _: False  # noqa: E731

        strikes.add_condition(temp_condition)
        assert len(strikes._conditions) == 2

        # Remove the temporary condition
        strikes.remove_condition(temp_condition)

        # Should be back to only the default
        assert len(strikes._conditions) == 1
        assert strikes._conditions[0] is default_condition


async def test_invariant_no_empty_dicts_in_task_strikes_after_restore(
    redis_url: str, strike_name: str
):
    """After restoring all strikes for a task, no empty dicts should remain."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        # Strike a specific task+parameter combination
        await strikes.strike(
            function="my_task", parameter="user_id", operator="==", value=123
        )
        await asyncio.sleep(0.1)

        # Verify structure exists
        assert "my_task" in strikes.task_strikes
        assert "user_id" in strikes.task_strikes["my_task"]

        # Restore the strike
        await strikes.restore(
            function="my_task", parameter="user_id", operator="==", value=123
        )
        await asyncio.sleep(0.1)

        # After restore, no empty dict entries should remain
        assert "my_task" not in strikes.task_strikes, (
            "task_strikes should not contain empty task entries"
        )


async def test_invariant_no_empty_dicts_in_parameter_strikes_after_restore(
    redis_url: str, strike_name: str
):
    """After restoring all parameter strikes, no empty sets should remain."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        # Strike a parameter (applies to all tasks)
        await strikes.strike(parameter="region", operator="==", value="us-west")
        await asyncio.sleep(0.1)

        # Verify structure exists
        assert "region" in strikes.parameter_strikes
        assert len(strikes.parameter_strikes["region"]) == 1

        # Restore the strike
        await strikes.restore(parameter="region", operator="==", value="us-west")
        await asyncio.sleep(0.1)

        # After restore, no empty set entries should remain
        assert "region" not in strikes.parameter_strikes, (
            "parameter_strikes should not contain empty parameter entries"
        )


async def test_invariant_multiple_strike_restore_cycles(
    redis_url: str, strike_name: str
):
    """Multiple strike/restore cycles should not accumulate orphan entries."""
    async with StrikeList(url=redis_url, name=strike_name) as strikes:
        for cycle in range(5):
            # Strike
            await strikes.strike(
                function="task_a",
                parameter="customer_id",
                operator="==",
                value=f"id-{cycle}",
            )
            await strikes.strike(
                parameter="global_param", operator=">=", value=100 + cycle
            )
            await asyncio.sleep(0.05)

            # Verify strikes are in effect
            assert "task_a" in strikes.task_strikes
            assert "global_param" in strikes.parameter_strikes

            # Restore
            await strikes.restore(
                function="task_a",
                parameter="customer_id",
                operator="==",
                value=f"id-{cycle}",
            )
            await strikes.restore(
                parameter="global_param", operator=">=", value=100 + cycle
            )
            await asyncio.sleep(0.05)

            # After each cycle, both dicts should be clean
            assert "task_a" not in strikes.task_strikes, (
                f"Orphan in task_strikes after cycle {cycle}"
            )
            assert "global_param" not in strikes.parameter_strikes, (
                f"Orphan in parameter_strikes after cycle {cycle}"
            )


async def test_invariant_strikelist_state_persists_through_context(
    redis_url: str, strike_name: str
):
    """StrikeList data structures should persist (not be deleted) after context exit."""
    strikes = StrikeList(url=redis_url, name=strike_name)

    # Data structures exist before entering context
    assert hasattr(strikes, "task_strikes")
    assert hasattr(strikes, "parameter_strikes")
    assert hasattr(strikes, "_conditions")

    await strikes.__aenter__()

    # Add some data
    await strikes.strike(parameter="test_param", operator="==", value="test_value")
    await asyncio.sleep(0.1)

    await strikes.__aexit__(None, None, None)

    # Data structures should still exist (not deleted like Worker)
    # This is intentional - StrikeList maintains state
    assert hasattr(strikes, "task_strikes")
    assert hasattr(strikes, "parameter_strikes")
    assert hasattr(strikes, "_conditions")

    # But connection-related state should be cleaned up
    assert strikes._monitor_task is None
    assert strikes._strikes_loaded is None
