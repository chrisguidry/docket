"""Tests for StrikeList."""

# pyright: reportPrivateUsage=false

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from docket import StrikeList
from docket.strikelist import Operator, Restore, Strike


@pytest.fixture
def strike_name() -> str:
    """Unique name for each test to avoid collisions."""
    return f"test-strikes-{uuid4()}"


async def send_strike(
    strikes: StrikeList,
    parameter: str,
    operator: str,
    value: Any,
) -> None:
    """Send a strike instruction directly to Redis."""
    instruction = Strike(None, parameter, Operator(operator), value)
    async with Redis(connection_pool=strikes._connection_pool) as r:
        await r.xadd(strikes.strike_key, instruction.as_message())  # type: ignore[arg-type]


async def send_restore(
    strikes: StrikeList,
    parameter: str,
    operator: str,
    value: Any,
) -> None:
    """Send a restore instruction directly to Redis."""
    instruction = Restore(None, parameter, Operator(operator), value)
    async with Redis(connection_pool=strikes._connection_pool) as r:
        await r.xadd(strikes.strike_key, instruction.as_message())  # type: ignore[arg-type]


class TestStrikeListBasic:
    """Basic functionality tests for StrikeList."""

    async def test_context_manager(self, redis_url: str, strike_name: str) -> None:
        """Test async context manager works correctly."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            assert strikes._connection_pool is not None
            assert strikes._monitor_task is not None

        # After exit, resources should be cleaned up
        assert strikes._connection_pool is None
        assert strikes._monitor_task is None

    async def test_explicit_connect_close(
        self, redis_url: str, strike_name: str
    ) -> None:
        """Test explicit connect/close lifecycle."""
        strikes = StrikeList(url=redis_url, name=strike_name)

        # Initially not connected
        assert strikes._connection_pool is None
        assert strikes._monitor_task is None

        # Connect
        await strikes.connect()
        assert strikes._connection_pool is not None
        assert strikes._monitor_task is not None

        # Close
        await strikes.close()
        assert strikes._connection_pool is None
        assert strikes._monitor_task is None

    async def test_connect_is_idempotent(
        self, redis_url: str, strike_name: str
    ) -> None:
        """Test that calling connect multiple times is safe."""
        strikes = StrikeList(url=redis_url, name=strike_name)

        await strikes.connect()
        pool1 = strikes._connection_pool

        # Second connect should be a no-op
        await strikes.connect()
        assert strikes._connection_pool is pool1

        await strikes.close()

    async def test_close_is_idempotent(self, redis_url: str, strike_name: str) -> None:
        """Test that calling close multiple times is safe."""
        strikes = StrikeList(url=redis_url, name=strike_name)

        await strikes.connect()
        await strikes.close()

        # Second close should be a no-op
        await strikes.close()

    async def test_prefix_property(self, redis_url: str, strike_name: str) -> None:
        """Test the prefix property returns the name."""
        strikes = StrikeList(url=redis_url, name=strike_name)
        assert strikes.prefix == strike_name

    async def test_strike_key_property(self, redis_url: str, strike_name: str) -> None:
        """Test the strike_key property uses prefix."""
        strikes = StrikeList(url=redis_url, name=strike_name)
        assert strikes.strike_key == f"{strikes.prefix}:strikes"

    async def test_local_only_mode(self, strike_name: str) -> None:
        """Test StrikeList works without Redis (local-only mode)."""
        # No URL = local-only mode
        strikes = StrikeList(name=strike_name)
        await strikes.connect()  # Should be a no-op

        assert strikes._connection_pool is None
        assert strikes._monitor_task is None
        assert strikes._strikes_loaded is None

        # wait_for_strikes_loaded returns immediately in local-only mode
        await strikes.wait_for_strikes_loaded()

        # Can still use locally
        strikes.update(Strike(None, "customer_id", Operator.EQUAL, "blocked"))
        assert strikes.is_stricken({"customer_id": "blocked"})
        assert not strikes.is_stricken({"customer_id": "allowed"})

        await strikes.close()

    async def test_memory_url_without_fakeredis(
        self, strike_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test error when memory:// used without fakeredis."""
        import sys

        # Temporarily make fakeredis unimportable
        monkeypatch.setitem(sys.modules, "fakeredis", None)
        monkeypatch.setitem(sys.modules, "fakeredis.aioredis", None)

        strikes = StrikeList(url="memory://", name=strike_name)
        with pytest.raises((ImportError, ModuleNotFoundError), match="fakeredis"):
            await strikes.connect()

    async def test_send_instruction_requires_connection(self, strike_name: str) -> None:
        """Test that send_instruction raises error when not connected."""
        strikes = StrikeList(url="memory://", name=strike_name)
        instruction = Strike(None, "customer_id", Operator.EQUAL, "blocked")

        with pytest.raises(RuntimeError, match="not connected to Redis"):
            await strikes.send_instruction(instruction)


class TestStrikeListMethods:
    """Tests for StrikeList strike/restore methods."""

    async def test_strike_method(self, redis_url: str, strike_name: str) -> None:
        """Test the strike() convenience method."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await strikes.strike(
                parameter="customer_id", operator="==", value="blocked"
            )

            assert strikes.is_stricken({"customer_id": "blocked"})
            assert not strikes.is_stricken({"customer_id": "allowed"})

    async def test_restore_method(self, redis_url: str, strike_name: str) -> None:
        """Test the restore() convenience method."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await strikes.strike(
                parameter="customer_id", operator="==", value="blocked"
            )
            assert strikes.is_stricken({"customer_id": "blocked"})

            await strikes.restore(
                parameter="customer_id", operator="==", value="blocked"
            )
            assert not strikes.is_stricken({"customer_id": "blocked"})


class TestStrikeListSync:
    """Tests for StrikeList receiving strikes from Redis stream."""

    async def test_receives_strikes(self, redis_url: str, strike_name: str) -> None:
        """Test that StrikeList receives strikes from the stream."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await send_strike(strikes, "customer_id", "==", "blocked")
            await asyncio.sleep(0.1)

            assert strikes.is_stricken({"customer_id": "blocked"})
            assert not strikes.is_stricken({"customer_id": "allowed"})

    async def test_receives_restore(self, redis_url: str, strike_name: str) -> None:
        """Test that StrikeList receives restores from the stream."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await send_strike(strikes, "customer_id", "==", "blocked")
            await asyncio.sleep(0.1)
            assert strikes.is_stricken({"customer_id": "blocked"})

            await send_restore(strikes, "customer_id", "==", "blocked")
            await asyncio.sleep(0.1)
            assert not strikes.is_stricken({"customer_id": "blocked"})

    async def test_receives_multiple_strikes(
        self, redis_url: str, strike_name: str
    ) -> None:
        """Test receiving multiple different strikes."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await send_strike(strikes, "region", "==", "us-west")
            await send_strike(strikes, "priority", ">=", 5)
            await asyncio.sleep(0.1)

            # Either condition triggers strike
            assert strikes.is_stricken({"region": "us-west", "priority": 1})
            assert strikes.is_stricken({"region": "us-east", "priority": 10})
            assert not strikes.is_stricken({"region": "us-east", "priority": 1})

    async def test_new_instance_receives_existing_strikes(
        self, redis_url: str, strike_name: str
    ) -> None:
        """Test that a new instance receives strikes from the stream history."""
        # Create first instance and send a strike
        async with StrikeList(url=redis_url, name=strike_name) as strikes1:
            await send_strike(strikes1, "customer_id", "==", "blocked")
            await asyncio.sleep(0.1)

        # Start a new StrikeList instance - should read existing strikes
        async with StrikeList(url=redis_url, name=strike_name) as strikes2:
            await strikes2.wait_for_strikes_loaded()
            assert strikes2.is_stricken({"customer_id": "blocked"})


class TestStrikeListMatching:
    """Tests for is_stricken matching logic."""

    async def test_all_operators(self, redis_url: str, strike_name: str) -> None:
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

    async def test_empty_dict_not_stricken(
        self, redis_url: str, strike_name: str
    ) -> None:
        """Test that an empty dict is never stricken."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await send_strike(strikes, "customer_id", "==", "blocked")
            await asyncio.sleep(0.1)

            assert not strikes.is_stricken({})

    async def test_type_mismatch_handled_gracefully(
        self, redis_url: str, strike_name: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that type mismatches don't raise exceptions."""
        async with StrikeList(url=redis_url, name=strike_name) as strikes:
            await send_strike(strikes, "amount", ">", 100)
            await asyncio.sleep(0.1)

            # Comparing string to int should not raise
            result = strikes.is_stricken({"amount": "not a number"})
            assert result is False
            assert "Incompatible type" in caplog.text
