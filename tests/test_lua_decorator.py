"""Tests for the ``@redis_script`` decorator.

These exercise the decorator in isolation -- partitioning of parameters
into KEYS / ARGV, encoding of bool / int / float / str / bytes values,
flattening of ``Args[dict]`` and spreading of ``Args[list]`` /
``Args[tuple]``, and the NOSCRIPT-retry path that's load-bearing for the
in-process memory backend.  Every Lua-backed code path in docket goes
through this decorator, so these unit tests guard the contract every
production wrapper relies on.
"""

from __future__ import annotations

import pytest

from docket import Docket
from docket._lua import Arg, Args, Key, redis_script
from docket._redis import RedisClient
from tests.conftest import skip_memory


async def test_keys_and_args_arrive_in_declaration_order(docket: Docket) -> None:
    @redis_script
    async def echo(
        redis: RedisClient,
        *,
        first_key: Key[str],
        second_key: Key[str],
        first_arg: Arg[str],
        second_arg: Arg[str],
    ) -> list[bytes]:
        """
        return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}
        """
        ...

    async with docket.redis() as redis:
        result = await echo(
            redis,
            first_key="K-one",
            second_key="K-two",
            first_arg="A-one",
            second_arg="A-two",
        )

    assert result == [b"K-one", b"K-two", b"A-one", b"A-two"]


async def test_bool_encodes_as_one_or_zero(docket: Docket) -> None:
    @redis_script
    async def echo_bool(
        redis: RedisClient,
        *,
        key: Key[str],
        flag: Arg[bool],
    ) -> bytes:
        """
        return ARGV[1]
        """
        ...

    async with docket.redis() as redis:
        true_value = await echo_bool(redis, key="k", flag=True)
        false_value = await echo_bool(redis, key="k", flag=False)

    assert true_value == b"1"
    assert false_value == b"0"


async def test_int_and_float_encode_via_str(docket: Docket) -> None:
    @redis_script
    async def echo_numbers(
        redis: RedisClient,
        *,
        key: Key[str],
        count: Arg[int],
        ratio: Arg[float],
    ) -> list[bytes]:
        """
        return {ARGV[1], ARGV[2]}
        """
        ...

    async with docket.redis() as redis:
        result = await echo_numbers(redis, key="k", count=42, ratio=3.14)

    assert result == [b"42", b"3.14"]


async def test_args_dict_flattens_preserving_insertion_order(docket: Docket) -> None:
    @redis_script
    async def echo_dict(
        redis: RedisClient,
        *,
        key: Key[str],
        fields: Args[dict[str, str]],
    ) -> list[bytes]:
        """
        return ARGV
        """
        ...

    async with docket.redis() as redis:
        result = await echo_dict(
            redis,
            key="k",
            fields={"alpha": "1", "beta": "2", "gamma": "3"},
        )

    assert result == [b"alpha", b"1", b"beta", b"2", b"gamma", b"3"]


async def test_args_list_spreads_element_wise(docket: Docket) -> None:
    @redis_script
    async def echo_list(
        redis: RedisClient,
        *,
        key: Key[str],
        items: Args[list[str]],
    ) -> list[bytes]:
        """
        return ARGV
        """
        ...

    async with docket.redis() as redis:
        result = await echo_list(redis, key="k", items=["x", "y", "z"])

    assert result == [b"x", b"y", b"z"]


async def test_args_tuple_spreads_element_wise(docket: Docket) -> None:
    @redis_script
    async def echo_tuple(
        redis: RedisClient,
        *,
        key: Key[str],
        items: Args[tuple[str, ...]],
    ) -> list[bytes]:
        """
        return ARGV
        """
        ...

    async with docket.redis() as redis:
        result = await echo_tuple(redis, key="k", items=("a", "b"))

    assert result == [b"a", b"b"]


async def test_empty_variadic_emits_no_argv_entries(docket: Docket) -> None:
    @redis_script
    async def echo_count(
        redis: RedisClient,
        *,
        key: Key[str],
        leading: Arg[str],
        rest: Args[dict[str, str]],
    ) -> int:
        """
        return #ARGV
        """
        ...

    async with docket.redis() as redis:
        result = await echo_count(redis, key="k", leading="solo", rest={})

    assert result == 1


@skip_memory
async def test_noscript_path_recovers_after_script_flush(docket: Docket) -> None:
    """Real-Redis NOSCRIPT recovery via SCRIPT FLUSH.

    The memory backend's cross-instance NOSCRIPT path is exercised by
    every existing dependency test that creates two ``Docket`` fixtures
    against ``memory://`` (each owning a separate ``BurnerRedis``).
    Here we explicitly drive the real-Redis path that ``SCRIPT FLUSH``
    enables.
    """

    @redis_script
    async def noscript_echo(
        redis: RedisClient,
        *,
        key: Key[str],
    ) -> bytes:
        """
        return 'hello'
        """
        ...

    async with docket.redis() as redis:
        first = await noscript_echo(redis, key="k")
        assert first == b"hello"

        # Wipe the server's script cache.  The next call's EVALSHA fails
        # with NOSCRIPT and the decorator must transparently reload via
        # SCRIPT LOAD and retry.
        await redis.execute_command("SCRIPT", "FLUSH")  # type: ignore[attr-defined]

        second = await noscript_echo(redis, key="k")
        assert second == b"hello"


def test_missing_docstring_is_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="needs a Lua body"):

        @redis_script
        async def no_doc(  # pyright: ignore[reportUnusedFunction]
            redis: RedisClient, *, key: Key[str]
        ) -> bytes: ...


def test_missing_redis_parameter_is_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="must take a RedisClient"):

        @redis_script
        async def no_redis(*, key: Key[str]) -> bytes:  # pyright: ignore[reportUnusedFunction]
            """return 'x'"""
            ...


def test_untagged_parameter_is_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="must be annotated as Key"):

        @redis_script
        async def untagged(  # pyright: ignore[reportUnusedFunction]
            redis: RedisClient,
            *,
            key: Key[str],
            mystery: str,
        ) -> bytes:
            """return 'x'"""
            ...


def test_missing_key_parameter_is_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="at least one Key"):

        @redis_script
        async def keyless(  # pyright: ignore[reportUnusedFunction]
            redis: RedisClient,
            *,
            something: Arg[str],
        ) -> bytes:
            """return 'x'"""
            ...
