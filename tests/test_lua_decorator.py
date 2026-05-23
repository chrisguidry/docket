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


# All KEYS for a given EVALSHA call must hash to the same Redis Cluster
# slot, so the test keys all share the ``{lua-test}`` hash tag.

_TAG = "{lua-test}"


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
            first_key=f"{_TAG}-K-one",
            second_key=f"{_TAG}-K-two",
            first_arg="A-one",
            second_arg="A-two",
        )

    assert result == [
        f"{_TAG}-K-one".encode(),
        f"{_TAG}-K-two".encode(),
        b"A-one",
        b"A-two",
    ]


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
        true_value = await echo_bool(redis, key=f"{_TAG}-k", flag=True)
        false_value = await echo_bool(redis, key=f"{_TAG}-k", flag=False)

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
        result = await echo_numbers(redis, key=f"{_TAG}-k", count=42, ratio=3.14)

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
            key=f"{_TAG}-k",
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
        result = await echo_list(redis, key=f"{_TAG}-k", items=["x", "y", "z"])

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
        result = await echo_tuple(redis, key=f"{_TAG}-k", items=("a", "b"))

    assert result == [b"a", b"b"]


async def test_codegen_binds_typed_locals(docket: Docket) -> None:
    """``Arg[int]`` / ``Arg[float]`` / ``Arg[bool]`` produce typed Lua locals.

    The script body refers to the parameters as locals (no ARGV indexing)
    and does arithmetic / boolean ops on them.  If the decorator forgot
    the ``tonumber`` wrapper, ``count + 1`` would be a string-concat
    error; if it forgot the ``== '1'`` decode, ``if flag then`` would
    always be truthy.
    """

    @redis_script
    async def calc(
        redis: RedisClient,
        *,
        key: Key[str],
        count: Arg[int],
        ratio: Arg[float],
        flag: Arg[bool],
    ) -> list[int]:
        """
        local bumped = count + 1
        local scaled = ratio * 10
        local picked = 0
        if flag then picked = 1 end
        return {bumped, scaled, picked}
        """
        ...

    async with docket.redis() as redis:
        result = await calc(redis, key=f"{_TAG}-k", count=41, ratio=2.5, flag=True)

    assert result == [42, 25, 1]


async def test_codegen_exposes_variadic_start_offset(docket: Docket) -> None:
    """``Args[...]`` emits a ``<name>_start`` local at the right offset.

    Without the codegen the script body would have to hard-code the
    1-indexed ARGV position where the variadic begins (after all the
    scalar args).  The constant lets us iterate without that bookkeeping.
    """

    @redis_script
    async def join_variadic(
        redis: RedisClient,
        *,
        key: Key[str],
        leading: Arg[str],
        also_leading: Arg[str],
        items: Args[list[str]],
    ) -> bytes:
        """
        -- items_start should be 3 (after leading + also_leading)
        local pieces = {}
        for i = items_start, #ARGV do
            pieces[#pieces + 1] = ARGV[i]
        end
        return table.concat(pieces, '|')
        """
        ...

    async with docket.redis() as redis:
        result = await join_variadic(
            redis,
            key=f"{_TAG}-k",
            leading="L1",
            also_leading="L2",
            items=["A", "B", "C"],
        )

    assert result == b"A|B|C"


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
        result = await echo_count(redis, key=f"{_TAG}-k", leading="solo", rest={})

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
        first = await noscript_echo(redis, key=f"{_TAG}-k")
        assert first == b"hello"

        # Wipe the server's script cache.  The next call's EVALSHA fails
        # with NOSCRIPT and the decorator must transparently reload via
        # SCRIPT LOAD and retry.
        await redis.execute_command("SCRIPT", "FLUSH")  # type: ignore[attr-defined]

        second = await noscript_echo(redis, key=f"{_TAG}-k")
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
