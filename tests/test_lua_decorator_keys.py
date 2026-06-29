"""Tests for variadic Redis key support in the ``@redis_script`` decorator."""

from __future__ import annotations

import pytest

from docket import Docket
from docket._lua import Arg, Key, Keys, redis_script
from docket._redis import RedisClient


@redis_script
async def _join_variadic_keys(
    redis: RedisClient,
    *,
    key: Key[str],
    dynamic_keys: Keys[list[str]],
    suffix: Arg[str],
) -> list[bytes]:
    """
    local pieces = {}
    for i = dynamic_keys_start, #KEYS do
        pieces[#pieces + 1] = KEYS[i]
    end
    pieces[#pieces + 1] = suffix
    return pieces
    """
    ...


def _k(docket: Docket, suffix: str) -> str:
    return f"{docket.prefix}:lua-test:{suffix}"


async def test_codegen_exposes_variadic_keys_start_offset(docket: Docket) -> None:
    """``Keys[...]`` emits dynamic names as KEYS while preserving ARGV order."""
    first_dynamic = _k(docket, "dynamic-1")
    second_dynamic = _k(docket, "dynamic-2")

    async with docket.redis() as redis:
        result = await _join_variadic_keys(
            redis,
            key=_k(docket, "fixed"),
            dynamic_keys=[first_dynamic, second_dynamic],
            suffix="tail",
        )

    assert result == [
        first_dynamic.encode(),
        second_dynamic.encode(),
        b"tail",
    ]


def test_key_after_keys_is_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match=r"Keys\[\.\.\.\] must be the last key"):

        @redis_script
        async def trailing_key(  # pyright: ignore[reportUnusedFunction]
            redis: RedisClient,
            *,
            fixed: Key[str],
            dynamic: Keys[list[str]],
            tail: Key[str],
            arg: Arg[str],
        ) -> bytes:
            """return 'x'"""
            ...


def test_multiple_keys_groups_are_rejected_at_decoration_time() -> None:
    with pytest.raises(TypeError, match=r"multiple Keys\[\.\.\.\]"):

        @redis_script
        async def multiple_variadic_keys(  # pyright: ignore[reportUnusedFunction]
            redis: RedisClient,
            *,
            fixed: Key[str],
            first_dynamic: Keys[list[str]],
            second_dynamic: Keys[list[str]],
            arg: Arg[str],
        ) -> bytes:
            """return 'x'"""
            ...
