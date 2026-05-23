"""Declarative Lua-script wrappers.

The ``@redis_script`` decorator collapses each Lua-backed Redis operation
into a single async function whose signature *is* the wrapper's calling
contract and whose docstring *is* the Lua source.  Compared with the
hand-rolled ``redis.register_script`` + lazy-singleton pattern, every
script gains:

* SHA1 computed once at decoration time and reused forever -- no
  per-call ``register_script`` hash work.
* A single ``EVALSHA`` round-trip, with one layer of ``NOSCRIPT``
  fallback for the in-process ``memory://`` backend whose script cache
  lives per ``BurnerRedis`` instance.
* Encoding rules (``bool`` -> ``"1"``/``"0"``, numbers -> ``str``, dicts
  flattened, lists/tuples spread) centralised here instead of repeated
  at every call site.

Authoring shape:

.. code-block:: python

    @redis_script
    async def _claim(
        redis: RedisClient,
        *,
        runs_key: Key[str],
        progress_key: Key[str],
        worker: Arg[str],
        started_at: Arg[str],
        generation: Arg[int],
    ) -> bytes:
        \"\"\"
        local runs_key = KEYS[1]
        -- ... Lua body ...
        return 'OK'
        \"\"\"
        ...

The trailing ``...`` is the standard Python stub idiom -- pyright
recognises ``docstring + Ellipsis`` as a stub body and stops asking the
function to ``return`` anything, so no per-function ``# type: ignore``
is needed.
"""

from __future__ import annotations

import hashlib
import inspect
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
    TypeAlias,
    TypeVar,
    cast,
    get_type_hints,
)

from redis.exceptions import NoScriptError

from ._redis import RedisClient


# Marker classes used as ``Annotated`` metadata.  The class objects
# themselves go into ``__metadata__`` -- no instances needed -- and the
# decorator uses identity checks (``meta is _Key``) to discriminate slots.


class _Key:
    """``Annotated`` metadata marker -- one Lua KEYS slot."""


class _Arg:
    """``Annotated`` metadata marker -- one Lua ARGV slot."""


class _Args:
    """``Annotated`` metadata marker -- variadic Lua ARGV slots.

    Dicts are flattened to alternating ``k1, v1, k2, v2, ...`` (insertion
    order); lists and tuples are spread element-wise.
    """


# Constrained TypeVars give the marker aliases their bounds: ``Key[int]``
# / ``Arg[dict[...]]`` / ``Args[str]`` fail at pyright time, not at
# decoration time, because the constraint lists what each ``TypeVar`` is
# allowed to resolve to.

_KeyT = TypeVar("_KeyT", str, bytes)
_ArgT = TypeVar("_ArgT", str, bytes, int, float, bool)
_ArgsT = TypeVar("_ArgsT", dict[Any, Any], list[Any], tuple[Any, ...])

Key: TypeAlias = Annotated[_KeyT, _Key]
"""One Lua ``KEYS`` slot.  Bounded to the types Redis accepts as keys."""

Arg: TypeAlias = Annotated[_ArgT, _Arg]
"""One Lua ``ARGV`` slot.  Bounded to scalar types the decoder knows how to format."""

Args: TypeAlias = Annotated[_ArgsT, _Args]
"""Variadic Lua ``ARGV`` slots.

A ``dict`` flattens into alternating field/value pairs in insertion order
(matching Redis's ``HSET`` / ``XADD`` field-value convention).  A
``list`` or ``tuple`` spreads element-wise; each element follows the
same per-value encoding rules as ``Arg``.
"""

_F = TypeVar("_F", bound=Callable[..., Awaitable[Any]])


def _encode_scalar(value: Any) -> str | bytes | int | float:
    """Encode a single value for inclusion in EVALSHA's keys-and-args list."""
    # ``bool`` is a subclass of ``int`` -- check it first so ``True`` becomes
    # ``"1"`` rather than encoding via the int branch (and stringifying as
    # ``"True"`` if we ever used ``str(value)`` there).
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return value


def _expand_args(value: Any) -> list[Any]:
    """Expand a variadic value (``dict`` / ``list`` / ``tuple``) into ARGV slots.

    The ``Args[T]`` bound (``dict | list | tuple``) already prevents any
    other shape from reaching this function.
    """
    if isinstance(value, Mapping):
        mapping = cast(Mapping[Any, Any], value)
        return [
            _encode_scalar(item)
            for field, val in mapping.items()
            for item in (field, val)
        ]
    sequence = cast(Sequence[Any], value)
    return [_encode_scalar(item) for item in sequence]


_Marker: TypeAlias = type[_Key] | type[_Arg] | type[_Args]


def _kind_for(hint: Any) -> _Marker | None:
    """Return the marker class for a parameter, or ``None`` if untagged.

    ``Key[str]`` / ``Arg[int]`` / ``Args[dict[...]]`` are ``Annotated``
    generic aliases; ``get_type_hints(..., include_extras=True)`` resolves
    them directly to ``Annotated[T, marker]`` and exposes ``__metadata__``.
    """
    for meta in getattr(hint, "__metadata__", ()):
        if meta in (_Key, _Arg, _Args):
            return meta
    return None


def redis_script(fn: _F) -> _F:
    """Wrap an async function declaring a Lua script as its docstring.

    See the module docstring for the authoring contract and the encoding
    rules applied to ``Arg`` / ``Args`` parameters.
    """
    lua = inspect.getdoc(fn)
    if not lua:
        raise TypeError(
            f"@redis_script function {fn.__qualname__} needs a Lua body in its docstring"
        )

    sha = hashlib.sha1(lua.encode("utf-8")).hexdigest()

    sig = inspect.signature(fn)
    hints = get_type_hints(fn, include_extras=True)

    key_params: list[str] = []
    arg_params: list[tuple[str, _Marker]] = []
    redis_param: str | None = None

    for name, param in sig.parameters.items():
        hint = hints.get(name, param.annotation)
        if redis_param is None and hint is RedisClient:
            redis_param = name
            continue
        kind = _kind_for(hint)
        if kind is None:
            raise TypeError(
                f"@redis_script: parameter {fn.__qualname__}.{name} must be "
                f"annotated as Key[...], Arg[...], or Args[...] "
                f"(or typed as RedisClient for the first parameter)"
            )
        if kind is _Key:
            key_params.append(name)
        else:
            arg_params.append((name, kind))

    if redis_param is None:
        raise TypeError(
            f"@redis_script: {fn.__qualname__} must take a RedisClient as its "
            f"first parameter"
        )
    if not key_params:
        raise TypeError(
            f"@redis_script: {fn.__qualname__} must declare at least one Key[...] parameter"
        )

    # Hot-path call: redis is the first positional, everything else is by
    # keyword.  Bypass ``inspect.Signature.bind`` (~20-50 us/call) -- we
    # already parsed the parameter ordering at decoration time and the
    # callers always pass keyword arguments.
    async def wrapper(redis: RedisClient, **kwargs: Any) -> Any:
        keys: list[Any] = [kwargs[name] for name in key_params]
        argv: list[Any] = []
        for name, kind in arg_params:
            value = kwargs[name]
            if kind is _Args:
                argv.extend(_expand_args(value))
            else:
                argv.append(_encode_scalar(value))

        try:
            return await redis.evalsha(sha, len(keys), *keys, *argv)
        except NoScriptError:
            await redis.script_load(lua)
            return await redis.evalsha(sha, len(keys), *keys, *argv)

    wrapper.__name__ = fn.__name__
    wrapper.__qualname__ = fn.__qualname__
    wrapper.__doc__ = fn.__doc__
    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    return cast(_F, wrapper)


__all__ = [
    "Arg",
    "Args",
    "Key",
    "redis_script",
]
