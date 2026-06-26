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

import functools
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
    get_args,
    get_type_hints,
)

from redis.commands.core import AsyncScript

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
    if isinstance(value, (str, bytes)):
        return value
    # The ``Arg[T]`` / ``Args[T]`` TypeVar bounds catch unsupported types
    # at decoration time, but a payload dict built dynamically (e.g. the
    # ``extra_fields`` list in ``_terminal``) can still smuggle a ``None``
    # or other unsupported value through at call time.  Reject it here
    # so the failure surfaces with a precise local message instead of an
    # opaque ``DataError`` from redis-py several frames down.
    raise TypeError(
        f"@redis_script value must be str/bytes/int/float/bool, "
        f"got {type(value).__name__}: {value!r}"
    )


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


def _annotation_for(hint: Any) -> tuple[_Marker, Any] | None:
    """Return ``(marker_class, underlying_type)`` for an ``Annotated`` hint.

    ``Key[str]`` resolves to ``Annotated[str, _Key]``; ``get_args`` returns
    ``(str, _Key)`` so the first positional is the parameter's underlying
    Python type and the rest is the ``__metadata__`` tuple.  We use the
    underlying type to pick a Lua decoder (``tonumber`` for numbers,
    ``== '1'`` for bools, raw for strings/bytes).
    """
    for meta in getattr(hint, "__metadata__", ()):
        if meta in (_Key, _Arg, _Args):
            return meta, get_args(hint)[0]
    return None


def _decode_for(py_type: Any) -> str:
    """Lua snippet that decodes ``{}``-placeholder into a typed local.

    Format with the ARGV index, e.g. ``_decode_for(int).format(3)`` gives
    ``tonumber(ARGV[3])``.  The Python encoder (``_encode_scalar``) and
    this decoder are paired: bools go over the wire as ``"1"``/``"0"``
    strings, numbers as decimal strings, str/bytes raw.
    """
    if py_type is bool:
        return "ARGV[{}] == '1'"
    if py_type in (int, float):
        return "tonumber(ARGV[{}])"
    return "ARGV[{}]"


def _generate_preamble(
    key_params: list[str],
    arg_params: list[tuple[str, _Marker, Any]],
) -> str:
    """Emit ``local name = KEYS[i]`` / ``ARGV[j]`` bindings for each slot.

    Scalar params get a typed local (with the right ``tonumber`` /
    ``== '1'`` wrapper) and consume one ARGV slot.  An ``Args[...]``
    parameter consumes no fixed slot -- instead, ``<name>_start`` is
    bound to the 1-indexed position where the variadic begins, so the
    script can iterate ``for i = <name>_start, #ARGV[, step] do``
    without hard-coding the offset.
    """
    lines: list[str] = []
    for i, name in enumerate(key_params, 1):
        lines.append(f"local {name} = KEYS[{i}]")
    argv_index = 1
    for name, kind, py_type in arg_params:
        if kind is _Args:
            lines.append(f"local {name}_start = {argv_index}")
            continue
        lines.append(f"local {name} = {_decode_for(py_type).format(argv_index)}")
        argv_index += 1
    return "\n".join(lines)


def redis_script(fn: _F) -> _F:
    """Wrap an async function declaring a Lua script as its docstring.

    See the module docstring for the authoring contract and the encoding
    rules applied to ``Arg`` / ``Args`` parameters.
    """
    body = inspect.getdoc(fn)
    if not body:
        raise TypeError(
            f"@redis_script function {fn.__qualname__} needs a Lua body in its docstring"
        )

    sig = inspect.signature(fn)
    hints = get_type_hints(fn, include_extras=True)

    key_params: list[str] = []
    arg_params: list[tuple[str, _Marker, Any]] = []
    redis_param: str | None = None

    for name, param in sig.parameters.items():
        hint = hints.get(name, param.annotation)
        if redis_param is None and hint is RedisClient:
            redis_param = name
            continue
        annotation = _annotation_for(hint)
        if annotation is None:
            raise TypeError(
                f"@redis_script: parameter {fn.__qualname__}.{name} must be "
                f"annotated as Key[...], Arg[...], or Args[...] "
                f"(or typed as RedisClient for the first parameter)"
            )
        kind, py_type = annotation
        if kind is _Key:
            key_params.append(name)
        else:
            arg_params.append((name, kind, py_type))

    if redis_param is None:
        raise TypeError(
            f"@redis_script: {fn.__qualname__} must take a RedisClient as its "
            f"first parameter"
        )
    if not key_params:
        raise TypeError(
            f"@redis_script: {fn.__qualname__} must declare at least one Key[...] parameter"
        )

    # A variadic ``Args[...]`` parameter consumes an unknown number of ARGV
    # slots at runtime, so any scalar ``Arg[...]`` after it would have an
    # indeterminate index in the generated preamble.  Forbid that shape at
    # decoration time rather than silently emit wrong ARGV indices.
    for position, (name, kind, _) in enumerate(arg_params):
        if kind is _Args:
            tail = arg_params[position + 1 :]
            if tail:
                trailing = ", ".join(rest_name for rest_name, _, _ in tail)
                raise TypeError(
                    f"@redis_script: {fn.__qualname__} has Arg[...] parameter(s) "
                    f"{trailing} after Args[...] parameter {name}; "
                    f"Args[...] must be the last parameter"
                )

    preamble = _generate_preamble(key_params, arg_params)
    lua = f"{preamble}\n\n{body}" if preamble else body

    # Pre-encode to bytes so ``AsyncScript.__init__`` skips its
    # client-encoder lookup; then put the ``str`` back on ``.script``
    # because burner's ``script_load`` (used on the NOSCRIPT path)
    # rejects bytes.
    script: AsyncScript = AsyncScript(None, lua.encode("utf-8"))  # type: ignore[arg-type]
    script.script = lua

    # Hot-path call: redis is the first positional, everything else is by
    # keyword.  Bypass ``inspect.Signature.bind`` (~20-50 us/call) -- we
    # already parsed the parameter ordering at decoration time and the
    # callers always pass keyword arguments.
    @functools.wraps(fn)
    async def wrapper(redis: RedisClient, **kwargs: Any) -> Any:
        keys: list[Any] = [kwargs[name] for name in key_params]
        argv: list[Any] = []
        for name, kind, _ in arg_params:
            value = kwargs[name]
            if kind is _Args:
                argv.extend(_expand_args(value))
            else:
                argv.append(_encode_scalar(value))
        return await script(keys=keys, args=argv, client=redis)  # type: ignore[arg-type]

    wrapper.__lua__ = lua  # type: ignore[attr-defined]
    return cast(_F, wrapper)


__all__ = [
    "Arg",
    "Args",
    "Key",
    "redis_script",
]
