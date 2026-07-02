"""Declarative Lua-script wrappers.

The ``@redis_script`` decorator collapses each Lua-backed Redis operation
into a single callable :class:`RedisScript` whose signature *is* the
declared function's calling contract and whose docstring *is* the Lua
source.  Compared with the hand-rolled ``redis.register_script`` +
lazy-singleton pattern, every script gains:

* SHA1 computed once at decoration time and reused forever -- no
  per-call ``register_script`` hash work.
* A single ``EVALSHA`` round-trip, with one layer of ``NOSCRIPT``
  fallback for the in-process ``memory://`` backend whose script cache
  lives per ``BurnerRedis`` instance.
* Encoding rules (``bool`` -> ``"1"``/``"0"``, numbers -> ``str``, dicts
  flattened, lists/tuples spread) centralised here instead of repeated
  at every call site.
* Pipelined forms: ``script.enqueue(pipeline, **same_kwargs)`` queues
  the EVALSHA on a pipeline so N invocations share one round-trip (after
  a ``script_load`` on the client, since a pipelined EVALSHA can't fall
  back on ``NOSCRIPT``), and ``script.enqueue_eval(...)`` sends the full
  source instead for servers whose script cache can't be relied on --
  the only pipelined form Redis Cluster permits.

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
    Concatenate,
    Generic,
    Mapping,
    ParamSpec,
    Sequence,
    TypeAlias,
    TypeVar,
    cast,
    get_args,
    get_type_hints,
)

from redis.commands.core import AsyncScript

from ._redis import Pipeline, RedisClient


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

_P = ParamSpec("_P")
_R = TypeVar("_R")


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


class RedisScript(Generic[_P, _R]):
    """A declared Lua script, callable directly or enqueueable on a pipeline.

    Calling the script executes it immediately (one ``EVALSHA`` round-trip
    with a ``NOSCRIPT`` reload fallback).  ``enqueue()`` instead queues the
    same ``EVALSHA`` on a pipeline, so N script invocations can share a
    single round-trip; callers are responsible for ensuring the script is
    loaded first (``await redis.script_load(script.lua)``), because a
    pipelined ``EVALSHA`` cannot fall back on ``NOSCRIPT``.
    ``enqueue_eval()`` queues a full-source ``EVAL`` instead, for servers
    (Redis Cluster nodes) whose script cache can't be relied upon.

    Script parameters are keyword-only at runtime: every ``Key``/``Arg``/
    ``Args`` value must be passed by name, matching the declared function's
    keyword-only signature.
    """

    def __init__(
        self,
        fn: Callable[..., Awaitable[Any]],
        lua: str,
        key_params: list[str],
        arg_params: list[tuple[str, _Marker, Any]],
    ) -> None:
        functools.update_wrapper(self, fn)
        self.lua = lua
        self._key_params = key_params
        self._arg_params = arg_params
        # Pre-encode to bytes so ``AsyncScript.__init__`` skips its
        # client-encoder lookup; then put the ``str`` back on ``.script``
        # because burner's ``script_load`` (used on the NOSCRIPT path)
        # rejects bytes.
        self._script: AsyncScript = AsyncScript(None, lua.encode("utf-8"))  # type: ignore[arg-type]
        self._script.script = lua

    @property
    def sha(self) -> str:
        return self._script.sha

    def _keys_and_argv(
        self, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> tuple[list[Any], list[Any]]:
        # The declared functions take every Key/Arg/Args parameter as
        # keyword-only, so anything positional would be silently dropped by
        # the name-based lookups below -- reject it loudly instead.
        if args:
            raise TypeError(
                f"{self.__name__} takes script parameters by keyword only, "
                f"but got {len(args)} positional argument(s)"
            )
        keys: list[Any] = [kwargs[name] for name in self._key_params]
        argv: list[Any] = []
        for name, kind, _ in self._arg_params:
            value = kwargs[name]
            if kind is _Args:
                argv.extend(_expand_args(value))
            else:
                argv.append(_encode_scalar(value))
        return keys, argv

    # Hot-path call: redis is the first positional, everything else is by
    # keyword.  Bypass ``inspect.Signature.bind`` (~20-50 us/call) -- we
    # already parsed the parameter ordering at decoration time and the
    # callers always pass keyword arguments.
    async def __call__(
        self, redis: RedisClient, /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        keys, argv = self._keys_and_argv(args, kwargs)
        return await self._script(keys=keys, args=argv, client=redis)  # type: ignore[arg-type]

    def enqueue(
        self, pipeline: Pipeline, /, *args: _P.args, **kwargs: _P.kwargs
    ) -> None:
        """Queue this script's EVALSHA on ``pipeline`` without executing it.

        The reply appears in ``pipeline.execute()``'s results at this
        command's position.  Unlike ``__call__``, there is no ``NOSCRIPT``
        fallback -- load the script into the server's script cache first via
        ``await redis.script_load(script.lua)``.  Not usable with cluster
        pipelines: redis-py blocks pipelined EVALSHA in cluster mode; use
        ``enqueue_eval`` there instead.
        """
        keys, argv = self._keys_and_argv(args, kwargs)
        pipeline.evalsha(self.sha, len(keys), *keys, *argv)

    def enqueue_eval(
        self, pipeline: Pipeline, /, *args: _P.args, **kwargs: _P.kwargs
    ) -> None:
        """Queue this script as a full-source EVAL on ``pipeline``.

        Unlike ``enqueue``, the Lua source travels with the command, so
        nothing depends on the server's script cache.  This is the only
        pipelined form that works on Redis Cluster: redis-py blocks
        pipelined EVALSHA there outright, and even if it didn't, a command
        could land on a node whose cache has never seen the script (after
        failover or resharding) with no NOSCRIPT fallback available.
        """
        keys, argv = self._keys_and_argv(args, kwargs)
        pipeline.eval(self.lua, len(keys), *keys, *argv)


def redis_script(
    fn: Callable[Concatenate[RedisClient, _P], Awaitable[_R]],
) -> RedisScript[_P, _R]:
    """Wrap an async function declaring a Lua script as its docstring.

    Returns a :class:`RedisScript`: call it exactly like the declared
    function to execute immediately, or use ``.enqueue(pipeline, ...)`` to
    queue it on a pipeline.  See the module docstring for the authoring
    contract and the encoding rules applied to ``Arg`` / ``Args``
    parameters.
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

    return RedisScript(fn, lua, key_params, arg_params)


__all__ = [
    "Arg",
    "Args",
    "Key",
    "RedisScript",
    "redis_script",
]
