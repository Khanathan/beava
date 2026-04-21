"""``@bv.stream`` decorator + Stream / StreamSource / StreamDerivation runtime types.

Plan 21-01 shipped the class form. Plan 21-02 adds:
  * :class:`StatelessOpsMixin` on :class:`Stream` — gives every Stream the 8
    per-row ops (filter / map / select / drop / rename / with_columns /
    cast / fillna).
  * :class:`StreamDerivation` — the descriptor returned by stateless ops and
    by function-form ``@bv.stream def X(...) -> Stream:`` definitions.
  * Function-form decorator dispatch — when ``@bv.stream`` is applied to a
    function rather than a class, the function is invoked once at registration
    with placeholder upstream instances (cloned from the parameter-annotated
    classes) and its return value becomes the descriptor.
"""

from __future__ import annotations

import inspect
import typing
from types import FunctionType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from beava._aggregation import GroupBy

from beava._describe import format_describe
from beava._schema_v0 import extract_schema
from beava._stateless_ops import StatelessOpsMixin
from beava._types_core import FieldSpec


def _validate_salt(salt: int | None) -> None:
    """Phase 60 D-A1..D-A2: client-side salt cardinality validation.

    Server re-validates independently (D-A4). Raises TypeError with
    actionable message on any invalid value so users catch bad declarations
    before the network round-trip.
    """
    if salt is None:
        return
    # bool is a subclass of int — reject so @bv.stream(salt=True) errors.
    if not isinstance(salt, int) or isinstance(salt, bool):
        raise TypeError(
            f"salt must be int or None, got {type(salt).__name__}"
        )
    if salt < 2 or salt > 256:
        raise TypeError(f"salt must be in [2, 256], got {salt}")
    if salt & (salt - 1) != 0:
        raise TypeError(f"salt must be a power of 2, got {salt}")


class Stream(StatelessOpsMixin):
    """Marker / runtime type for streaming inputs.

    All v0 Stream flavours (source + derivation + intermediate view) subclass
    this. The :class:`StatelessOpsMixin` gives every subclass the 8 per-row
    op methods; subclasses implement ``_derive`` to build the next Stream in
    the chain.
    """

    # Phase 24-02: Dispatch marker used by :meth:`beava.App.push` to route
    # between the Stream fire-and-forget path and the Table push-through
    # path. Every Stream subclass inherits this attribute.
    _beava_kind: str = "stream"

    def _derive(
        self,
        *,
        schema: dict[str, FieldSpec],
        op: dict[str, Any],
    ) -> "StreamDerivation":
        """Default Stream-flavoured derive: wraps self + appended op."""
        return StreamDerivation(
            name=self._name,
            schema=schema,
            ops=list(self._ops) + [op],
            upstream=self,
            upstreams=[self],
        )

    def group_by(self, *keys: str) -> "GroupBy":
        """Begin a Stream→Table aggregation. Terminal call: ``.agg(**features)``.

        See :mod:`beava._aggregation`. Execution lands in Phase 22; the SDK
        infers and exposes the output Table's schema at registration time.
        """
        from beava._aggregation import GroupBy
        return GroupBy(self, keys)

    def join(
        self,
        other: Any,
        *,
        on: "str | list[str]",
        within: str | None = None,
        type: str = "inner",
    ) -> "Stream":
        """Join this Stream against a Stream (windowed) or Table (enrichment).

        See :mod:`beava._join`. Execution lands in Phase 23.
        """
        from beava._join import stream_join
        return stream_join(self, other, on=on, within=within, type_=type)


class StreamSource(Stream):
    """An external stream source — the node the engine ingests events into.

    Carries the declared schema plus stream-level options (``history_ttl``
    for Phase 25 watermark/retention work). Exposes the App-register compat
    protocol (``_beava_stream_name``, ``_compile``, ``_to_register_json``,
    ``_collect_registrations``).
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, FieldSpec],
        *,
        history_ttl: str | None = None,
        watermark_lateness: str | None = None,
        shard_key: str | tuple | None = None,
        salt: int | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._history_ttl = history_ttl
        self._watermark_lateness = watermark_lateness
        self._beava_shard_key = shard_key
        # Phase 60 D-A1..D-A3: per-stream salt cardinality for hot-key mitigation.
        # Validated client-side here (fast fail); server re-validates.
        _validate_salt(salt)
        self._beava_salt = salt
        # Sources have no upstream ops.
        self._ops: list[dict[str, Any]] = []
        self._upstreams: list[Stream] = []

    # --- public introspection ---
    def describe(self) -> dict[str, Any]:
        return format_describe(
            name=self._name,
            kind="stream",
            key=None,
            mode=None,
            schema=self._schema,
            history_ttl=self._history_ttl,
        )

    # --- App.register compat ---
    @property
    def _beava_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        """Compile to a RegisterRequest JSON dict (keyless stream source)."""
        from beava._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        from beava._serialize import collect_registrations
        return collect_registrations(self)

    def __repr__(self) -> str:
        return f"StreamSource({self._name!r})"


class StreamDerivation(Stream):
    """A Stream produced by applying stateless ops or a function derivation.

    Equivalent for three wire-surface cases:

      * Intermediate view inside a ``@bv.stream def ...`` body — returned by
        any stateless op applied to an upstream Stream.
      * The top-level descriptor produced by ``@bv.stream def X(...) -> Stream:``
        — has ``_name = func.__name__`` and ``_upstreams`` set by the decorator.
      * A chain-head returned by composition on an already-named derivation.

    Carries the upstream graph (``_upstreams`` — the direct parents used for
    DAG build) plus the linear op chain (``_ops``) that produced this
    schema from those upstreams.
    """

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        ops: list[dict[str, Any]],
        upstream: Stream | None,
        upstreams: list[Stream],
        func: FunctionType | None = None,
        type_hints: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._ops = ops
        self._upstream = upstream  # direct parent (last in chain)
        self._upstreams = upstreams  # parameter-declared parents (fan-in)
        self._func = func
        self._type_hints = type_hints or {}

    # --- introspection ---
    def describe(self) -> dict[str, Any]:
        return format_describe(
            name=self._name,
            kind="stream",
            key=None,
            mode=None,
            schema=self._schema,
        )

    # --- App.register compat ---
    @property
    def _beava_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        from beava._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        """Transitive REGISTER walk via the serializer (handles agg/join/union
        upstream dispatch uniformly)."""
        from beava._serialize import collect_registrations
        return collect_registrations(self)

    def __repr__(self) -> str:
        return f"StreamDerivation({self._name!r}, ops={len(self._ops)})"


# ---------------------------------------------------------------------------
# @bv.stream decorator
# ---------------------------------------------------------------------------


def _resolve_func_hints(func: FunctionType) -> dict[str, Any]:
    """Resolve a function's type annotations with best-effort forward-ref support.

    ``typing.get_type_hints`` fails when annotations reference names that live
    only in an enclosing function's locals (common under pytest). We first try
    ``get_type_hints`` with an augmented ``localns`` pulled from the nearest
    enclosing frames, then fall back to eval'ing each string annotation against
    function globals + caller locals.
    """
    # Collect caller-frame locals — works for decorators applied inside test
    # methods / closures.
    localns: dict[str, Any] = {}
    try:
        import sys as _sys
        frame = _sys._getframe(1)
        depth = 0
        while frame is not None and depth < 8:
            for k, v in frame.f_locals.items():
                localns.setdefault(k, v)
            frame = frame.f_back
            depth += 1
    except Exception:
        pass

    try:
        return typing.get_type_hints(func, localns=localns)
    except Exception:
        pass

    # Final fallback: eval each annotation individually against globals + localns.
    globalns = getattr(func, "__globals__", {})
    ns = {**globalns, **localns}
    out: dict[str, Any] = {}
    for name, ann in getattr(func, "__annotations__", {}).items():
        if isinstance(ann, str):
            try:
                out[name] = eval(ann, ns)
            except Exception:
                out[name] = ann  # leave as string — downstream will error
        else:
            out[name] = ann
    return out


def _build_stream_derivation_from_func(
    func: FunctionType,
    *,
    history_ttl: str | None = None,
) -> StreamDerivation:
    """Invoke a derivation function once to build its StreamDerivation."""
    hints = _resolve_func_hints(func)

    if "return" not in hints:
        raise TypeError(
            f"@bv.stream function {func.__name__!r} must declare a "
            f"return type annotation (``-> Stream``)"
        )
    ret = hints["return"]
    if not (isinstance(ret, type) and issubclass(ret, Stream)):
        raise TypeError(
            f"@bv.stream function {func.__name__!r} must return Stream; "
            f"annotation was {ret!r}"
        )

    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"derivation function {func.__name__!r} has no upstreams; "
            f"annotate parameters with your Stream/Table types"
        )

    # Resolve parameter annotations. We require each parameter to be
    # annotated with a descriptor — either a Stream/Table instance (the
    # source object produced by @bv.stream class X) or a descriptor class.
    upstreams: list[Any] = []
    for p in params:
        if p.name not in hints:
            raise TypeError(
                f"derivation function {func.__name__!r} parameter {p.name!r} "
                f"has no type annotation; annotate it with the upstream "
                f"Stream/Table"
            )
        upstreams.append(hints[p.name])

    # Invoke the function with the upstream descriptors themselves.
    result = func(*upstreams)

    if not isinstance(result, Stream):
        raise TypeError(
            f"derivation function {func.__name__!r} annotated -> Stream but "
            f"returned {type(result).__name__}"
        )

    # Rename the returned derivation to the function name and record its
    # parameter-declared upstreams (so DAG build can see fan-in).
    if isinstance(result, StreamDerivation):
        result._name = func.__name__
        result._upstreams = list(upstreams)
        result._func = func
        result._type_hints = hints
        return result

    # Result is a StreamSource (unusual — passthrough body) — wrap.
    return StreamDerivation(
        name=func.__name__,
        schema=dict(result._schema),
        ops=[],
        upstream=result,
        upstreams=list(upstreams),
        func=func,
        type_hints=hints,
    )


def stream(cls: type | FunctionType | None = None, *, history_ttl: str | None = None, watermark_lateness: str | None = None, shard_key: str | tuple | None = None, salt: int | None = None):  # noqa: D401
    # D-09 / TPC-DX-01: validate shard_key type client-side.
    if shard_key is not None and not isinstance(shard_key, (str, tuple)):
        raise TypeError(
            f"shard_key must be str, tuple[str, ...], or None, got {type(shard_key).__name__}"
        )
    if isinstance(shard_key, tuple):
        if len(shard_key) == 0:
            raise TypeError("shard_key tuple must not be empty")
        if not all(isinstance(k, str) for k in shard_key):
            raise TypeError("shard_key tuple elements must all be str")
    # Phase 25-02: validate history_ttl client-side (server also validates).
    if history_ttl is not None:
        from beava._table import _validate_duration_str
        _validate_duration_str(history_ttl, field="history_ttl")
    # D-11 / CORR-03: validate watermark_lateness client-side (server also parses).
    if watermark_lateness is not None:
        from beava._table import _validate_duration_str
        _validate_duration_str(watermark_lateness, field="watermark_lateness")
    # Phase 60 D-A1..D-A2: validate salt client-side BEFORE calling _stream_impl
    # so decorator-form (`@bv.stream(salt=10)`) fails fast during module import,
    # not later at registration.
    _validate_salt(salt)
    return _stream_impl(cls, history_ttl=history_ttl, watermark_lateness=watermark_lateness, shard_key=shard_key, salt=salt)


def _stream_impl(cls: type | FunctionType | None = None, *, history_ttl: str | None = None, watermark_lateness: str | None = None, shard_key: str | tuple | None = None, salt: int | None = None):
    """Decorator that declares a Stream — class form or function form.

    Class form::

        @bv.stream
        class Clicks:
            user_id: str
            url: str

        @bv.stream(history_ttl="90d")
        class Logins:
            user_id: str

    Function form (Plan 21-02)::

        @bv.stream
        def Checkouts(clicks: Clicks) -> bv.Stream:
            return clicks.filter(bv.col('page') == '/checkout')

    The function body is invoked *once* at registration time with the
    upstream descriptors themselves, to infer the output schema. Derivation
    function bodies must therefore be pure (no side effects).
    """

    def _wrap(target: Any) -> Stream:
        # Function form → build a StreamDerivation.
        if isinstance(target, FunctionType):
            return _build_stream_derivation_from_func(
                target, history_ttl=history_ttl
            )
        if isinstance(target, type):
            schema = extract_schema(target)
            return StreamSource(
                name=target.__name__,
                schema=schema,
                history_ttl=history_ttl,
                watermark_lateness=watermark_lateness,
                shard_key=shard_key,
                salt=salt,
            )
        raise TypeError(
            f"@bv.stream must be applied to a class or function, got "
            f"{type(target).__name__}"
        )

    if cls is not None:
        return _wrap(cls)
    return _wrap


__all__ = ["stream", "Stream", "StreamSource", "StreamDerivation"]
