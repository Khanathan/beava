"""``@tl.stream`` decorator + Stream / StreamSource / StreamDerivation runtime types.

Plan 21-01 shipped the class form. Plan 21-02 adds:
  * :class:`StatelessOpsMixin` on :class:`Stream` — gives every Stream the 8
    per-row ops (filter / map / select / drop / rename / with_columns /
    cast / fillna).
  * :class:`StreamDerivation` — the descriptor returned by stateless ops and
    by function-form ``@tl.stream def X(...) -> Stream:`` definitions.
  * Function-form decorator dispatch — when ``@tl.stream`` is applied to a
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
    from tally._aggregation import GroupBy

from tally._describe import format_describe
from tally._schema_v0 import extract_schema
from tally._stateless_ops import StatelessOpsMixin
from tally._types_core import FieldSpec


class Stream(StatelessOpsMixin):
    """Marker / runtime type for streaming inputs.

    All v0 Stream flavours (source + derivation + intermediate view) subclass
    this. The :class:`StatelessOpsMixin` gives every subclass the 8 per-row
    op methods; subclasses implement ``_derive`` to build the next Stream in
    the chain.
    """

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

        See :mod:`tally._aggregation`. Execution lands in Phase 22; the SDK
        infers and exposes the output Table's schema at registration time.
        """
        from tally._aggregation import GroupBy
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

        See :mod:`tally._join`. Execution lands in Phase 23.
        """
        from tally._join import stream_join
        return stream_join(self, other, on=on, within=within, type_=type)


class StreamSource(Stream):
    """An external stream source — the node the engine ingests events into.

    Carries the declared schema plus stream-level options (``history_ttl``
    for Phase 25 watermark/retention work). Exposes the App-register compat
    protocol (``_tally_stream_name``, ``_compile``, ``_to_register_json``,
    ``_collect_registrations``).
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, FieldSpec],
        *,
        history_ttl: str | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._history_ttl = history_ttl
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
    def _tally_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        """Compile to a RegisterRequest JSON dict (keyless stream source)."""
        d: dict[str, Any] = {
            "name": self._name,
            "key_field": None,
            "features": [],
            "fields": {
                fname: {
                    "type": spec.py_type.__name__,
                    "optional": spec.optional,
                }
                for fname, spec in self._schema.items()
            },
        }
        if self._history_ttl is not None:
            d["history_ttl"] = self._history_ttl
        return d

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        return [self._compile()]

    def __repr__(self) -> str:
        return f"StreamSource({self._name!r})"


class StreamDerivation(Stream):
    """A Stream produced by applying stateless ops or a function derivation.

    Equivalent for three wire-surface cases:

      * Intermediate view inside a ``@tl.stream def ...`` body — returned by
        any stateless op applied to an upstream Stream.
      * The top-level descriptor produced by ``@tl.stream def X(...) -> Stream:``
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
    def _tally_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self._name,
            "key_field": None,
            "features": [],
            "fields": {
                fname: {
                    "type": spec.py_type.__name__,
                    "optional": spec.optional,
                }
                for fname, spec in self._schema.items()
            },
        }
        if self._ops:
            d["ops"] = list(self._ops)
        if self._upstreams:
            d["depends_on"] = [u._name for u in self._upstreams]
        return d

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        """Transitive REGISTER walk: upstreams (depth-first) then self.

        Dedupe happens at App.register level; here we just append in post-order.
        """
        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        def walk(node: Stream) -> None:
            # Sources and derivations both implement _collect_registrations,
            # but StreamDerivation recurses through _upstreams explicitly so
            # we can carry the dedupe set.
            if isinstance(node, StreamDerivation):
                for u in node._upstreams:
                    walk(u)
            elif hasattr(node, "_collect_registrations"):
                for reg in node._collect_registrations():
                    if reg["name"] not in seen:
                        seen.add(reg["name"])
                        out.append(reg)
            if node is self or isinstance(node, StreamDerivation):
                # Append the derivation's own frame once we've covered its upstreams.
                pass

        # Walk upstreams first (left-to-right, depth-first).
        for u in self._upstreams:
            walk(u)
        # Then append our own frame if not yet seen.
        if self._name not in seen:
            seen.add(self._name)
            out.append(self._compile())
        return out

    def __repr__(self) -> str:
        return f"StreamDerivation({self._name!r}, ops={len(self._ops)})"


# ---------------------------------------------------------------------------
# @tl.stream decorator
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
            f"@tl.stream function {func.__name__!r} must declare a "
            f"return type annotation (``-> Stream``)"
        )
    ret = hints["return"]
    if not (isinstance(ret, type) and issubclass(ret, Stream)):
        raise TypeError(
            f"@tl.stream function {func.__name__!r} must return Stream; "
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
    # source object produced by @tl.stream class X) or a descriptor class.
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


def stream(cls: type | FunctionType | None = None, *, history_ttl: str | None = None):
    """Decorator that declares a Stream — class form or function form.

    Class form::

        @tl.stream
        class Clicks:
            user_id: str
            url: str

        @tl.stream(history_ttl="90d")
        class Logins:
            user_id: str

    Function form (Plan 21-02)::

        @tl.stream
        def Checkouts(clicks: Clicks) -> tl.Stream:
            return clicks.filter(tl.col('page') == '/checkout')

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
            )
        raise TypeError(
            f"@tl.stream must be applied to a class or function, got "
            f"{type(target).__name__}"
        )

    if cls is not None:
        return _wrap(cls)
    return _wrap


__all__ = ["stream", "Stream", "StreamSource", "StreamDerivation"]
