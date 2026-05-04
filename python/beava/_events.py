"""@bv.event decorator + EventSource/EventDerivation/GroupBy classes — Phase 13.5 Plan 03.

@bv.event has two forms:

- **Class form:** declares an event source with a typed schema. Each
  annotated field becomes a schema column. Per-source kwargs
  (``keep_events_for``, ``cold_after``, ``dedupe_key``, ``dedupe_window``)
  are accepted via the decorator-factory shape ``@bv.event(...)``.

- **Function form:** declares an event derivation — a pipeline that chains
  on one or more upstream event sources. The function body builds a chain
  via the ``filter / select / drop / rename / with_columns / cast / fillna /
  group_by / agg`` chain methods.

ADR-003 amendment 2026-05-03: ``events.group_by()`` (no args) returns a
*global* GroupBy whose subsequent ``.agg(...)`` produces a chain step with
``keys=[]``. ``events.agg(**aggs)`` is a direct shorthand equivalent to
``events.group_by().agg(**aggs)``.

`project_redis_shaped_no_event_time_ever` (locked 2026-04-30) prohibits
event-time semantics in v0; the decorator rejects ``event_time`` schema
fields and the ``tolerate_delay`` / ``event_time_field`` decorator kwargs
with ``TypeError`` at decorator-time.
"""
from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

from beava._col import _Expr

_FORBIDDEN_FIELD_NAMES = ("event_time",)
_FORBIDDEN_DECORATOR_KWARGS = ("tolerate_delay", "event_time_field")
_VALID_CAST_TARGETS = ("str", "int", "float", "bool")


# ── Chain mixin ─────────────────────────────────────────────────────────────


class _ChainMixin:
    """Common chain methods for ``EventSource`` and ``EventDerivation``."""

    _chain: list[dict[str, Any]]
    _name: str
    _schema: dict[str, type] | None

    def filter(self, expr: _Expr) -> "EventDerivation":
        return _make_derivation(self, {"op": "filter", "expr": expr.to_expr_string()})

    def select(self, *cols: str) -> "EventDerivation":
        return _make_derivation(self, {"op": "select", "cols": list(cols)})

    def drop(self, *cols: str) -> "EventDerivation":
        return _make_derivation(self, {"op": "drop", "cols": list(cols)})

    def rename(self, **mapping: str) -> "EventDerivation":
        return _make_derivation(
            self, {"op": "rename", "mapping": dict(mapping)}
        )

    def with_columns(self, **exprs: Any) -> "EventDerivation":
        return _make_derivation(
            self,
            {
                "op": "with_columns",
                "exprs": {
                    k: (v.to_expr_string() if isinstance(v, _Expr) else v)
                    for k, v in exprs.items()
                },
            },
        )

    def map(self, **exprs: Any) -> "EventDerivation":
        return self.with_columns(**exprs)

    def cast(self, **type_map: str) -> "EventDerivation":
        for k, v in type_map.items():
            if v not in _VALID_CAST_TARGETS:
                raise ValueError(
                    f"cast target {v!r} for {k!r} not in {_VALID_CAST_TARGETS}"
                )
        return _make_derivation(
            self, {"op": "cast", "type_map": dict(type_map)}
        )

    def fillna(self, **defaults: Any) -> "EventDerivation":
        return _make_derivation(
            self, {"op": "fillna", "defaults": dict(defaults)}
        )

    def group_by(self, *keys: str) -> "GroupBy":
        # ADR-003: empty *keys allowed → global GroupBy.
        return GroupBy(parent=self, keys=tuple(keys))

    def agg(self, **named: Any) -> "EventDerivation":
        """ADR-003 direct shorthand: events.agg(...) == events.group_by().agg(...)."""
        return self.group_by().agg(**named)

    def named(self, name: str) -> "EventDerivation":
        """Tag a derivation with a stable name (used by v0 tests + register output)."""
        # Implemented as a chain-noop step ``{"op": "rename_self", "name": ...}``
        # so it survives the chain serialization unchanged but exposes a
        # deterministic ``_name`` for downstream registration.
        d = _make_derivation(self, {"op": "rename_self", "name": name})
        d._name = name
        return d


# ── Public classes ──────────────────────────────────────────────────────────


class EventSource(_ChainMixin):
    """An event source declared via ``@bv.event class Foo: ...``."""

    def __init__(
        self, name: str, schema: dict[str, type], **kwargs: Any
    ) -> None:
        self._name = name
        self._schema = schema
        self._chain: list[dict[str, Any]] = []
        self._keep_events_for = kwargs.get("keep_events_for")
        self._cold_after = kwargs.get("cold_after")
        self._dedupe_key = kwargs.get("dedupe_key")
        self._dedupe_window = kwargs.get("dedupe_window")
        self._kind = "event_source"


class EventDerivation(_ChainMixin):
    """An event derivation chained on top of an upstream source/derivation."""

    def __init__(
        self,
        name: str,
        parent: Any,
        chain: list[dict[str, Any]],
    ) -> None:
        self._name = name
        self._parent = parent
        self._chain = chain
        self._schema: dict[str, type] | None = None  # propagated at register-time
        self._kind = "event_derivation"
        self._key_cols: list[str] | None = None


def _make_derivation(parent: Any, step: dict[str, Any]) -> EventDerivation:
    new_chain = list(parent._chain) + [step]
    name = f"{parent._name}__derived_{len(new_chain)}"
    return EventDerivation(name=name, parent=parent, chain=new_chain)


class GroupBy:
    """Intermediate group-by object — call ``.agg(**aggs)`` to materialize."""

    def __init__(self, parent: Any, keys: tuple[str, ...]) -> None:
        self._parent = parent
        self._keys = keys

    def agg(self, **named: Any) -> EventDerivation:
        """Apply named aggregations.

        Each value is either a Plan-04 ``AggDescriptor`` (which exposes
        ``to_dict()``) or a primitive that is serialized directly.
        """
        new_chain = list(self._parent._chain) + [
            {
                "op": "agg",
                "keys": list(self._keys),
                "aggs": {
                    name: (agg.to_dict() if hasattr(agg, "to_dict") else agg)
                    for name, agg in named.items()
                },
            }
        ]
        d = EventDerivation(
            name=f"{self._parent._name}__agg",
            parent=self._parent,
            chain=new_chain,
        )
        d._kind = "aggregation"
        d._key_cols = list(self._keys)
        return d


# ── @bv.event decorator ─────────────────────────────────────────────────────


def _validate_class_event(cls: type, kwargs: dict[str, Any]) -> None:
    for forbidden_kw in _FORBIDDEN_DECORATOR_KWARGS:
        if forbidden_kw in kwargs:
            raise TypeError(
                f"@bv.event {forbidden_kw!r} kwarg is not supported in v0 "
                f"per project_redis_shaped_no_event_time_ever (locked 2026-04-30); "
                f"server stamps wall-clock processing time on every push."
            )
    hints = get_type_hints(cls, include_extras=True)
    for forbidden_field in _FORBIDDEN_FIELD_NAMES:
        if forbidden_field in hints:
            raise TypeError(
                f"@bv.event class field {forbidden_field!r} is not supported in v0 "
                f"per project_redis_shaped_no_event_time_ever; server stamps "
                f"wall-clock processing time on every push."
            )


def _make_event_source(cls: type, kwargs: dict[str, Any]) -> type:
    _validate_class_event(cls, kwargs)
    hints = get_type_hints(cls, include_extras=True)
    src = EventSource(name=cls.__name__, schema=hints, **kwargs)
    # Attach EventSource fields to the class for static-attribute access
    # (e.g., ``_Txn._schema`` / ``_Txn._chain``).
    for attr in (
        "_name",
        "_schema",
        "_chain",
        "_keep_events_for",
        "_cold_after",
        "_dedupe_key",
        "_dedupe_window",
        "_kind",
    ):
        setattr(cls, attr, getattr(src, attr))
    # Attach chain methods as static methods (so ``Cls.filter(...)`` works).
    for method_name in (
        "filter",
        "select",
        "drop",
        "rename",
        "with_columns",
        "map",
        "cast",
        "fillna",
        "group_by",
        "agg",
        "named",
    ):
        bound = getattr(src, method_name)
        setattr(cls, method_name, staticmethod(bound))
    return cls


def _make_event_derivation(fn: Callable[..., Any]) -> EventDerivation:
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"@bv.event function {fn.__name__!r} must take at least one parameter"
        )
    # With ``from __future__ import annotations`` (PEP 563), ``p.annotation``
    # may be a string. Resolve via ``get_type_hints`` against the function's
    # globals + locals so the annotation is the actual upstream class.
    try:
        resolved = get_type_hints(fn)
    except Exception:
        resolved = {}
    upstream_proxies: list[Any] = []
    for p in params:
        ann = resolved.get(p.name, p.annotation)
        if isinstance(ann, str):
            # Last-ditch: try fn.__globals__ for the bare name.
            ann = fn.__globals__.get(ann, ann)
        upstream_proxies.append(ann)
    result = fn(*upstream_proxies)
    if not isinstance(result, EventDerivation):
        raise TypeError(
            f"@bv.event function {fn.__name__!r} must return a chain "
            f"(filter/select/group_by/agg/...); got {type(result).__name__}"
        )
    result._name = fn.__name__
    return result


def event(cls_or_fn: Any = None, /, **kwargs: Any) -> Any:
    """``@bv.event`` decorator — class form OR function form.

    Class form::

        @bv.event
        class Click:
            user_id: str
            page: str

    Function form::

        @bv.event
        def BigClick(click: Click):
            return click.filter(bv.col("page") == "/checkout")

    Decorator-factory form (per-source kwargs)::

        @bv.event(keep_events_for="30d", dedupe_key="id", dedupe_window="5m")
        class Login:
            id: str

    Forbidden kwargs (raise ``TypeError`` at decorator-time):
        - ``event_time_field`` / ``tolerate_delay`` (no event-time in v0)

    Forbidden class fields (raise ``TypeError``): ``event_time``.
    """
    if cls_or_fn is None:
        # Decorator factory: @bv.event(cold_after="1d") class Foo: ...
        def _wrap(target: Any) -> Any:
            if inspect.isclass(target):
                return _make_event_source(target, kwargs)
            return _make_event_derivation(target)

        return _wrap
    if inspect.isclass(cls_or_fn):
        return _make_event_source(cls_or_fn, {})
    return _make_event_derivation(cls_or_fn)
