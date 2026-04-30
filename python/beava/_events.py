"""``@bv.event`` decorator — class form and function form.

Public API (re-exported from beava.__init__):
  - event: decorator for declaring event sources and derivations

Runtime classes (internal, used by Plan 03-05 DAG walker):
  - EventSource: descriptor produced by class-form @bv.event
  - EventDerivation: descriptor produced by function-form @bv.event

Plan 12.6-08 strict-deny (no-event-time pivot, locked 2026-04-30):
  - `event_time` field declarations on the class form raise TypeError
  - `tolerate_delay` parameter raises TypeError
  - `event_time_field` parameter raises TypeError
  See `project_redis_shaped_no_event_time_ever` for the architectural
  commitment that drove this change.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from ._schema import FieldSpec, duration_to_ms, extract_schema, validate_duration_string
from ._types import py_type_to_field_type

if TYPE_CHECKING:
    from ._agg import GroupBy
    from ._col import _ExprAST

__all__ = ["event", "EventSource", "EventDerivation"]

# Valid cast target types (client-side SDK-OPS-07 check).
_VALID_CAST_TARGETS: frozenset[str] = frozenset({"str", "int", "float", "bool"})


# ---------------------------------------------------------------------------
# Op-method mixin
# ---------------------------------------------------------------------------


class _EventOpsMixin:
    """8 stateless op methods shared by EventSource and EventDerivation.

    Each method constructs a NEW EventDerivation — never mutating ``self``.
    The new derivation carries ``upstream=self`` and
    ``ops = [*existing_ops, new_op_dict]``.

    SDK-OPS-09: object identity of ``self`` is always distinct from the
    returned derivation; callers that hold a reference to an intermediate
    derivation will see its ``ops`` list unchanged after further chaining.
    """

    # Subclasses must expose these attributes:
    #   _name: str
    #   _schema: dict[str, FieldSpec]
    #   _ops: list[Any]              (empty for EventSource; non-empty for EventDerivation)
    #   _output_kind: str            (always "event" in this mixin)

    @property
    def ops(self) -> list[Any]:
        """Public read-only view of the accumulated op list."""
        return list(self._ops)  # type: ignore[attr-defined]

    @property
    def upstream(self) -> Any:
        """The direct parent in the derivation chain (None for sources)."""
        return getattr(self, "_upstream", None)

    def named(self, name: str) -> "EventDerivation":
        """Return a copy of this derivation with a different name.

        Allows ``Transaction.filter(...).named("BigTx")`` without re-deriving.
        Returns ``self`` if already an EventDerivation (copies with new name);
        for EventSource, wraps in a zero-op EventDerivation with the given name.
        """
        upstream_name = _source_name(self)
        return EventDerivation(
            name=name,
            schema=self._schema,  # type: ignore[attr-defined]
            upstreams=[upstream_name],
            ops=list(self._ops),  # type: ignore[attr-defined]
            output_kind=getattr(self, "_output_kind", "event"),
            upstream=self,
        )

    # ------------------------------------------------------------------ #
    # 8 op methods
    # ------------------------------------------------------------------ #

    def filter(self, expr: "_ExprAST") -> "EventDerivation":
        """Append a Filter op — keep only rows where *expr* is True."""
        op: dict[str, Any] = {"op": "filter", "expr": expr.to_expr_string()}
        return self._new_derivation(op)

    def select(self, *fields: str) -> "EventDerivation":
        """Append a Select op — keep only the named fields."""
        op = {"op": "select", "fields": list(fields)}
        return self._new_derivation(op)

    def drop(self, *fields: str) -> "EventDerivation":
        """Append a Drop op — remove the named fields."""
        op = {"op": "drop", "fields": list(fields)}
        return self._new_derivation(op)

    def rename(self, **mapping: str) -> "EventDerivation":
        """Append a Rename op — rename fields according to *mapping*."""
        op = {"op": "rename", "mapping": dict(mapping)}
        return self._new_derivation(op)

    def with_columns(self, **exprs: "_ExprAST") -> "EventDerivation":
        """Append a WithColumns op — add or overwrite derived fields."""
        op = {
            "op": "with_columns",
            "exprs": {name: e.to_expr_string() for name, e in exprs.items()},
        }
        return self._new_derivation(op)

    def map(self, **exprs: "_ExprAST") -> "EventDerivation":
        """Append a Map op (alias for with_columns; retains 'map' on the wire)."""
        op = {
            "op": "map",
            "exprs": {name: e.to_expr_string() for name, e in exprs.items()},
        }
        return self._new_derivation(op)

    def cast(self, **type_map: str) -> "EventDerivation":
        """Append a Cast op — change field types.

        Raises:
            ValueError: If any target type is not in {'str', 'int', 'float', 'bool'}.
        """
        for field, target in type_map.items():
            if target not in _VALID_CAST_TARGETS:
                raise ValueError(
                    f"invalid cast target for field {field!r}: {target!r}; "
                    f"must be one of {sorted(_VALID_CAST_TARGETS)}"
                )
        op = {"op": "cast", "type_map": dict(type_map)}
        return self._new_derivation(op)

    def fillna(self, **defaults: Any) -> "EventDerivation":
        """Append a Fillna op — replace null values with given defaults.

        Raises:
            ValueError: If any default value is None (fillna with null is meaningless).
        """
        for field, val in defaults.items():
            if val is None:
                raise ValueError(
                    f"fillna default for field {field!r} cannot be None; "
                    "use a concrete scalar value"
                )
        op = {"op": "fillna", "defaults": dict(defaults)}
        return self._new_derivation(op)

    def group_by(self, *keys: str) -> "GroupBy":
        """Start a group_by.agg() aggregation pipeline (SDK-AGG-01).

        Returns a GroupBy builder; no server call is made until .agg() is invoked.

        Args:
            *keys: One or more schema field names to group by.

        Raises:
            TypeError: If any key is not a string.
            ValueError: If no keys are given, or any key is not in the upstream schema.
        """
        from ._agg import GroupBy  # noqa: PLC0415 — avoid circular import at module level

        if not keys:
            raise ValueError(
                "group_by() requires at least one key; "
                "e.g. Event.group_by('user_id')"
            )
        for k in keys:
            if not isinstance(k, str):
                raise TypeError(
                    f"group_by keys must be strings; got {type(k).__name__!r} "
                    f"(value: {k!r})"
                )
            if k not in self._schema:  # type: ignore[attr-defined]
                raise ValueError(
                    f"group_by key {k!r} is not in schema "
                    f"(available: {sorted(self._schema.keys())})"  # type: ignore[attr-defined]
                )
        return GroupBy(self, list(keys))

    # ------------------------------------------------------------------ #
    # Internal helper
    # ------------------------------------------------------------------ #

    def _new_derivation(self, op: dict[str, Any]) -> "EventDerivation":
        """Construct a new EventDerivation with *op* appended to the ops list."""
        existing_ops: list[Any] = list(self._ops)  # type: ignore[attr-defined]
        upstream_name = _source_name(self)
        return EventDerivation(
            name=upstream_name,  # placeholder; callers use .named() to assign
            schema=self._schema,  # type: ignore[attr-defined]
            upstreams=[upstream_name],
            ops=[*existing_ops, op],
            output_kind=getattr(self, "_output_kind", "event"),
            upstream=self,
        )


def _source_name(obj: Any) -> str:
    """Return the effective source name for *obj* (EventSource or EventDerivation)."""
    # For EventDerivation chains, trace back to the root source name.
    current: Any = obj
    while hasattr(current, "_upstream") and current._upstream is not None:
        current = current._upstream
    name: str = current._name
    return name


# ---------------------------------------------------------------------------
# Runtime descriptor classes
# ---------------------------------------------------------------------------


class EventSource(_EventOpsMixin):
    """Descriptor for a class-form @bv.event declaration.

    Produced at decoration time; consumed by Plan 03-05 DAG walker and
    Plan 03-06 bv.App.register() serializer.

    Exposes:
        _name: str                          — class name
        _schema: dict[str, FieldSpec]       — ordered field map
        _beava_kind: str = "event"
        _upstreams: list[str] = []          — always empty for sources
        _ops: list = []                     — always empty for sources
        _to_register_json() -> dict         — wire JSON matching Phase 2 contract
    """

    _beava_kind: str = "event"

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        dedupe_key: str | None,
        dedupe_window_ms: int | None,
        keep_events_for_ms: int | None,
    ) -> None:
        # Plan 12.6-08 (no-event-time pivot): event_time_field and
        # tolerate_delay_ms parameters were deleted. Sources only carry
        # processing-time-safe metadata.
        self._name = name
        self._schema = schema
        self._dedupe_key = dedupe_key
        self._dedupe_window_ms = dedupe_window_ms
        self._keep_events_for_ms = keep_events_for_ms
        self._upstreams: list[str] = []
        self._ops: list[Any] = []

    def _to_register_json(self) -> dict[str, Any]:
        """Return JSON dict matching Phase 2 EventDescriptor wire shape."""
        return {
            "kind": "event",
            "name": self._name,
            "schema": {
                "fields": {
                    n: py_type_to_field_type(s.py_type) for n, s in self._schema.items()
                },
                "optional_fields": [n for n, s in self._schema.items() if s.optional],
            },
            "dedupe_key": self._dedupe_key,
            "dedupe_window_ms": self._dedupe_window_ms,
            "keep_events_for_ms": self._keep_events_for_ms,
        }

    def __repr__(self) -> str:
        return f"EventSource({self._name!r})"


class EventDerivation(_EventOpsMixin):
    """Descriptor for a function-form @bv.event declaration OR a fluent-op derivation.

    Produced when the decorator is applied to a function whose parameters are
    annotated with upstream EventSource / TableSource descriptors, or when an
    op method (`.filter()`, `.select()`, etc.) is called on an EventSource /
    EventDerivation.

    Exposes:
        _name: str                          — function/derivation name
        _schema: dict[str, FieldSpec]       — inherited from upstream / return (Phase 4+)
        _beava_kind: str = "derivation"
        _upstreams: list[str]               — upstream descriptor names
        _ops: list                          — op chain
        _output_kind: str = "event"
        _upstream: EventSource|EventDerivation|None  — direct parent (fluent-API only)
        upstream: property                  — public alias for _upstream
        ops: property                       — public read-only list copy
        _to_register_json() -> dict         — wire JSON matching Phase 2 DerivationDescriptor
    """

    _beava_kind: str = "derivation"

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        upstreams: list[str],
        ops: list[Any],
        output_kind: str = "event",
        upstream: Any = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._upstreams = upstreams
        self._ops = ops
        self._output_kind = output_kind
        self._upstream = upstream  # direct parent in fluent chain (None for @bv.event fn-form)

    def _to_register_json(self) -> dict[str, Any]:
        """Return JSON dict matching Phase 2 DerivationDescriptor wire shape."""
        return {
            "kind": "derivation",
            "name": self._name,
            "output_kind": self._output_kind,
            "upstreams": list(self._upstreams),
            "ops": list(self._ops),
            "schema": {
                "fields": {
                    n: py_type_to_field_type(s.py_type) for n, s in self._schema.items()
                },
                "optional_fields": [n for n, s in self._schema.items() if s.optional],
            },
            "table_primary_key": None,
        }

    def __repr__(self) -> str:
        return f"EventDerivation({self._name!r}, upstreams={self._upstreams!r})"


# ---------------------------------------------------------------------------
# Decorator implementation
# ---------------------------------------------------------------------------


def _decorate_event_class(
    cls: type,
    *,
    keep_events_for: str | None,
    dedupe_key: str | None,
    dedupe_window: str | None,
) -> EventSource:
    """Apply @bv.event semantics to a class, returning an EventSource descriptor."""
    schema = extract_schema(cls)

    # Plan 12.6-08 strict-deny: event_time field declarations are no longer
    # supported per the no-event-time pivot (locked 2026-04-30). Beava is
    # processing-time only; the server stamps wall-clock arrival time on
    # every push, and windowed/decay/velocity ops bucket on that.
    if "event_time" in schema:
        raise TypeError(
            "event_time field on @bv.event is no longer supported per the "
            "no-event-time pivot (2026-04-30). Beava is processing-time only; "
            "remove the event_time field from your event class. The server "
            "stamps wall-clock arrival time on every push automatically."
        )

    # Validate dedupe_key is in schema
    if dedupe_key is not None and dedupe_key not in schema:
        raise TypeError(
            f"dedupe_key {dedupe_key!r} is not a declared schema field; "
            f"available fields: {list(schema.keys())}"
        )

    # Validate and convert duration strings
    keep_events_for_ms: int | None = None
    if keep_events_for is not None:
        validate_duration_string(keep_events_for)
        keep_events_for_ms = duration_to_ms(keep_events_for)

    dedupe_window_ms: int | None = None
    if dedupe_window is not None:
        validate_duration_string(dedupe_window)
        dedupe_window_ms = duration_to_ms(dedupe_window)

    return EventSource(
        name=cls.__name__,
        schema=schema,
        dedupe_key=dedupe_key,
        dedupe_window_ms=dedupe_window_ms,
        keep_events_for_ms=keep_events_for_ms,
    )


def _decorate_event_function(
    func: Any,
    *,
    keep_events_for: str | None,
    dedupe_key: str | None,
    dedupe_window: str | None,
) -> EventDerivation:
    """Apply @bv.event semantics to a function, returning an EventDerivation descriptor.

    The function is invoked ONCE at decoration time with its parameter-annotated
    upstream descriptors as placeholder arguments. The returned value's _schema
    is carried forward (or {} if unavailable in Phase 3).
    """
    sig = inspect.signature(func)

    upstream_names: list[str] = []
    placeholder_args: list[Any] = []

    for param_name, param in sig.parameters.items():
        # Read the annotation directly from the parameter object.
        # This works even when the annotated type is a local variable (a decorated
        # EventSource / TableSource) that would not be resolvable by
        # typing.get_type_hints() in the function's defining module namespace.
        upstream_cls = param.annotation
        if (
            upstream_cls is inspect.Parameter.empty
            or not hasattr(upstream_cls, "_beava_kind")
            or not hasattr(upstream_cls, "_name")
        ):
            raise TypeError(
                f"@bv.event function form: parameter {param_name!r} must be annotated "
                f"with a @bv.event- or @bv.table-decorated descriptor "
                f"(got {upstream_cls!r})"
            )
        upstream_names.append(upstream_cls._name)
        placeholder_args.append(upstream_cls)

    result = func(*placeholder_args)

    # Carry schema from the result descriptor if available (Phase 3: passthrough)
    schema: dict[str, FieldSpec] = getattr(result, "_schema", {})
    ops: list[Any] = getattr(result, "_ops", [])

    return EventDerivation(
        name=func.__name__,
        schema=schema,
        upstreams=upstream_names,
        ops=ops,
        output_kind="event",
    )


def event(
    arg: Any = None,
    *,
    keep_events_for: str | None = None,
    dedupe_key: str | None = None,
    dedupe_window: str | None = None,
    **legacy_kwargs: Any,
) -> Any:
    """Decorator to declare an event source or derivation.

    Supports two calling styles:

    **Class form (source):**
    ::

        @bv.event
        class Transaction:
            amount: float
            user_id: str

        @bv.event(keep_events_for="7d")
        class Transaction:
            ...

    **Function form (derivation):**
    ::

        @bv.event
        def Checkouts(source: Transaction) -> object:
            return source

    Args:
        arg: The class or function being decorated (bare ``@bv.event`` form).
             ``None`` when called with parentheses (``@bv.event(...)``).
        keep_events_for: Duration string for event retention (e.g. ``"7d"``).
        dedupe_key: Schema field name used for idempotency deduplication.
        dedupe_window: Deduplication window duration (e.g. ``"24h"``).

    Returns:
        An :class:`EventSource` (class form) or :class:`EventDerivation` (function form),
        or a decorator function when called with parentheses.

    Raises:
        TypeError:
          * If a field type is unsupported, dedupe_key is not in schema, or
            a duration string is malformed.
          * If a class declares an ``event_time`` field (no longer supported
            per the 2026-04-30 no-event-time pivot).
          * If ``tolerate_delay`` or ``event_time_field`` is passed as a
            keyword argument (no longer supported per the same pivot).
    """
    # Plan 12.6-08 strict-deny: legacy event-time keyword arguments are
    # rejected at decorator time. Per D-03 (CONTEXT.md) the no-event-time
    # pivot has zero compat surface — no parse-and-strip, no warn-then-error.
    if "tolerate_delay" in legacy_kwargs:
        raise TypeError(
            "tolerate_delay parameter on @bv.event is no longer supported per "
            "the no-event-time pivot (2026-04-30). Remove the parameter; the "
            "server stamps processing time on every push so there is no late-"
            "arrival window to tolerate."
        )
    if "event_time_field" in legacy_kwargs:
        raise TypeError(
            "event_time_field parameter on @bv.event is no longer supported "
            "per the no-event-time pivot (2026-04-30). Remove the parameter; "
            "the server stamps processing time on every push automatically."
        )
    if legacy_kwargs:
        # Forward any other unexpected kwargs to the canonical TypeError shape.
        first = next(iter(legacy_kwargs))
        raise TypeError(
            f"event() got an unexpected keyword argument {first!r}"
        )

    kwargs = {
        "keep_events_for": keep_events_for,
        "dedupe_key": dedupe_key,
        "dedupe_window": dedupe_window,
    }

    if arg is None:
        # Called with parentheses: @bv.event(...) — return a decorator
        def _decorator(target: Any) -> Any:
            if inspect.isclass(target):
                return _decorate_event_class(target, **kwargs)
            if callable(target):
                return _decorate_event_function(target, **kwargs)
            raise TypeError(
                f"@bv.event can only be applied to a class or function, "
                f"got {type(target).__name__!r}"
            )

        return _decorator

    # Bare @bv.event — arg is the class or function
    if inspect.isclass(arg):
        return _decorate_event_class(arg, **kwargs)
    if callable(arg):
        return _decorate_event_function(arg, **kwargs)

    raise TypeError(
        f"@bv.event can only be applied to a class or function, "
        f"got {type(arg).__name__!r}"
    )
