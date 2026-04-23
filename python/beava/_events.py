"""``@bv.event`` decorator — class form and function form.

Public API (re-exported from beava.__init__):
  - event: decorator for declaring event sources and derivations

Runtime classes (internal, used by Plan 03-05 DAG walker):
  - EventSource: descriptor produced by class-form @bv.event
  - EventDerivation: descriptor produced by function-form @bv.event
"""

from __future__ import annotations

import datetime
import inspect
from typing import Any

from ._schema import FieldSpec, duration_to_ms, extract_schema, validate_duration_string
from ._types import py_type_to_field_type

__all__ = ["event", "EventSource", "EventDerivation"]


# ---------------------------------------------------------------------------
# Runtime descriptor classes
# ---------------------------------------------------------------------------


class EventSource:
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
        event_time_field: str | None,
        dedupe_key: str | None,
        dedupe_window_ms: int | None,
        keep_events_for_ms: int | None,
        tolerate_delay_ms: int | None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._event_time_field = event_time_field
        self._dedupe_key = dedupe_key
        self._dedupe_window_ms = dedupe_window_ms
        self._keep_events_for_ms = keep_events_for_ms
        self._tolerate_delay_ms = tolerate_delay_ms
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
            "event_time_field": self._event_time_field,
            "dedupe_key": self._dedupe_key,
            "dedupe_window_ms": self._dedupe_window_ms,
            "keep_events_for_ms": self._keep_events_for_ms,
            "tolerate_delay_ms": self._tolerate_delay_ms,
        }

    def __repr__(self) -> str:
        return f"EventSource({self._name!r})"


class EventDerivation:
    """Descriptor for a function-form @bv.event declaration.

    Produced when the decorator is applied to a function whose parameters are
    annotated with upstream EventSource / TableSource descriptors. The function
    is invoked ONCE at decoration time with the upstream descriptors as
    placeholder arguments; its return value is discarded (schema is derived
    from the function's return annotation or carried from upstream for Phase 4+).

    Exposes:
        _name: str                          — function name
        _schema: dict[str, FieldSpec]       — inherited from upstream / return (Phase 4+)
        _beava_kind: str = "derivation"
        _upstreams: list[str]               — upstream descriptor names
        _ops: list                          — op chain (empty in Phase 3)
        _output_kind: str = "event"
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
    ) -> None:
        self._name = name
        self._schema = schema
        self._upstreams = upstreams
        self._ops = ops
        self._output_kind = output_kind

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
    tolerate_delay: str | None,
    dedupe_key: str | None,
    dedupe_window: str | None,
) -> EventSource:
    """Apply @bv.event semantics to a class, returning an EventSource descriptor."""
    schema = extract_schema(cls)

    # SDK-DEC-08 devex-first: event_time is OPTIONAL.
    # If declared, it MUST be int (millis) or datetime.datetime.
    event_time_field: str | None = None
    if "event_time" in schema:
        py_t = schema["event_time"].py_type
        if py_t is not int and py_t is not datetime.datetime:
            raise TypeError(
                f"event_time field must be int (milliseconds epoch) or datetime.datetime, "
                f"got {py_t!r}; if omitted the server stamps wall-clock on receipt"
            )
        event_time_field = "event_time"

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

    tolerate_delay_ms: int | None = None
    if tolerate_delay is not None:
        validate_duration_string(tolerate_delay)
        tolerate_delay_ms = duration_to_ms(tolerate_delay)

    dedupe_window_ms: int | None = None
    if dedupe_window is not None:
        validate_duration_string(dedupe_window)
        dedupe_window_ms = duration_to_ms(dedupe_window)

    return EventSource(
        name=cls.__name__,
        schema=schema,
        event_time_field=event_time_field,
        dedupe_key=dedupe_key,
        dedupe_window_ms=dedupe_window_ms,
        keep_events_for_ms=keep_events_for_ms,
        tolerate_delay_ms=tolerate_delay_ms,
    )


def _decorate_event_function(
    func: Any,
    *,
    keep_events_for: str | None,
    tolerate_delay: str | None,
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
        if upstream_cls is inspect.Parameter.empty or not hasattr(upstream_cls, "_name"):
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
    tolerate_delay: str | None = None,
    dedupe_key: str | None = None,
    dedupe_window: str | None = None,
) -> Any:
    """Decorator to declare an event source or derivation.

    Supports two calling styles:

    **Class form (source):**
    ::

        @bv.event
        class Transaction:
            amount: float
            user_id: str
            event_time: int  # optional; server stamps wall-clock if omitted

        @bv.event(keep_events_for="7d", tolerate_delay="5s")
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
        tolerate_delay: Duration string for late-arrival tolerance (e.g. ``"5s"``).
        dedupe_key: Schema field name used for idempotency deduplication.
        dedupe_window: Deduplication window duration (e.g. ``"24h"``).

    Returns:
        An :class:`EventSource` (class form) or :class:`EventDerivation` (function form),
        or a decorator function when called with parentheses.

    Raises:
        TypeError: If a field type is unsupported, event_time type is wrong,
                   dedupe_key is not in schema, or a duration string is malformed.
    """
    kwargs = {
        "keep_events_for": keep_events_for,
        "tolerate_delay": tolerate_delay,
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
