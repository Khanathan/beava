"""``@bv.table`` decorator — class form and function form.

Public API (re-exported from beava.__init__):
  - table: decorator for declaring table sources and derivations

Runtime classes (internal, used by Plan 03-05 DAG walker):
  - TableSource: descriptor produced by class-form @bv.table
  - TableDerivation: descriptor produced by function-form @bv.table
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from ._schema import FieldSpec, duration_to_ms, extract_schema, validate_duration_string
from ._types import py_type_to_field_type

if TYPE_CHECKING:
    from ._col import _ExprAST

__all__ = ["table", "TableSource", "TableDerivation"]

# Sentinel used to detect bare @bv.table (no parentheses) when the first
# positional argument is a class.
_SENTINEL = object()

# Valid cast target types (client-side SDK-OPS-07 check).
_VALID_CAST_TARGETS: frozenset[str] = frozenset({"str", "int", "float", "bool"})


# ---------------------------------------------------------------------------
# Op-method mixin for table descriptors
# ---------------------------------------------------------------------------


class _TableOpsMixin:
    """8 stateless op methods shared by TableSource and TableDerivation.

    Same contract as _EventOpsMixin in _events.py, but returns TableDerivation
    and applies table-specific checks:
      - .drop() rejects key fields (SDK-OPS-03 table half)
      - .rename() cascades the key list (SDK-OPS-04 table half)

    SDK-OPS-09: every method returns a new object; self is never mutated.
    """

    # Subclasses expose:
    #   _name: str
    #   _schema: dict[str, FieldSpec]
    #   _ops: list[Any]
    #   _primary_key / _table_primary_key: list[str]  (key fields)

    @property
    def ops(self) -> list[Any]:
        """Public read-only copy of the accumulated op list."""
        return list(self._ops)  # type: ignore[attr-defined]

    @property
    def upstream(self) -> Any:
        """The direct parent in the derivation chain (None for sources)."""
        return getattr(self, "_upstream", None)

    @property
    def key(self) -> list[str]:
        """Current key field list (may have been cascaded by rename)."""
        if hasattr(self, "_table_primary_key"):
            return list(self._table_primary_key)  # type: ignore[attr-defined]
        return list(self._primary_key)  # type: ignore[attr-defined]

    def named(self, name: str) -> "TableDerivation":
        """Return a copy of this derivation with a different name."""
        upstream_name = _table_source_name(self)  # type: ignore[arg-type]
        return TableDerivation(
            name=name,
            schema=self._schema,  # type: ignore[attr-defined]
            upstreams=[upstream_name],
            ops=list(self._ops),  # type: ignore[attr-defined]
            output_kind=getattr(self, "_output_kind", "table"),
            table_primary_key=self.key,
            upstream=self,
        )

    # ------------------------------------------------------------------ #
    # 8 op methods
    # ------------------------------------------------------------------ #

    def filter(self, expr: "_ExprAST") -> "TableDerivation":
        op: dict[str, Any] = {"op": "filter", "expr": expr.to_expr_string()}
        return self._new_table_derivation(op, key_override=None)

    def select(self, *fields: str) -> "TableDerivation":
        op = {"op": "select", "fields": list(fields)}
        return self._new_table_derivation(op, key_override=None)

    def drop(self, *fields: str) -> "TableDerivation":
        """Append a Drop op. Raises ValueError if any field is a key field."""
        current_key = self.key
        for f in fields:
            if f in current_key:
                raise ValueError(
                    f"cannot drop key field {f!r} from table derivation; "
                    f"key fields are: {current_key}"
                )
        op = {"op": "drop", "fields": list(fields)}
        return self._new_table_derivation(op, key_override=None)

    def rename(self, **mapping: str) -> "TableDerivation":
        """Append a Rename op. Cascades the key list if any key field is renamed."""
        current_key = self.key
        new_key = [mapping.get(k, k) for k in current_key]
        op = {"op": "rename", "mapping": dict(mapping)}
        return self._new_table_derivation(op, key_override=new_key)

    def with_columns(self, **exprs: "_ExprAST") -> "TableDerivation":
        op = {
            "op": "with_columns",
            "exprs": {name: e.to_expr_string() for name, e in exprs.items()},
        }
        return self._new_table_derivation(op, key_override=None)

    def map(self, **exprs: "_ExprAST") -> "TableDerivation":
        op = {
            "op": "map",
            "exprs": {name: e.to_expr_string() for name, e in exprs.items()},
        }
        return self._new_table_derivation(op, key_override=None)

    def cast(self, **type_map: str) -> "TableDerivation":
        for field, target in type_map.items():
            if target not in _VALID_CAST_TARGETS:
                raise ValueError(
                    f"invalid cast target for field {field!r}: {target!r}; "
                    f"must be one of {sorted(_VALID_CAST_TARGETS)}"
                )
        op = {"op": "cast", "type_map": dict(type_map)}
        return self._new_table_derivation(op, key_override=None)

    def fillna(self, **defaults: Any) -> "TableDerivation":
        for field, val in defaults.items():
            if val is None:
                raise ValueError(
                    f"fillna default for field {field!r} cannot be None"
                )
        op = {"op": "fillna", "defaults": dict(defaults)}
        return self._new_table_derivation(op, key_override=None)

    # ------------------------------------------------------------------ #
    # Internal helper
    # ------------------------------------------------------------------ #

    def _new_table_derivation(
        self, op: dict[str, Any], key_override: list[str] | None
    ) -> "TableDerivation":
        existing_ops: list[Any] = list(self._ops)  # type: ignore[attr-defined]
        upstream_name = _table_source_name(self)  # type: ignore[arg-type]
        effective_key = key_override if key_override is not None else self.key
        return TableDerivation(
            name=upstream_name,
            schema=self._schema,  # type: ignore[attr-defined]
            upstreams=[upstream_name],
            ops=[*existing_ops, op],
            output_kind=getattr(self, "_output_kind", "table"),
            table_primary_key=effective_key,
            upstream=self,
        )


def _table_source_name(obj: Any) -> str:
    """Return the root source name for *obj* (TableSource or TableDerivation)."""
    current = obj
    while hasattr(current, "_upstream") and current._upstream is not None:
        current = current._upstream
    return current._name


# ---------------------------------------------------------------------------
# Runtime descriptor classes
# ---------------------------------------------------------------------------


class TableSource(_TableOpsMixin):
    """Descriptor for a class-form @bv.table declaration.

    Exposes:
        _name: str                          — class name
        _schema: dict[str, FieldSpec]       — ordered field map
        _beava_kind: str = "table"
        _primary_key: list[str]             — key field names (required)
        _ttl_ms: int | None                 — retention in ms; None = no TTL
        _mode: str = "upsert"
        _upstreams: list[str] = []
        _ops: list = []
        _to_register_json() -> dict         — wire JSON matching Phase 2 TableDescriptor
    """

    _beava_kind: str = "table"

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        primary_key: list[str],
        ttl_ms: int | None,
        mode: str = "upsert",
    ) -> None:
        self._name = name
        self._schema = schema
        self._primary_key = primary_key
        self._ttl_ms = ttl_ms
        self._mode = mode
        self._upstreams: list[str] = []
        self._ops: list[Any] = []

    def _to_register_json(self) -> dict[str, Any]:
        """Return JSON dict matching Phase 2 TableDescriptor wire shape."""
        return {
            "kind": "table",
            "name": self._name,
            "primary_key": list(self._primary_key),
            "schema": {
                "fields": {
                    n: py_type_to_field_type(s.py_type) for n, s in self._schema.items()
                },
                "optional_fields": [n for n, s in self._schema.items() if s.optional],
            },
            "ttl_ms": self._ttl_ms,
            "mode": self._mode,
        }

    def __repr__(self) -> str:
        return f"TableSource({self._name!r}, key={self._primary_key!r})"


class TableDerivation(_TableOpsMixin):
    """Descriptor for a function-form @bv.table declaration OR a fluent-op derivation.

    Exposes:
        _name: str
        _schema: dict[str, FieldSpec]
        _beava_kind: str = "derivation"
        _upstreams: list[str]
        _ops: list
        _output_kind: str = "table"
        _table_primary_key: list[str]
        _upstream: TableSource|TableDerivation|None  — direct parent (fluent-API only)
        upstream: property                           — public alias for _upstream
        ops: property                                — public read-only list copy
        key: property                                — current key field list
        _to_register_json() -> dict
    """

    _beava_kind: str = "derivation"

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        upstreams: list[str],
        ops: list[Any],
        output_kind: str = "table",
        table_primary_key: list[str],
        upstream: Any = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._upstreams = upstreams
        self._ops = ops
        self._output_kind = output_kind
        self._table_primary_key = table_primary_key
        self._upstream = upstream  # direct parent in fluent chain (None for @bv.table fn-form)

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
            "table_primary_key": list(self._table_primary_key),
        }

    def __repr__(self) -> str:
        return (
            f"TableDerivation({self._name!r}, upstreams={self._upstreams!r}, "
            f"key={self._table_primary_key!r})"
        )


# ---------------------------------------------------------------------------
# Decorator implementation
# ---------------------------------------------------------------------------


def _normalize_key(key: Any) -> list[str]:
    """Normalize key argument to a list of strings."""
    if isinstance(key, str):
        return [key]
    if isinstance(key, (list, tuple)):
        return list(key)
    raise TypeError(
        f"@bv.table key must be a str or list[str], got {type(key).__name__!r}"
    )


def _decorate_table_class(
    cls: type,
    *,
    key: Any,
    ttl: str | None,
    mode: str,
) -> TableSource:
    """Apply @bv.table semantics to a class, returning a TableSource descriptor."""
    if key is None:
        raise TypeError(
            "@bv.table requires a key= argument specifying the primary key field(s); "
            "e.g. @bv.table(key='user_id') or @bv.table(key=['region', 'user_id'])"
        )

    key_list = _normalize_key(key)
    schema = extract_schema(cls)

    # Validate every key field exists in the schema
    for k in key_list:
        if k not in schema:
            raise TypeError(
                f"@bv.table key {k!r} is not declared in the class schema; "
                f"available fields: {list(schema.keys())}"
            )

    # TTL handling: "forever" → None; other strings convert to ms
    ttl_ms: int | None = None
    if ttl is not None:
        validate_duration_string(ttl)
        if ttl != "forever":
            ttl_ms = duration_to_ms(ttl)

    return TableSource(
        name=cls.__name__,
        schema=schema,
        primary_key=key_list,
        ttl_ms=ttl_ms,
        mode=mode,
    )


def _decorate_table_function(
    func: Any,
    *,
    key: Any,
    ttl: str | None,
    mode: str,
) -> TableDerivation:
    """Apply @bv.table semantics to a function, returning a TableDerivation descriptor."""
    if key is None:
        raise TypeError(
            "@bv.table requires a key= argument even for function-form; "
            "e.g. @bv.table(key='user_id') def X(source: SomeEvent) -> object: ..."
        )

    key_list = _normalize_key(key)
    sig = inspect.signature(func)

    upstream_names: list[str] = []
    placeholder_args: list[Any] = []

    for param_name, param in sig.parameters.items():
        # Read the annotation directly from the parameter object.
        # Avoids typing.get_type_hints() which fails when the annotated type is
        # a local variable (EventSource / TableSource) not visible in the
        # function's defining module namespace.
        upstream_cls = param.annotation
        if (
            upstream_cls is inspect.Parameter.empty
            or not hasattr(upstream_cls, "_beava_kind")
            or not hasattr(upstream_cls, "_name")
        ):
            raise TypeError(
                f"@bv.table function form: parameter {param_name!r} must be annotated "
                f"with a @bv.event- or @bv.table-decorated descriptor "
                f"(got {upstream_cls!r})"
            )
        upstream_names.append(upstream_cls._name)
        placeholder_args.append(upstream_cls)

    result = func(*placeholder_args)

    schema: dict[str, FieldSpec] = getattr(result, "_schema", {})
    ops: list[Any] = getattr(result, "_ops", [])

    # Note: TTL is a source-table concern; derivations do not carry ttl_ms.
    # The `ttl` kwarg is accepted for API symmetry but is intentionally unused
    # in function-form — the output table's TTL is set during registration.

    return TableDerivation(
        name=func.__name__,
        schema=schema,
        upstreams=upstream_names,
        ops=ops,
        output_kind="table",
        table_primary_key=key_list,
    )


def table(
    arg: Any = _SENTINEL,
    *,
    key: Any = None,
    ttl: str | None = None,
    mode: str = "upsert",
) -> Any:
    """Decorator to declare a table source or derivation.

    **key is REQUIRED.** Omitting key raises TypeError at decoration time.

    Supports two calling styles:

    **Class form (source):**
    ::

        @bv.table(key="user_id")
        class UserProfile:
            user_id: str
            name: str

        @bv.table(key=["region", "user_id"], ttl="7d")
        class RegionalProfile:
            ...

    **Function form (derivation):**
    ::

        @bv.table(key="user_id")
        def Counts(source: Transaction) -> object:
            return source

    Args:
        arg: Internal positional sentinel — do NOT pass explicitly.
             Used to detect bare ``@bv.table`` (no parentheses) and raise an
             error directing the user to add ``key=``.
        key: Primary key field name (str) or list of names (list[str]). Required.
        ttl: Retention duration string (e.g. ``"7d"``), or ``"forever"`` for no TTL.
        mode: Write mode — always ``"upsert"`` in Phase 3.

    Returns:
        A :class:`TableSource` (class form) or :class:`TableDerivation` (function form),
        or a decorator function when called with parentheses.

    Raises:
        TypeError: If ``key`` is missing, a key field is not in schema, a duration
                   string is malformed, or applied to a non-class/function.
    """
    kwargs = {"key": key, "ttl": ttl, "mode": mode}

    if arg is _SENTINEL:
        # Called with parentheses: @bv.table(...) — return a decorator
        def _decorator(target: Any) -> Any:
            if inspect.isclass(target):
                return _decorate_table_class(target, **kwargs)
            if callable(target):
                return _decorate_table_function(target, **kwargs)
            raise TypeError(
                f"@bv.table can only be applied to a class or function, "
                f"got {type(target).__name__!r}"
            )

        return _decorator

    # Bare @bv.table (no parentheses) — arg is the class or function.
    # key is missing (still None) so we must raise immediately.
    if inspect.isclass(arg) or callable(arg):
        raise TypeError(
            "@bv.table requires a key= argument; "
            "use @bv.table(key='field_name') not bare @bv.table"
        )

    raise TypeError(
        f"@bv.table can only be applied to a class or function, "
        f"got {type(arg).__name__!r}"
    )
