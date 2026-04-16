"""Stateless per-row operator catalog for Stream and Table descriptors.

Provides :class:`StatelessOpsMixin` with the 8 v0 ops:

    filter, map, select, drop, rename, with_columns, cast, fillna

Each op is pure at the schema level: it takes an input schema (``dict[str,
FieldSpec]``) + operator arguments, validates argument references against
the current schema (raising :class:`TypeError` with surgical Levenshtein
hints on missing fields), computes an output schema, and returns a *new*
descriptor of the same outer runtime type wrapping the upstream + the
appended op.

The mixin is applied to ``Stream`` and ``Table`` so the same method surface
is available on every Stream/Table subclass. Instances returned by the ops
are ``StreamDerivation`` / ``TableDerivation`` from ``_stream`` / ``_table``;
those modules are imported lazily to break the circular import.
"""

from __future__ import annotations

from typing import Any

from beava._col import _ExprAST
from beava._schema_v0 import schema_mismatch_error
from beava._types_core import FieldSpec


# ---------------------------------------------------------------------------
# Accepted cast targets (v0 primitive set)
# ---------------------------------------------------------------------------


_CAST_TYPE_MAP: dict[str, type] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


# ---------------------------------------------------------------------------
# Minimal expression-type inference (v0)
# ---------------------------------------------------------------------------


def _infer_expr_type(expr: _ExprAST, schema: dict[str, FieldSpec]) -> type:
    """Best-effort type inference for a captured expression AST.

    Rules (v0):
      - Arithmetic BinOp (``+ - * /``) → ``float``.
      - Comparison / boolean (``> < >= <= == != and or``) → ``bool``.
      - Unary ``not`` → ``bool``.
      - Field reference → schema[name].py_type.
      - Literal → ``type(value)``.
      - ``cast(x, <type_name>)`` call → ``<type_name>`` mapped to Python type.
      - Anything else falls back to ``object``.
    """
    # Local imports to avoid cycles in module setup.
    from beava._col import _BinOp, _Call, _Field, _Literal, _UnaryOp, _BareIdent

    if isinstance(expr, _Field):
        spec = schema.get(expr.name)
        if spec is not None:
            return spec.py_type
        return object
    if isinstance(expr, _Literal):
        v = expr.value
        if v is None:
            return object
        if isinstance(v, _BareIdent):
            return object
        return type(v)
    if isinstance(expr, _UnaryOp):
        return bool
    if isinstance(expr, _BinOp):
        if expr.op in ("+", "-", "*", "/"):
            return float
        return bool  # comparison + and/or
    if isinstance(expr, _Call):
        if expr.fn == "cast" and len(expr.args) == 2:
            # Second arg is a literal _BareIdent carrying the target type name
            arg = expr.args[1]
            if isinstance(arg, _Literal) and isinstance(arg.value, _BareIdent):
                return _CAST_TYPE_MAP.get(arg.value.name, object)
        return object
    return object


# ---------------------------------------------------------------------------
# Reference validation helper
# ---------------------------------------------------------------------------


def _validate_fields_or_raise(
    refs: set[str],
    schema: dict[str, FieldSpec],
    context: str,
) -> None:
    """Raise TypeError listing every missing reference (not just the first)."""
    missing = [r for r in refs if r not in schema]
    if not missing:
        return
    blocks = [schema_mismatch_error(r, schema, context) for r in missing]
    raise TypeError("\n\n".join(blocks))


# ---------------------------------------------------------------------------
# The mixin
# ---------------------------------------------------------------------------


class StatelessOpsMixin:
    """The 8 stateless ops — methods on Stream and Table alike.

    Subclasses must expose ``_name: str``, ``_schema: dict[str, FieldSpec]``,
    and ``_ops: list[dict]``. They must also implement ``_derive(name,
    schema, ops, upstream)`` returning a new instance of the same outer
    runtime type. Stream and Table provide their own ``_derive`` — the mixin
    does not care which concrete class it wraps.
    """

    # These attributes are provided by the host class; declared here for
    # type checkers.
    _name: str
    _schema: dict[str, FieldSpec]
    _ops: list[dict[str, Any]]

    def _derive(
        self,
        *,
        schema: dict[str, FieldSpec],
        op: dict[str, Any],
    ) -> "StatelessOpsMixin":  # pragma: no cover - overridden
        raise NotImplementedError

    # --- .filter ---
    def filter(self, expr: _ExprAST) -> "StatelessOpsMixin":
        """Keep rows where ``expr`` evaluates truthy. Schema unchanged."""
        if isinstance(expr, str):
            raise TypeError(
                "filter() requires a bv.col expression, not a string; "
                "use bv.col('x') > 5"
            )
        if not isinstance(expr, _ExprAST):
            raise TypeError(
                f"filter() requires a bv.col expression, got "
                f"{type(expr).__name__}"
            )
        _validate_fields_or_raise(
            expr.referenced_fields(), self._schema, self._name
        )
        return self._derive(
            schema=dict(self._schema),
            op={"op": "filter", "expr": expr.to_expr_string()},
        )

    # --- .select ---
    def select(self, *field_names: str) -> "StatelessOpsMixin":
        """Keep only the listed fields, in the order given."""
        if not field_names:
            raise TypeError("select() requires at least one field name")
        _validate_fields_or_raise(set(field_names), self._schema, self._name)
        new_schema: dict[str, FieldSpec] = {n: self._schema[n] for n in field_names}
        return self._derive(
            schema=new_schema,
            op={"op": "select", "fields": list(field_names)},
        )

    # --- .drop ---
    def drop(self, *field_names: str) -> "StatelessOpsMixin":
        """Remove the listed fields; preserves remaining order."""
        if not field_names:
            raise TypeError("drop() requires at least one field name")
        _validate_fields_or_raise(set(field_names), self._schema, self._name)
        # Table key-field protection
        key = getattr(self, "_key", None)
        if key is not None:
            for k in field_names:
                if k in key:
                    raise TypeError(
                        f"cannot drop key field {k!r} from Table {self._name!r}"
                    )
        dropped = set(field_names)
        new_schema = {n: s for n, s in self._schema.items() if n not in dropped}
        return self._derive(
            schema=new_schema,
            op={"op": "drop", "fields": list(field_names)},
        )

    # --- .rename ---
    def rename(self, **mapping: str) -> "StatelessOpsMixin":
        """Rename fields. Source names must exist; targets must not collide."""
        if not mapping:
            raise TypeError("rename() requires at least one old=new pair")
        _validate_fields_or_raise(set(mapping.keys()), self._schema, self._name)
        # Collision check: target must not already be in the *remaining* schema.
        for old, new in mapping.items():
            if new in self._schema and new != old and new not in mapping.keys():
                raise TypeError(
                    f"rename target {new!r} collides with existing field "
                    f"{new!r} — drop it first"
                )
            # Also catch collisions within the mapping itself.
            for o2, n2 in mapping.items():
                if o2 != old and n2 == new:
                    raise TypeError(
                        f"rename target {new!r} collides with rename target "
                        f"of field {o2!r}"
                    )
        new_schema: dict[str, FieldSpec] = {}
        for name, spec in self._schema.items():
            if name in mapping:
                new_name = mapping[name]
                new_schema[new_name] = FieldSpec(
                    name=new_name,
                    py_type=spec.py_type,
                    optional=spec.optional,
                    desc=spec.desc,
                    default=spec.default,
                )
            else:
                new_schema[name] = spec
        derived = self._derive(
            schema=new_schema,
            op={"op": "rename", "mapping": dict(mapping)},
        )
        # Cascade key rename for Tables.
        key = getattr(self, "_key", None)
        if key is not None:
            new_key = [mapping.get(k, k) for k in key]
            derived._key = new_key  # type: ignore[attr-defined]
        return derived

    # --- .with_columns ---
    def with_columns(self, **derivations: _ExprAST) -> "StatelessOpsMixin":
        """Add / replace derived fields. Values must be bv.col expressions."""
        if not derivations:
            raise TypeError("with_columns() requires at least one name=expr pair")

        all_refs: set[str] = set()
        exprs_out: dict[str, str] = {}
        for name, expr in derivations.items():
            if not isinstance(expr, _ExprAST):
                raise TypeError(
                    f"with_columns({name}=...) requires a bv.col expression, "
                    f"got {type(expr).__name__}"
                )
            all_refs.update(expr.referenced_fields())
            exprs_out[name] = expr.to_expr_string()
        _validate_fields_or_raise(all_refs, self._schema, self._name)

        new_schema = dict(self._schema)
        for name, expr in derivations.items():
            inferred = _infer_expr_type(expr, self._schema)
            new_schema[name] = FieldSpec(
                name=name,
                py_type=inferred,
                optional=False,
                desc=None,
            )
        return self._derive(
            schema=new_schema,
            op={"op": "with_columns", "exprs": exprs_out},
        )

    # --- .map (alias for with_columns) ---
    def map(self, **derivations: _ExprAST) -> "StatelessOpsMixin":
        """Alias for :meth:`with_columns` — kept for DataFrame parity."""
        return self.with_columns(**derivations)

    # --- .cast ---
    def cast(self, **type_map: str) -> "StatelessOpsMixin":
        """Coerce field types. Targets must be one of int/float/str/bool."""
        if not type_map:
            raise TypeError("cast() requires at least one field=type pair")
        _validate_fields_or_raise(set(type_map.keys()), self._schema, self._name)
        for name, t in type_map.items():
            if not isinstance(t, str) or t not in _CAST_TYPE_MAP:
                raise TypeError(
                    f"cast({name}={t!r}) — type must be one of "
                    f"{sorted(_CAST_TYPE_MAP.keys())}"
                )
        new_schema = dict(self._schema)
        for name, t in type_map.items():
            spec = new_schema[name]
            new_schema[name] = FieldSpec(
                name=name,
                py_type=_CAST_TYPE_MAP[t],
                optional=spec.optional,
                desc=spec.desc,
                default=spec.default,
            )
        return self._derive(
            schema=new_schema,
            op={"op": "cast", "casts": dict(type_map)},
        )

    # --- .fillna ---
    def fillna(self, **defaults: Any) -> "StatelessOpsMixin":
        """Fill nulls with a scalar default. Clears the field's optional flag."""
        if not defaults:
            raise TypeError("fillna() requires at least one field=value pair")
        _validate_fields_or_raise(set(defaults.keys()), self._schema, self._name)
        for name, val in defaults.items():
            if not isinstance(val, (int, float, str, bool)) and val is not None:
                raise TypeError(
                    f"fillna({name}=...) default must be a JSON-serializable "
                    f"scalar, got {type(val).__name__}"
                )
        new_schema = dict(self._schema)
        for name in defaults:
            spec = new_schema[name]
            new_schema[name] = FieldSpec(
                name=name,
                py_type=spec.py_type,
                optional=False,
                desc=spec.desc,
                default=spec.default,
            )
        return self._derive(
            schema=new_schema,
            op={"op": "fillna", "defaults": dict(defaults)},
        )


__all__ = ["StatelessOpsMixin"]
