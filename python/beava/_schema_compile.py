"""Phase 59.6 Wave 1 (TPC-PERF-11) — Python-side schema compilation from class annotations.

Extracts field specs from ``@bv.stream`` / ``@bv.source_table`` / ``@bv.table``
decorators at import time; assigns byte offsets; emits a JSON-serializable
schema dict that matches ``engine::schema::RegisteredSchema``'s serde shape.

The output dict is placed under the ``schema`` key of the REGISTER JSON by
``python/beava/_serialize.py::_compile_source``; the server's
``engine::register::RegisterSchemaJson`` deserializer consumes it into an
``engine::schema::RegisteredSchema`` and registers it with
``PipelineEngine::register_typed_schema``. From there, Wave 2+ wire codecs
and operators branch on ``engine.is_typed_stream(name)`` to take the typed
fast path.

# Key invariants (must stay in sync with src/engine/schema.rs)

- ``inline_str_cap`` is the number of user-visible bytes; the *slot* size
  reserved in the payload is ``inline_str_cap + 1`` (trailing NUL byte).
  ``_field_width("inline_str", cap)`` therefore returns ``cap + 1``.
- Field types are serialized as snake_case strings matching Rust's
  ``#[serde(rename_all = "snake_case")]`` on ``FieldTy``:
  ``"i64" | "f64" | "bool" | "inline_str" | "string" | "bytes"``.
- ``row_size`` equals the tightest-packed layout: ``max(offset + width)``
  across all fields, which is also the sum of field widths when offsets
  are assigned sequentially (as we do here).
"""

from __future__ import annotations

from dataclasses import dataclass, field as _dc_field
from typing import Any

# Phase 59.6 D-A1 — default inline-string cap; must match Rust's
# ``engine::schema::default_inline_str_cap()``.
DEFAULT_INLINE_STR_CAP = 15

# Mapping from Python primitive types to Rust FieldTy names (snake_case
# matches serde rename). ``str`` defaults to ``inline_str`` — short string
# inline-in-row is the common case; users with genuinely-long strings can
# opt into the arena path via a future annotation / override API (Wave 4+).
_PY_TO_FIELD_TY = {
    int: "i64",
    float: "f64",
    bool: "bool",
    str: "inline_str",
    bytes: "bytes",
}


@dataclass
class CompiledFieldSpec:
    """One column in a compiled schema; mirrors Rust's ``FieldSpec``.

    - ``name``: Python class-annotation attribute name.
    - ``ty``: Rust FieldTy as snake_case string (wire contract).
    - ``offset``: byte offset in the row's payload.
    - ``nullable``: True iff the Python annotation used ``Optional[...]`` or
      ``T | None``.
    """

    name: str
    ty: str
    offset: int
    nullable: bool = False


@dataclass
class CompiledSchema:
    """Compiled schema for a decorated class; mirrors Rust's ``RegisteredSchema``.

    ``inline_str_cap`` + ``fields`` + ``row_size`` are the three fields that
    ship in the REGISTER JSON ``schema:`` block via ``to_json()``.
    """

    inline_str_cap: int = DEFAULT_INLINE_STR_CAP
    fields: list[CompiledFieldSpec] = _dc_field(default_factory=list)
    row_size: int = 0

    def to_json(self) -> dict:
        """Emit the REGISTER JSON ``schema:`` block shape.

        Matches ``src/engine/register.rs::RegisterSchemaJson`` exactly —
        this dict is embedded under ``d["schema"]`` in
        ``_serialize.py::_compile_source``.
        """
        return {
            "inline_str_cap": self.inline_str_cap,
            "fields": [
                {
                    "name": f.name,
                    "ty": f.ty,
                    "offset": f.offset,
                    "nullable": f.nullable,
                }
                for f in self.fields
            ],
            "row_size": self.row_size,
        }


def _field_width(ty: str, inline_str_cap: int) -> int:
    """Fixed-layout byte width for a FieldTy — mirrors Rust's
    ``FieldTy::fixed_width``.

    The ``inline_str`` slot is ``cap + 1`` bytes (extra byte holds a NUL
    terminator). ``string`` / ``bytes`` use ``(start: u32, len: u32)`` into
    the per-row arena, so they occupy 8 bytes in the payload regardless of
    the actual data length.
    """
    if ty in ("i64", "f64"):
        return 8
    if ty == "bool":
        return 1
    if ty == "inline_str":
        return inline_str_cap + 1  # +1 NUL terminator, matches Rust
    if ty in ("string", "bytes"):
        return 8  # (start: u32, len: u32)
    raise ValueError(f"unknown FieldTy {ty!r}")


def _strip_optional(py_ty: Any) -> tuple[Any, bool]:
    """Collapse ``Optional[X]`` / ``X | None`` → ``(X, True)``.

    Returns ``(py_ty, False)`` unchanged for non-nullable annotations.
    Uses ``typing.get_origin``/``get_args`` so we catch both
    ``typing.Optional`` and PEP-604 ``X | None`` union types.
    """
    # Beava-specific wrapper: _types_core.Optional[T] → _OptionalSpec(T).
    try:
        from beava._types_core import _OptionalSpec  # local import; module may not exist during tests
        if isinstance(py_ty, _OptionalSpec):
            return py_ty.inner, True
    except Exception:  # pragma: no cover — defensive
        pass

    from typing import Union, get_args, get_origin
    import types as _types

    origin = get_origin(py_ty)
    is_union = origin is Union
    # PEP-604 X | None produces types.UnionType on Python 3.10+.
    if hasattr(_types, "UnionType") and isinstance(py_ty, _types.UnionType):
        is_union = True
    if is_union:
        args = [a for a in get_args(py_ty) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return py_ty, False


def compile_schema_from_class(
    cls: type, inline_str_cap: int = DEFAULT_INLINE_STR_CAP
) -> CompiledSchema:
    """Walk ``cls.__annotations__``; emit a ``CompiledSchema``.

    Unsupported Python types raise ``TypeError`` with the field name + type so
    users get an actionable message at import time (decorator application),
    not at server-register time. Classes with no annotations raise too —
    typed-pipeline records require annotated fields; an un-annotated class
    falls back to the untyped REGISTER path (see caller in ``_stream.py``).

    Args:
        cls: The class being decorated by ``@bv.stream`` / ``@bv.source_table``
            / ``@bv.table``.
        inline_str_cap: Per-schema override of the inline-string slot width.
            Default 15 matches ``DEFAULT_INLINE_STR_CAP``.

    Returns:
        A ``CompiledSchema`` whose ``to_json()`` is the REGISTER JSON
        ``schema:`` block shape.
    """
    schema = CompiledSchema(inline_str_cap=inline_str_cap)
    offset = 0
    # Resolve annotations to real Python types. Under
    # ``from __future__ import annotations`` the class's __annotations__
    # dict carries *string* forward-references; typing.get_type_hints
    # resolves those against the class's module globals + builtins. Fall
    # back to the raw dict if get_type_hints fails (e.g. locally-defined
    # classes whose annotations reference names only in caller locals).
    import typing as _typing
    # Collect caller-frame locals — works for decorators / compile calls
    # invoked inside test methods / closures where annotations reference
    # names only visible in those scopes. Mirrors
    # python/beava/_stream.py::_resolve_func_hints.
    _localns: dict = {}
    try:
        import sys as _sys
        _frame = _sys._getframe(1)
        _depth = 0
        while _frame is not None and _depth < 8:
            for _k, _v in _frame.f_locals.items():
                _localns.setdefault(_k, _v)
            _frame = _frame.f_back
            _depth += 1
    except Exception:
        pass
    try:
        annotations = _typing.get_type_hints(cls, localns=_localns)
    except Exception:
        annotations = dict(getattr(cls, "__annotations__", None) or {})
    if not annotations:
        raise TypeError(
            f"@bv.stream/source_table/table class {cls.__name__!r} has no type "
            f"annotations; typed-pipeline records require annotated fields."
        )
    for name, py_ty in annotations.items():
        # When get_type_hints falls back to raw __annotations__, values may
        # still be strings; eval against the class's module globals as a
        # best-effort recovery. This matches _schema_v0.extract_schema's
        # tolerance for forward references inside test closures.
        if isinstance(py_ty, str):
            try:
                module = getattr(cls, "__module__", None)
                ns = {}
                if module is not None:
                    import sys as _sys
                    mod = _sys.modules.get(module)
                    if mod is not None:
                        ns.update(vars(mod))
                import builtins as _bi
                ns.update(vars(_bi))
                py_ty = eval(py_ty, ns)
            except Exception as exc:
                raise TypeError(
                    f"@bv.stream field {cls.__name__}.{name} annotation "
                    f"{py_ty!r} could not be resolved: {exc}"
                )
        py_ty, nullable = _strip_optional(py_ty)
        if py_ty not in _PY_TO_FIELD_TY:
            raise TypeError(
                f"@bv.stream field {cls.__name__}.{name} has unsupported "
                f"type {py_ty!r}; supported: int, float, bool, str, bytes "
                f"(and Optional[...] thereof)."
            )
        ty = _PY_TO_FIELD_TY[py_ty]
        width = _field_width(ty, inline_str_cap)
        schema.fields.append(
            CompiledFieldSpec(
                name=name,
                ty=ty,
                offset=offset,
                nullable=nullable,
            )
        )
        offset += width
    schema.row_size = offset
    return schema


__all__ = [
    "DEFAULT_INLINE_STR_CAP",
    "CompiledFieldSpec",
    "CompiledSchema",
    "compile_schema_from_class",
]
