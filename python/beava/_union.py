"""``bv.union(*streams)`` stub — Plan 21-03.

Validates field-by-field schema compatibility at registration; execution
lands in Phase 22. Compatibility is strict in v0: fields must match by
name, by Python type, and by optional flag. If users need to bridge
differing nullability, they should ``.fillna()`` one side first — the
error message spells this out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from beava._types_core import FieldSpec

if TYPE_CHECKING:  # pragma: no cover
    from beava._stream import Stream, StreamDerivation


class UnionSpec:
    """Descriptor carried on the StreamDerivation returned by :func:`union`."""

    def __init__(self, sources: list[Any]) -> None:
        self.sources = list(sources)

    def _compile_for_server(self) -> None:
        raise NotImplementedError("union ships in Phase 22")

    def _to_union_json(self) -> dict[str, Any]:
        return {"sources": [s._name for s in self.sources]}


def _format_schema(schema: dict[str, FieldSpec]) -> str:
    parts = [
        f"{name}: {spec.py_type.__name__}"
        f"{'?' if spec.optional else ''}"
        for name, spec in schema.items()
    ]
    return "{" + ", ".join(parts) + "}"


def _check_compatible(left: Any, right: Any) -> None:
    lschema = left._schema
    rschema = right._schema
    # Missing / extra fields.
    lset = set(lschema.keys())
    rset = set(rschema.keys())
    if lset != rset:
        only_l = sorted(lset - rset)
        only_r = sorted(rset - lset)
        raise TypeError(
            f"bv.union: schemas differ between {left._name!r} and "
            f"{right._name!r}. "
            f"{'Only in ' + left._name + ': ' + repr(only_l) + '. ' if only_l else ''}"
            f"{'Only in ' + right._name + ': ' + repr(only_r) + '. ' if only_r else ''}"
            f"union requires exactly matching schemas; "
            f"if fields differ in nullability, apply .fillna() first on one side.\n"
            f"  {left._name} schema: {_format_schema(lschema)}\n"
            f"  {right._name} schema: {_format_schema(rschema)}"
        )
    # Per-field type + optional check.
    for fname in lschema:
        ls = lschema[fname]
        rs = rschema[fname]
        if ls.py_type is not rs.py_type or ls.optional != rs.optional:
            raise TypeError(
                f"bv.union: field {fname!r} type mismatch between "
                f"{left._name!r} and {right._name!r}: "
                f"{ls.py_type.__name__}{'?' if ls.optional else ''} vs "
                f"{rs.py_type.__name__}{'?' if rs.optional else ''}. "
                f"union requires exactly matching schemas; if fields differ "
                f"in nullability, apply .fillna() first on one side.\n"
                f"  {left._name} schema: {_format_schema(lschema)}\n"
                f"  {right._name} schema: {_format_schema(rschema)}"
            )


def union(*streams: Any) -> Any:
    """Combine two or more Streams with identical schemas into one Stream.

    All inputs must have field-by-field compatible schemas (same names,
    same Python types, same optional flags). Execution ships in Phase 22;
    schema validation runs at registration.
    """
    from beava._stream import Stream, StreamDerivation

    if len(streams) < 2:
        raise TypeError(
            f"bv.union requires 2 or more Streams; got {len(streams)}"
        )
    for s in streams:
        if not isinstance(s, Stream):
            raise TypeError(
                f"bv.union arguments must be Streams; got {type(s).__name__}"
            )

    first = streams[0]
    for other in streams[1:]:
        _check_compatible(first, other)

    # Unified schema = the first's schema (all are equivalent by construction).
    out_schema: dict[str, FieldSpec] = dict(first._schema)
    names = "_".join(s._name for s in streams)
    derivation = StreamDerivation(
        name=f"Union_{names}",
        schema=out_schema,
        ops=[],
        upstream=first,
        upstreams=list(streams),
    )
    derivation._union_spec = UnionSpec(list(streams))  # type: ignore[attr-defined]
    return derivation


__all__ = ["UnionSpec", "union"]
