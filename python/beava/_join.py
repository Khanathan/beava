"""Join stubs — Stream↔Stream, Stream↔Table, Table↔Table (Plan 21-03).

All three shapes produce a descriptor carrying a :class:`JoinSpec`. The
execution stub (``_compile_for_server``) raises
``NotImplementedError("ships in Phase 23")`` — Phase 23 wires joins into
the Rust engine. Schema inference is real so users get surgical errors at
registration.

Shape dispatch rules (enforced here, not in the engine):

  * Stream ↔ Stream — ``within=`` REQUIRED (symmetric interval join),
    output is a Stream of joined events.
  * Stream ↔ Table — ``within=`` is FORBIDDEN (enrichment is current-state);
    output is a Stream.
  * Table ↔ Table — ``within=`` is FORBIDDEN; ``on`` must be the full key of
    both tables (partial-key joins deferred past v0); output is a Table.

v0 ``type`` restrictions: ``inner`` and ``left`` only. ``outer`` / ``right``
/ ``full`` / ``cross`` are rejected at registration with the exact deferred
messages specified in 21-CONTEXT.md.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from beava._schema_v0 import schema_mismatch_error
from beava._types_core import FieldSpec

if TYPE_CHECKING:  # pragma: no cover
    from beava._stream import Stream, StreamDerivation
    from beava._table import Table, TableDerivation


_ALLOWED_JOIN_TYPES = {"inner", "left"}


def _normalize_on(on: Any) -> list[str]:
    if isinstance(on, str):
        if not on:
            raise TypeError("join(on=...) requires a non-empty field name")
        return [on]
    if isinstance(on, (list, tuple)):
        if not on:
            raise TypeError("join(on=[...]) requires at least one field")
        for k in on:
            if not isinstance(k, str) or not k:
                raise TypeError(
                    f"join(on=[...]) keys must be non-empty strings; got {k!r}"
                )
        return list(on)
    raise TypeError(
        f"join(on=...) must be a string or list of strings; got "
        f"{type(on).__name__}"
    )


def _check_join_type(type_: str) -> str:
    if type_ == "outer":
        raise RuntimeError(
            "outer joins deferred to v0.1; v0 supports 'inner' and 'left' only"
        )
    if type_ not in _ALLOWED_JOIN_TYPES:
        raise TypeError(
            f"join(type={type_!r}) must be 'inner' or 'left' in v0; "
            f"'right' / 'full' / 'cross' are not supported"
        )
    return type_


def compute_joined_schema(
    left_schema: dict[str, FieldSpec],
    right_schema: dict[str, FieldSpec],
    right_name: str,
    on_keys: list[str],
) -> dict[str, FieldSpec]:
    """Polars-style: left wins on collision; right's colliding field gets
    ``_right`` suffix. Join keys appear once (left side)."""
    out: dict[str, FieldSpec] = dict(left_schema)
    on_set = set(on_keys)
    for rname, rspec in right_schema.items():
        if rname in on_set:
            # Join key — already in the left-side output; do not duplicate.
            continue
        if rname in out:
            # Collision → right gets suffix.
            new_name = f"{rname}_right"
            # If the suffixed name also collides, append an index.
            idx = 2
            while new_name in out:
                new_name = f"{rname}_right{idx}"
                idx += 1
            out[new_name] = FieldSpec(
                name=new_name,
                py_type=rspec.py_type,
                optional=rspec.optional,
                desc=rspec.desc,
                default=rspec.default,
            )
        else:
            out[rname] = rspec
    return out


def _validate_keys_in_schema(
    keys: list[str],
    schema: dict[str, FieldSpec],
    context: str,
) -> None:
    for k in keys:
        if k not in schema:
            raise TypeError(schema_mismatch_error(k, schema, context))


class JoinSpec:
    """Descriptor attached to a StreamDerivation/TableDerivation for a join.

    Phase 23 consumes ``_to_join_json``. ``_compile_for_server`` is a
    hard-stop sentinel — tests assert it raises with the expected message.
    """

    def __init__(
        self,
        left: Any,
        right: Any,
        on: list[str],
        within: str | None,
        type_: str,
        shape: str,  # "stream_stream" | "stream_table" | "table_table"
    ) -> None:
        self.left = left
        self.right = right
        self.on = list(on)
        self.within = within
        self.type_ = type_
        self.shape = shape

    def _compile_for_server(self) -> None:
        raise NotImplementedError("join ships in Phase 23")

    def _to_join_json(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "op": "join",
            "left": self.left._name,
            "right": self.right._name,
            "on": list(self.on),
            "type": self.type_,
            "shape": self.shape,
        }
        if self.within is not None:
            d["within"] = self.within
        return d


# ---------------------------------------------------------------------------
# Dispatch helpers — called by Stream.join / Table.join
# ---------------------------------------------------------------------------


def stream_join(
    left: "Stream",
    other: Any,
    *,
    on: Any,
    within: str | None,
    type_: str,
) -> "Stream":
    """Join from the Stream side. Dispatches on ``other`` being Stream or Table."""
    from beava._stream import Stream, StreamDerivation
    from beava._table import Table

    on_keys = _normalize_on(on)
    type_ = _check_join_type(type_)

    if isinstance(other, Stream):
        # Stream ↔ Stream — within required.
        if within is None:
            raise TypeError(
                "Stream↔Stream join requires within=... (e.g. '30m'); "
                "symmetric interval joins without a window are not supported"
            )
        _validate_keys_in_schema(on_keys, left._schema, left._name)
        _validate_keys_in_schema(on_keys, other._schema, other._name)
        out_schema = compute_joined_schema(
            left._schema, other._schema, other._name, on_keys
        )
        derivation = StreamDerivation(
            name=f"{left._name}_Join_{other._name}",
            schema=out_schema,
            ops=[],
            upstream=left,
            upstreams=[left, other],
        )
        derivation._join_spec = JoinSpec(  # type: ignore[attr-defined]
            left, other, on_keys, within, type_, shape="stream_stream"
        )
        return derivation

    if isinstance(other, Table):
        # Stream ↔ Table enrichment — within NOT allowed.
        if within is not None:
            raise TypeError(
                "Stream↔Table enrichment does not accept within=...; "
                "the Table's current row is looked up at the stream event's "
                "event-time (no symmetric window)"
            )
        _validate_keys_in_schema(on_keys, left._schema, left._name)
        _validate_keys_in_schema(on_keys, other._schema, other._name)
        out_schema = compute_joined_schema(
            left._schema, other._schema, other._name, on_keys
        )
        derivation = StreamDerivation(
            name=f"{left._name}_Enrich_{other._name}",
            schema=out_schema,
            ops=[],
            upstream=left,
            upstreams=[left, other],
        )
        derivation._join_spec = JoinSpec(  # type: ignore[attr-defined]
            left, other, on_keys, None, type_, shape="stream_table"
        )
        return derivation

    raise TypeError(
        f"Stream.join(other=...) requires other to be a Stream or Table; "
        f"got {type(other).__name__}"
    )


def table_join(
    left: "Table",
    right: "Table",
    *,
    on: Any,
    type_: str,
) -> "Table":
    """Table↔Table same-key join.

    Enforces ``on`` matches both tables' full key lists (set-equal) — partial
    keys are deferred past v0.
    """
    from beava._table import Table, TableDerivation

    on_keys = _normalize_on(on)
    type_ = _check_join_type(type_)

    left_key = list(getattr(left, "_key", []))
    right_key = list(getattr(right, "_key", []))
    # v0: full-key match required — set equality (order-insensitive).
    if set(on_keys) != set(left_key) or set(on_keys) != set(right_key):
        raise RuntimeError(
            f"Table↔Table join requires full-key match; "
            f"Table {left._name!r} key={left_key!r}, "
            f"{right._name!r} key={right_key!r}, on={on_keys!r}"
        )

    _validate_keys_in_schema(on_keys, left._schema, left._name)
    _validate_keys_in_schema(on_keys, right._schema, right._name)
    out_schema = compute_joined_schema(
        left._schema, right._schema, right._name, on_keys
    )
    derivation = TableDerivation(
        name=f"{left._name}_Join_{right._name}",
        schema=out_schema,
        key=list(left_key),
        mode="append",
        ttl=None,
        ops=[],
        upstream=left,
        upstreams=[left, right],
    )
    derivation._join_spec = JoinSpec(  # type: ignore[attr-defined]
        left, right, on_keys, None, type_, shape="table_table"
    )
    return derivation


__all__ = [
    "JoinSpec",
    "stream_join",
    "table_join",
    "compute_joined_schema",
]
