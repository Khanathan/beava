"""``.group_by(...).agg(...)`` builder + AggregationSpec.

Plan 21-03: SDK-level schema inference only. Execution is Phase 22.

Contract:
  * ``Stream.group_by(*keys)`` returns a :class:`GroupBy` builder; validates
    every key against the upstream schema.
  * ``GroupBy.agg(**features)`` returns a :class:`~beava._table.TableDerivation`
    wrapping an :class:`AggregationSpec`. The Table's key is the grouping
    keys; its schema is the group keys ∪ each feature's inferred output type.
  * ``Table.group_by(...)`` raises the v0 rejection message (see
    :mod:`beava._table`). Table-input aggregation ships in v0.1.

The ``AggregationSpec.compile_for_server()`` path raises
``NotImplementedError("stream aggregation ships in Phase 22")`` — Phase 22
wires the Rust side. The ``AggregationSpec._to_feature_list()`` method
produces the ``features: [...]`` list that Phase 22's REGISTER handler will
consume via :mod:`beava._serialize`.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from beava._agg_ops import AggOp
from beava._schema_v0 import schema_mismatch_error
from beava._types_core import FieldSpec

if TYPE_CHECKING:  # pragma: no cover
    from beava._stream import Stream
    from beava._table import TableDerivation


def _short_hash(*parts: str) -> str:
    h = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
    return h[:8]


class AggregationSpec:
    """Specification for a Stream→Table aggregation (compiled in Phase 22).

    Held by a :class:`TableDerivation` as ``_agg_spec``. Exposes:

      * ``_compile_for_server()`` — raises ``NotImplementedError`` pointing at
        Phase 22. Used by :mod:`beava._serialize` to assert that the stub is
        never silently bypassed.
      * ``_to_feature_list()`` — serializes the ``features`` to a list of
        JSON dicts (one per user-assigned name). Consumed by the REGISTER
        JSON payload builder.
    """

    def __init__(
        self,
        upstream: "Stream",
        keys: list[str],
        features: dict[str, AggOp],
    ) -> None:
        self.upstream = upstream
        self.keys = list(keys)
        self.features = dict(features)

    def _compile_for_server(self) -> None:
        raise NotImplementedError(
            "stream aggregation ships in Phase 22"
        )

    def _to_feature_list(self) -> list[dict[str, Any]]:
        return [op.to_json(name) for name, op in self.features.items()]

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"AggregationSpec(upstream={self.upstream._name!r}, "
            f"keys={self.keys!r}, features={list(self.features.keys())!r})"
        )


class GroupBy:
    """Builder returned by ``Stream.group_by(*keys)``.

    Validates every key against the upstream's schema. :meth:`agg` is the
    terminal method — it builds the output schema, instantiates the
    :class:`AggregationSpec`, and returns a :class:`TableDerivation`.
    """

    def __init__(self, upstream: "Stream", keys: tuple[str, ...]) -> None:
        if not keys:
            raise TypeError(
                f"group_by requires at least one key on {upstream._name!r}"
            )
        for k in keys:
            if not isinstance(k, str):
                raise TypeError(
                    f"group_by keys must be strings; got {type(k).__name__}"
                )
            if k not in upstream._schema:
                raise TypeError(
                    schema_mismatch_error(k, upstream._schema, upstream._name)
                )
        self.upstream = upstream
        self.keys = list(keys)

    def agg(self, **features: AggOp) -> "TableDerivation":
        """Produce a TableDerivation with the aggregated schema.

        Each kwarg's name becomes an output column. Values must be AggOp
        instances (``bv.count(...)``, ``bv.sum(...)``, ...). Field references
        inside each op are validated against the upstream schema here so
        users see surgical errors at registration — not at run time.
        """
        from beava._table import TableDerivation  # local to break cycle

        if not features:
            raise TypeError(
                f"group_by(...).agg(...) requires at least one feature on "
                f"{self.upstream._name!r}"
            )

        upstream_schema = self.upstream._schema
        for feat_name, op in features.items():
            if not isinstance(op, AggOp):
                raise TypeError(
                    f"agg({feat_name}=...) requires a beava aggregation "
                    f"operator (bv.count/bv.sum/...); got {type(op).__name__}"
                )
            # Window requirement
            if op.requires_window and op.window is None:
                raise TypeError(
                    f"agg({feat_name}={type(op).__name__.lstrip('_').lower()}"
                    f"(...)) requires window=...; it's mandatory for this op"
                )
            # Field reference check (ops with no field — Count — skip)
            if op.field is not None:
                if op.field not in upstream_schema:
                    raise TypeError(
                        schema_mismatch_error(
                            op.field, upstream_schema, self.upstream._name
                        )
                    )
            # Feature names must not collide with group keys
            if feat_name in self.keys:
                raise TypeError(
                    f"agg feature name {feat_name!r} collides with group "
                    f"key; pick a different name"
                )

        # Build the output schema: group keys (with their upstream types)
        # followed by every feature's inferred type.
        out_schema: dict[str, FieldSpec] = {}
        for k in self.keys:
            src = upstream_schema[k]
            out_schema[k] = FieldSpec(
                name=k,
                py_type=src.py_type,
                optional=src.optional,
                desc=src.desc,
                default=src.default,
            )
        for feat_name, op in features.items():
            out_schema[feat_name] = FieldSpec(
                name=feat_name,
                py_type=op.output_type_for(upstream_schema),
                optional=False,
            )

        spec = AggregationSpec(self.upstream, self.keys, features)
        gen_name = (
            f"{self.upstream._name}_Agg_"
            f"{_short_hash(self.upstream._name, *self.keys, *features.keys())}"
        )
        derivation = TableDerivation(
            name=gen_name,
            schema=out_schema,
            key=list(self.keys),
            mode="append",
            ttl=None,
            ops=[],
            upstream=self.upstream,  # type: ignore[arg-type]
            upstreams=[self.upstream],
        )
        # Attach the aggregation spec — serializer uses this.
        derivation._agg_spec = spec  # type: ignore[attr-defined]
        # Phase 59.6 Wave 8 (TPC-PERF-11): compile typed schema for derived
        # tables so downstream cascade state can use the typed path. Without
        # this, every @bv.table function-form derivation falls back to the
        # serde_json::Value hot path on the server (push_internal_on_shard)
        # even when the input stream was typed. Samply profiling on the
        # fraud-pipeline benchmark shows this is ~17.8% of CPU.
        try:
            from beava._schema_compile import compile_schema_from_fields
            compiled = compile_schema_from_fields(
                out_schema, source_name=gen_name
            )
            derivation._beava_schema = compiled  # type: ignore[attr-defined]
        except TypeError as exc:
            import warnings
            warnings.warn(
                f"@bv.table (derived {gen_name!r}): typed schema compile "
                f"failed ({exc}); falling back to untyped REGISTER.",
                category=UserWarning,
                stacklevel=2,
            )
            derivation._beava_schema = None  # type: ignore[attr-defined]
        return derivation


__all__ = ["GroupBy", "AggregationSpec"]
