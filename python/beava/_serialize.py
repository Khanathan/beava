"""REGISTER JSON serializer — the contract Phase 22/23 executors consume.

``compile_to_register_json(descriptor)`` dispatches on descriptor kind and
emits the canonical JSON payload. ``_collect_registrations`` walks a
descriptor's upstreams (including aggregation sources, join right sides,
and union sources) in topological order, deduping by ``_name``.

This module is the single source of truth for the wire format. Phase 22
treats the output dicts verbatim; Phase 23 likewise for joins.

Payload shapes (one per descriptor kind):

  * StreamSource / TableSource:
      {name, kind, key_field, [key_fields], mode?, fields, [history_ttl],
       [entity_ttl]}

  * StreamDerivation / TableDerivation carrying only _ops (stateless chain):
      {name, kind, upstream, ops, fields, depends_on}

  * TableDerivation wrapping an AggregationSpec:
      {name, kind: "table", key_field, aggregation: {
          source: <upstream._name>, keys: [...],
          features: [op.to_json(name) for each feature]
      }, fields, depends_on}

  * StreamDerivation / TableDerivation wrapping a JoinSpec:
      {name, kind, join: {left, right, on, within?, type, shape}, fields,
       depends_on}

  * StreamDerivation wrapping a UnionSpec:
      {name, kind: "stream", union: {sources: [...]}, fields, depends_on}
"""

from __future__ import annotations

from typing import Any

from beava._types_core import FieldSpec


def _schema_to_fields_dict(schema: dict[str, FieldSpec]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, spec in schema.items():
        entry: dict[str, Any] = {
            "type": spec.py_type.__name__,
            "optional": spec.optional,
        }
        if spec.desc is not None:
            entry["desc"] = spec.desc
        out[name] = entry
    return out


def _compile_source(descriptor: Any) -> dict[str, Any]:
    """StreamSource / TableSource."""
    from beava._stream import StreamSource
    from beava._table import TableSource

    d: dict[str, Any] = {
        "name": descriptor._name,
        "fields": _schema_to_fields_dict(descriptor._schema),
    }

    if isinstance(descriptor, StreamSource):
        d["kind"] = "stream"
        d["key_field"] = None
        if getattr(descriptor, "_history_ttl", None) is not None:
            d["history_ttl"] = descriptor._history_ttl
        # D-11 / CORR-03: emit per-stream watermark lateness override when set.
        # Server-side SourceDescriptor receives this as Option<String> and
        # parses via parse_duration_str → StreamDefinition.watermark_lateness.
        if getattr(descriptor, "_watermark_lateness", None) is not None:
            d["watermark_lateness"] = descriptor._watermark_lateness
        # D-07 / TPC-DX-01: emit shard_key in REGISTER payload when declared.
        # str → ShardKeySpec::Single on server; tuple → ShardKeySpec::Tuple.
        sk = getattr(descriptor, "_beava_shard_key", None)
        if sk is not None:
            if isinstance(sk, str):
                d["shard_key"] = sk
            elif isinstance(sk, tuple):
                d["shard_key"] = list(sk)
    elif isinstance(descriptor, TableSource):
        # Phase 56-NEXT #6: @bv.source_table emits kind="source_table" so the
        # server's REGISTER dispatch routes it through SourceTableDescriptor
        # and has_registered_source_table(name) returns true — a prerequisite
        # for OP_UPSERT_TABLE_ROW / OP_DELETE_TABLE_ROW to be accepted.
        from beava._table import SourceTable

        is_source_table = isinstance(descriptor, SourceTable)
        d["kind"] = "source_table" if is_source_table else "table"
        d["mode"] = descriptor._mode
        key = list(descriptor._key)
        if is_source_table:
            # Always emit key_fields array for source tables — matches the
            # in-process `register_source_table()` helper's signature.
            d["key_field"] = None
            d["key_fields"] = key
        elif len(key) == 1:
            d["key_field"] = key[0]
        else:
            d["key_field"] = None
            d["key_fields"] = key
        if getattr(descriptor, "_ttl", None) is not None:
            d["entity_ttl"] = descriptor._ttl
    return d


def _compile_aggregation(descriptor: Any) -> dict[str, Any]:
    """TableDerivation wrapping an AggregationSpec."""
    spec = descriptor._agg_spec
    source = spec.upstream
    d: dict[str, Any] = {
        "name": descriptor._name,
        "kind": "table",
        "fields": _schema_to_fields_dict(descriptor._schema),
        "mode": descriptor._mode,
        "aggregation": {
            "source": source._name,
            "keys": list(spec.keys),
            "features": spec._to_feature_list(),
        },
        "depends_on": [source._name],
    }
    key = list(descriptor._key)
    if len(key) == 1:
        d["key_field"] = key[0]
    else:
        d["key_field"] = None
        d["key_fields"] = key
    return d


def _compile_join(descriptor: Any) -> dict[str, Any]:
    """Stream/Table derivation wrapping a JoinSpec."""
    spec = descriptor._join_spec
    from beava._stream import StreamDerivation
    from beava._table import TableDerivation

    kind = "stream" if isinstance(descriptor, StreamDerivation) else "table"
    d: dict[str, Any] = {
        "name": descriptor._name,
        "kind": kind,
        "fields": _schema_to_fields_dict(descriptor._schema),
        "join": spec._to_join_json(),
        "depends_on": [u._name for u in descriptor._upstreams],
    }
    if isinstance(descriptor, TableDerivation):
        d["mode"] = descriptor._mode
        key = list(descriptor._key)
        if len(key) == 1:
            d["key_field"] = key[0]
        else:
            d["key_field"] = None
            d["key_fields"] = key
    else:
        d["key_field"] = None
    return d


def _compile_union(descriptor: Any) -> dict[str, Any]:
    """StreamDerivation wrapping a UnionSpec."""
    spec = descriptor._union_spec
    return {
        "name": descriptor._name,
        "kind": "stream",
        "key_field": None,
        "fields": _schema_to_fields_dict(descriptor._schema),
        "union": spec._to_union_json(),
        "depends_on": [s._name for s in spec.sources],
    }


def _compile_op_chain(descriptor: Any) -> dict[str, Any]:
    """StreamDerivation / TableDerivation carrying only stateless _ops."""
    from beava._stream import StreamDerivation
    from beava._table import TableDerivation

    kind = "stream" if isinstance(descriptor, StreamDerivation) else "table"
    d: dict[str, Any] = {
        "name": descriptor._name,
        "kind": kind,
        "fields": _schema_to_fields_dict(descriptor._schema),
        "ops": list(descriptor._ops),
        "depends_on": [u._name for u in descriptor._upstreams],
    }
    if isinstance(descriptor, TableDerivation):
        d["mode"] = descriptor._mode
        key = list(descriptor._key)
        if len(key) == 1:
            d["key_field"] = key[0]
        else:
            d["key_field"] = None
            d["key_fields"] = key
        if getattr(descriptor, "_ttl", None) is not None:
            d["entity_ttl"] = descriptor._ttl
    else:
        d["key_field"] = None
    return d


def compile_to_register_json(descriptor: Any) -> dict[str, Any]:
    """Emit the REGISTER JSON payload for a single descriptor.

    Dispatches on descriptor kind:

      * Aggregation-bearing TableDerivation → aggregation payload.
      * Join-bearing Stream/TableDerivation → join payload.
      * Union-bearing StreamDerivation → union payload.
      * Stateless-chain derivation → op-chain payload.
      * Source (Stream/Table) → source payload.
    """
    from beava._stream import StreamSource, StreamDerivation
    from beava._table import TableSource, TableDerivation

    if isinstance(descriptor, (StreamSource, TableSource)):
        return _compile_source(descriptor)

    if isinstance(descriptor, TableDerivation):
        if getattr(descriptor, "_agg_spec", None) is not None:
            return _compile_aggregation(descriptor)
        if getattr(descriptor, "_join_spec", None) is not None:
            return _compile_join(descriptor)
        return _compile_op_chain(descriptor)

    if isinstance(descriptor, StreamDerivation):
        if getattr(descriptor, "_join_spec", None) is not None:
            return _compile_join(descriptor)
        if getattr(descriptor, "_union_spec", None) is not None:
            return _compile_union(descriptor)
        return _compile_op_chain(descriptor)

    raise TypeError(
        f"compile_to_register_json: unsupported descriptor "
        f"{type(descriptor).__name__}"
    )


def _descriptor_upstreams(descriptor: Any) -> list[Any]:
    """Return the descriptors this one depends on for registration order.

    Covers: _upstreams list (set by stateless ops and decorators), the
    aggregation source, the join's left+right, and the union's sources.
    Duplicates are removed preserving first-seen order.
    """
    seen: dict[int, Any] = {}

    # Op-chain / decorator upstreams.
    for u in getattr(descriptor, "_upstreams", []) or []:
        if id(u) not in seen:
            seen[id(u)] = u

    # Aggregation source (single upstream).
    spec = getattr(descriptor, "_agg_spec", None)
    if spec is not None:
        u = spec.upstream
        if id(u) not in seen:
            seen[id(u)] = u

    # Join has left + right.
    jspec = getattr(descriptor, "_join_spec", None)
    if jspec is not None:
        for u in (jspec.left, jspec.right):
            if id(u) not in seen:
                seen[id(u)] = u

    # Union sources.
    uspec = getattr(descriptor, "_union_spec", None)
    if uspec is not None:
        for u in uspec.sources:
            if id(u) not in seen:
                seen[id(u)] = u

    return list(seen.values())


def collect_registrations(descriptor: Any) -> list[dict[str, Any]]:
    """Walk upstreams depth-first, dedupe by name, append self's frame.

    Returns a topologically-ordered list of REGISTER JSON dicts ready to be
    sent to the engine one-by-one.
    """
    out: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    def walk(node: Any) -> None:
        for u in _descriptor_upstreams(node):
            walk(u)
        name = node._name
        if name in seen_names:
            return
        seen_names.add(name)
        out.append(compile_to_register_json(node))

    walk(descriptor)
    return out


__all__ = ["compile_to_register_json", "collect_registrations"]
