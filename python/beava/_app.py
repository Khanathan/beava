"""bv.App — Phase 13.5 v0 client core.

Public client class implementing the 7 wire-mapped lifecycle methods documented
in docs/sdk-api/python.md § App class:

  - register(*descriptors, force=False, dry_run=False) → dict
  - push(event_name, fields)                            → dict
  - get(table, key=None)                                → dict
  - batch_get(requests)                                 → list[dict]
  - reset()                                             → None
  - ping()                                              → dict
  - close()                                             → None

Transport selection is via URL scheme (handled by `make_transport`):
  http(s):// → HttpTransport
  tcp://     → TcpTransport
  None       → EmbedTransport (spawns local binary)

Per Phase 13.5 D-05 (cross-amendment from 13.4 D-03), the `test_mode=True`
kwarg propagates BEAVA_TEST_MODE=1 to the spawned binary in embed mode; in
network mode (url is set), test_mode is ignored with a UserWarning.
"""
from __future__ import annotations

import json
import warnings
from typing import Any
from urllib.parse import urlparse

from beava._transport import Transport, make_transport


def _to_register_json(
    descriptors: tuple[Any, ...],
    *,
    force: bool = False,
    dry_run: bool = False,
) -> bytes:
    """Convert ``@bv.event`` / ``@bv.table`` descriptors into a UTF-8 JSON
    payload matching the wire-spec ``{"nodes": [...]}`` shape.

    Plan 11 contract — exposed at module level so ``python/tests/v0/test_lit.py``
    + ``test_global.py`` can introspect the wire payload independently of an
    actual transport.
    """
    nodes: list[dict[str, Any]] = []
    for d in descriptors:
        node = _descriptor_to_node(d)
        if node is None:
            continue
        nodes.append(node)
    payload: dict[str, Any] = {"nodes": nodes}
    if force:
        payload["force"] = True
    if dry_run:
        payload["dry_run"] = True
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _descriptor_to_node(d: Any) -> dict[str, Any] | None:
    """Best-effort serialization of a single descriptor to wire-shape JSON.

    Recognised inputs:
      - ``EventSource`` (class with ``_kind == "event_source"``)
      - ``EventDerivation``
      - ``TableDescriptor``
      - Plain dict (passthrough — for tests that hand-build payloads)
    """
    # Plain dict — assume already wire-shaped.
    if isinstance(d, dict):
        return d
    kind = getattr(d, "_kind", None)
    if kind in ("event_source", None) and hasattr(d, "_schema"):
        # @bv.event class form.
        name = getattr(d, "_name", getattr(d, "__name__", None))
        if not name:
            return None
        schema_dict = getattr(d, "_schema", {}) or {}
        fields_obj: dict[str, str] = {}
        for fname, ftype in schema_dict.items():
            fields_obj[fname] = _python_type_to_wire(ftype)
        return {
            "kind": "event",
            "name": name,
            "schema": {"fields": fields_obj, "optional_fields": []},
        }
    if kind == "table":
        # @bv.table descriptor.
        chain = getattr(d, "_chain", []) or []
        key_cols = getattr(d, "_key_cols", []) or []
        parent = getattr(d, "_parent", None)
        upstreams = [getattr(parent, "_name", "")] if parent is not None else []
        ops = _chain_to_ops(chain)
        # Phase 13.5.1 Plan 05 (Rule 3 — schema is required by the server's
        # `DerivationDescriptor` deserializer). Compute schema.fields from
        # key cols + chain agg outputs.
        parent_schema = _parent_wire_schema(d)
        fields = _infer_derivation_schema(chain, parent_schema, list(key_cols))
        return {
            "kind": "derivation",
            "name": getattr(d, "_name", ""),
            "output_kind": "table",
            "upstreams": upstreams,
            "ops": ops,
            "schema": {"fields": fields, "optional_fields": []},
            "table_primary_key": list(key_cols),
        }
    if kind in ("event_derivation", "aggregation"):
        chain = getattr(d, "_chain", []) or []
        parent = getattr(d, "_parent", None)
        upstreams = [getattr(parent, "_name", "")] if parent is not None else []
        ops = _chain_to_ops(chain)
        parent_schema = _parent_wire_schema(d)
        fields = _infer_derivation_schema(chain, parent_schema, [])
        return {
            "kind": "derivation",
            "name": getattr(d, "_name", ""),
            "output_kind": "table",
            "upstreams": upstreams,
            "ops": ops,
            "schema": {"fields": fields, "optional_fields": []},
        }
    return None


def _python_type_to_wire(t: Any) -> str:
    """Map Python type / annotation to wire-format type string."""
    if t in (str,) or getattr(t, "__name__", "") == "str":
        return "str"
    if t in (int,) or getattr(t, "__name__", "") == "int":
        return "i64"
    if t in (float,) or getattr(t, "__name__", "") == "float":
        return "f64"
    if t in (bool,) or getattr(t, "__name__", "") == "bool":
        return "bool"
    return "str"


# Agg-op output type mapping — mirrors Rust `output_type_for` in
# `crates/beava-core/src/agg_op.rs::output_type_for`. Used by
# `_infer_derivation_schema` to populate the `schema.fields` map of a
# `kind: derivation` node so the server-side `DerivationDescriptor`
# deserializer (which requires `schema`) accepts the payload.
#
# Phase 13.5.1 Plan 05 (Rule 3 — blocking issue auto-fix). Plan 13.5-11 left
# `_descriptor_to_node` emitting derivation nodes without a `schema` field;
# server rejected with `invalid_registration: missing field schema`. The fix
# infers the schema Python-side from the chain.

# Ops whose output type is fixed (independent of field).
_FIXED_OP_OUTPUT_TYPE: dict[str, str] = {
    # Core scalar ops
    "count": "i64",
    "sum": "f64",
    "mean": "f64",
    "avg": "f64",  # legacy alias
    "var": "f64",
    "variance": "f64",  # legacy alias
    "std": "f64",
    "stddev": "f64",  # legacy alias
    "ratio": "f64",
    # Sketch family
    "n_unique": "i64",
    "count_distinct": "i64",  # legacy alias
    "quantile": "f64",
    "percentile": "f64",  # legacy alias
    "top_k": "str",  # JSON-array-as-string per Rust schema propagation
    "bloom_member": "bool",
    "entropy": "f64",
    # Recency / streak
    "first_seen": "i64",  # Datetime → i64 epoch ms on the wire
    "last_seen": "i64",
    "age": "i64",
    "time_since": "i64",
    "time_since_last_n": "i64",
    "has_seen": "bool",
    "first_seen_in_window": "bool",
    "streak": "i64",
    "max_streak": "i64",
    "negative_streak": "i64",
    # Decay / velocity / z
    "ewma": "f64",
    "ema": "f64",
    "ewvar": "f64",
    "ew_zscore": "f64",
    "decayed_sum": "f64",
    "decayed_count": "f64",
    "twa": "f64",
    "rate_of_change": "f64",
    "inter_arrival_stats": "f64",
    "trend": "f64",
    "trend_residual": "f64",
    "z_score": "f64",
    "burst_count": "i64",
    "outlier_count": "i64",
    "value_change_count": "i64",
    # Phase 8 point ops returning JSON-array-as-string
    "first_n": "str",
    "last_n": "str",
    # Phase 11 buffer / histogram / mix → str (Json placeholder)
    "histogram": "str",
    "hour_of_day_histogram": "str",
    "dow_hour_histogram": "str",
    "event_type_mix": "str",
    "most_recent_n": "str",
    "reservoir_sample": "str",
    # Phase 11 scalar geo + seasonal
    "seasonal_deviation": "f64",
    "geo_velocity": "f64",
    "geo_distance": "f64",
    "geo_spread": "f64",
    "distance_from_home": "f64",
}

# Ops whose output type inherits from the named upstream field.
_FIELD_INHERITING_OPS: frozenset[str] = frozenset(
    {"min", "max", "first", "last", "lag", "delta_from_prev"}
)


def _agg_output_type(
    op_name: str, field_name: str | None, upstream_schema: dict[str, str]
) -> str:
    """Return the wire-type string for an agg op's output column.

    Mirrors Rust ``crates/beava-core/src/agg_op.rs::output_type_for``.
    Falls back to ``str`` for unknown ops (server will validate at
    register time and reject if the inferred type is wrong; the goal here
    is to populate the schema field shape so the deserializer accepts the
    payload — incorrect inferred types surface as ``invalid_registration``
    with a helpful server-side message).
    """
    if op_name in _FIXED_OP_OUTPUT_TYPE:
        return _FIXED_OP_OUTPUT_TYPE[op_name]
    if op_name in _FIELD_INHERITING_OPS:
        if field_name is not None and field_name in upstream_schema:
            return upstream_schema[field_name]
        # Fallback when field can't be resolved.
        return "str"
    # Unknown op — defer to the server's validate pass.
    return "str"


def _infer_derivation_schema(
    chain: list[dict[str, Any]],
    parent_schema_wire: dict[str, str],
    key_cols: list[str],
) -> dict[str, str]:
    """Compute a derivation node's ``schema.fields`` map from its chain.

    Walks the chain looking for the (single) ``agg`` step (always present
    as the terminal step in a v0 ``@bv.table`` function). Combines key
    columns (typed via ``parent_schema_wire`` lookup; ``str`` fallback)
    with each agg's output column type via ``_agg_output_type``.

    Args:
        chain: The list of chain steps (raw, pre-``_chain_to_ops`` form).
        parent_schema_wire: Wire-format ``{field: type-str}`` map of the
            upstream event source's schema.
        key_cols: Table key columns (post-``@bv.table(key=...)``).

    Returns:
        ``{column-name: wire-type-str}`` ordered with key cols first, then
        agg output columns. Used as the ``fields`` value of the emitted
        ``schema`` JSON object.
    """
    fields: dict[str, str] = {}
    # 1. Key columns — type from parent schema, fallback to str.
    for k in key_cols:
        fields[k] = parent_schema_wire.get(k, "str")
    # 2. Agg output columns — walk the chain to find the agg step.
    for step in chain:
        if step.get("op") != "agg":
            continue
        # Per `_chain_to_ops`, agg step shape: {"op":"agg", "keys":[...], "aggs":{name: spec}}
        aggs = step.get("aggs", {})
        for out_name, spec in aggs.items():
            if isinstance(spec, dict):
                op_name = spec.get("op", "count")
                field_name = spec.get("field")
            else:
                op_name = "count"
                field_name = None
            fields[out_name] = _agg_output_type(
                op_name, field_name, parent_schema_wire
            )
    return fields


def _parent_wire_schema(d: Any) -> dict[str, str]:
    """Best-effort extraction of the upstream event source's wire schema.

    Returns ``{field-name: wire-type-str}`` derived from the parent's
    ``_schema`` attribute (a ``{name: python-type}`` map populated by
    ``@bv.event``). Empty dict if the parent or schema is missing.
    """
    parent = getattr(d, "_parent", None)
    if parent is None:
        return {}
    schema_dict = getattr(parent, "_schema", {}) or {}
    out: dict[str, str] = {}
    for fname, ftype in schema_dict.items():
        out[fname] = _python_type_to_wire(ftype)
    return out


def _chain_to_ops(chain: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert chain steps (list of ``{"op": ..., ...}`` dicts) to wire-shape
    ops. The aggregation step shape uses ``{"op": "group_by", "keys": [...],
    "agg": {name: {"op": <op>, "params": {...}}}}``.
    """
    out: list[dict[str, Any]] = []
    for step in chain:
        op = step.get("op")
        if op == "agg":
            keys = step.get("keys", [])
            aggs = step.get("aggs", {})
            agg_obj: dict[str, Any] = {}
            for name, spec in aggs.items():
                if isinstance(spec, dict):
                    op_name = spec.get("op", "count")
                    params = {k: v for k, v in spec.items() if k != "op"}
                    agg_obj[name] = {"op": op_name, "params": params}
                else:
                    agg_obj[name] = {"op": "count", "params": {}}
            out.append({"op": "group_by", "keys": list(keys), "agg": agg_obj})
        else:
            # Pass-through for filter/select/etc.
            out.append(dict(step))
    return out


class App:
    """Beava synchronous client — 7 wire-mapped methods + context manager.

    Embed mode (``url=None``) REQUIRES use as a context manager so the
    spawned binary is torn down on exit. Network mode may be used either
    way.

    Args:
        url: Server URL. ``http://...`` / ``https://...`` for HTTP transport,
            ``tcp://...`` for the custom-framed TCP fast-path. ``None``
            (default) for embed mode (local binary spawn).
        timeout: Socket / HTTP timeout in seconds. Defaults to 30.0.
        test_mode: When True + embed mode, sets BEAVA_TEST_MODE=1 in the
            spawned binary's env (gates OP_RESET and other test-only
            opcodes per Phase 13.4 D-03). When True + network mode,
            emits a UserWarning and proceeds without effect.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        timeout: float = 30.0,
        test_mode: bool = False,
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._test_mode = test_mode
        self._transport: Transport | None = None
        self._closed = False
        self._entered = False

        # Transport-kind classification — used by tests + close() routing.
        if url is None:
            self._transport_kind = "embed"
        else:
            scheme = urlparse(url).scheme
            if scheme in ("http", "https"):
                self._transport_kind = "http"
                if test_mode:
                    warnings.warn(
                        "test_mode kwarg ignored for network mode (url is set); "
                        "server controls test mode in network mode per Phase 13.4 D-03.",
                        UserWarning,
                        stacklevel=2,
                    )
            elif scheme == "tcp":
                self._transport_kind = "tcp"
                if test_mode:
                    warnings.warn(
                        "test_mode kwarg ignored for network mode (url is set); "
                        "server controls test mode in network mode per Phase 13.4 D-03.",
                        UserWarning,
                        stacklevel=2,
                    )
            else:
                raise ValueError(f"unsupported URL scheme: {scheme!r}")

    def __enter__(self) -> "App":
        self._entered = True
        if self._transport is None:
            self._transport = make_transport(
                url=self._url, timeout=self._timeout, test_mode=self._test_mode
            )
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        if self._closed:
            return
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
        self._closed = True

    def _require_transport(self) -> Transport:
        if self._closed:
            raise RuntimeError("App is closed")
        if self._transport_kind == "embed" and not self._entered:
            raise RuntimeError(
                "Embed mode requires use as a context manager: "
                "`with bv.App() as app:`"
            )
        if self._transport is None:
            self._transport = make_transport(
                url=self._url, timeout=self._timeout, test_mode=self._test_mode
            )
        return self._transport

    # ── 7 wire-mapped methods ──────────────────────────────────────────────

    def register(
        self,
        *descriptors: Any,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Register one or more event/table descriptors with the server.

        Args:
            *descriptors: Descriptor objects produced by ``@bv.event`` /
                ``@bv.table`` decorators (Plan 03).
            force: If True, server replaces any existing pipeline of the
                same name.
            dry_run: If True, server validates and returns a categorized
                diff payload but does not commit. Implies dry_run=True
                response shape per docs/error-codes.md.
        """
        t = self._require_transport()
        payload = _to_register_json(descriptors, force=force, dry_run=dry_run)
        result: dict[str, Any] = t.send_register(payload)
        return result

    def push(self, event_name: str, fields: dict[str, Any]) -> dict[str, Any]:
        """Push a single event to the server.

        Args:
            event_name: Name of the registered event type.
            fields: Event fields as a plain Python dict.
        """
        t = self._require_transport()
        result: dict[str, Any] = t.send_push(event_name=event_name, fields=fields)
        return result

    def get(
        self,
        table: str,
        key: str | list[str | int | bool] | None = None,
        features: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get a single feature row by entity key.

        Per ADR-003 global-aggregation semantics, calling ``get(table)``
        without a key (or with key=None) routes to the global table
        sentinel (empty-string entity_id).

        ``features`` (D-03 USER-LOCKED, Phase 13.5.1): when provided, the
        server narrows the returned row to the named subset. Default
        ``None`` returns the full row (Redis-shaped).
        """
        t = self._require_transport()
        effective_key: str | list[Any] = "" if key is None else key
        result: dict[str, Any] = t.send_get(
            table=table, key=effective_key, features=features
        )
        return result

    def batch_get(
        self,
        requests: list[
            tuple[str, str | list[str | int | bool]]
            | tuple[str, str | list[str | int | bool], list[str] | None]
        ],
    ) -> list[dict[str, Any]]:
        """Batch GET — N requests, returns a list of dicts in the same order.

        Args:
            requests: A list of per-entry tuples. Each entry is EITHER a
                ``(table, key)`` 2-tuple OR a ``(table, key, features)``
                3-tuple where ``features`` is an optional ``list[str]``
                filter (per D-03 USER-LOCKED — same shape as ``app.get``'s
                ``features`` kwarg, applied per entry). The 2-tuple form
                returns the full row; the 3-tuple form narrows to the
                named features. Both shapes can be mixed within the same
                call. Cold-start per-entry is ``{}``.
        """
        t = self._require_transport()
        coerced: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ] = []
        for entry in requests:
            # D-03 explicitly accepts ONLY the tuple shape (2 or 3); reject
            # dict entries here so callers fall through to the tuple form
            # (tests/v0/test_transport_equivalence.py:246-260 relies on this
            # to test BOTH shapes — the dict path raises TypeError, the
            # fallback tuple path is the lock-locked Plan-05 contract).
            if not isinstance(entry, tuple):
                raise TypeError(
                    f"batch_get request entry must be a tuple "
                    f"(table, key) or (table, key, features); "
                    f"got {type(entry).__name__}"
                )
            if len(entry) == 2:
                tbl, k = entry
                coerced.append((tbl, k))
            elif len(entry) == 3:
                tbl, k, feats = entry
                coerced.append((tbl, k, feats))
            else:
                raise TypeError(
                    f"batch_get request entry must be a 2- or 3-tuple "
                    f"(table, key) or (table, key, features); "
                    f"got {len(entry)}-tuple"
                )
        result: list[dict[str, Any]] = t.send_batch_get(requests=coerced)
        return result

    def reset(self) -> None:
        """Reset all server state (test-mode-gated per Phase 13.4 D-03).

        Raises:
            RuntimeError: If the server is not in test mode (error code
                ``reset_disabled_in_production``).
        """
        t = self._require_transport()
        t.send_reset()

    def ping(self) -> dict[str, Any]:
        """Server liveness check; returns server_version + registry_version."""
        t = self._require_transport()
        result: dict[str, Any] = t.send_ping()
        return result
