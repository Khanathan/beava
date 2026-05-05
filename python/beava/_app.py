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
        # Walk back through parents to find the root upstream (an
        # EventSource OR a named EventDerivation registered separately).
        # For ``@bv.table def F(x: SomeEvent)`` the parent IS already the
        # named upstream — just use it. For chained derivations the root
        # is the original event source.
        parent = getattr(d, "_parent", None)
        # If parent is a derivation that was given a stable name via
        # .named(...), prefer that as the upstream (it'll be registered
        # as a separate node). Otherwise walk to the root.
        if parent is not None:
            parent_kind = getattr(parent, "_kind", None)
            parent_name = getattr(parent, "_name", "")
            # Named-derivation upstreams (from .named(...)) keep their
            # stable name; intermediate auto-named ones (Foo__derived_N)
            # should resolve to the root.
            if parent_kind == "event_source" or "__derived_" not in parent_name:
                upstreams = [parent_name] if parent_name else []
            else:
                root: Any = parent
                while True:
                    p = getattr(root, "_parent", None)
                    if p is None:
                        break
                    root = p
                upstreams = [getattr(root, "_name", "")]
        else:
            upstreams = []
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
        # Walk back through the parent chain to find the original
        # event-source. Each EventDerivation step's _parent is the previous
        # derivation; the root EventSource is the only node with kind=event_source.
        ev_root: Any = d
        while True:
            p = getattr(ev_root, "_parent", None)
            if p is None:
                break
            ev_root = p
        upstreams = (
            [getattr(ev_root, "_name", "")] if ev_root is not d else []
        )
        ops = _chain_to_ops(chain)
        # output_kind: "table" if the chain terminates in an agg step
        # (event → table boundary); "event" otherwise (event-only
        # transform: filter/select/with_columns/etc.).
        has_agg = any(step.get("op") == "agg" for step in chain)
        output_kind = "table" if has_agg else "event"
        parent_schema = _parent_wire_schema_from_root(ev_root)
        if has_agg:
            fields = _infer_derivation_schema(chain, parent_schema, [])
        else:
            # Event-derivation: schema = upstream fields ∪ with_columns / rename targets.
            fields = _infer_event_derivation_schema(chain, parent_schema)
        return {
            "kind": "derivation",
            "name": getattr(d, "_name", ""),
            "output_kind": output_kind,
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


# Phase 13.5.1 Plan 07b (Deviation 3 FORMALIZE-V0): Plan 05's
# `_FIXED_OP_OUTPUT_TYPE` mirror of Rust `output_type_for` is removed.
# Server's `DerivationDescriptor.schema` is now `serde(default)`-able
# (registry.rs) and `validate_expressions` infers schema.fields from the
# chain at register-time via the existing `OpChain::compile` ->
# `propagated_schemas` pipeline. The server is the single source of truth.
#
# `_infer_derivation_schema` now returns an empty dict; the server fills
# it in. We keep the function so existing callers in `_descriptor_to_node`
# don't break, but it's a no-op shim.


def _infer_derivation_schema(
    chain: list[dict[str, Any]],
    parent_schema_wire: dict[str, str],
    key_cols: list[str],
) -> dict[str, str]:
    """No-op shim post-Plan 07b — server infers schema at register-time.

    Pre-07b this mirrored Rust ``output_type_for`` to populate the
    derivation's ``schema.fields`` map (server's deserializer used to
    require it). Post-07b ``DerivationDescriptor.schema`` is
    ``serde(default)`` and ``validate_expressions`` writes the inferred
    schema back via ``propagated_schemas``. We return an empty dict;
    server fills it. Function retained as a stable extension point in
    case future v0.0.x work needs Python-side previewing.
    """
    del chain, parent_schema_wire, key_cols  # unused
    return {}


def _parent_wire_schema(d: Any) -> dict[str, str]:
    """Best-effort extraction of the upstream event source's wire schema.

    Returns ``{field-name: wire-type-str}`` derived from the parent's
    ``_schema`` attribute (a ``{name: python-type}`` map populated by
    ``@bv.event``). Empty dict if the parent or schema is missing.

    Walks the parent chain to find the root event source (the only one
    with ``_kind == "event_source"`` and a populated ``_schema`` map).
    """
    cur = getattr(d, "_parent", None)
    while cur is not None:
        kind = getattr(cur, "_kind", None)
        schema_dict = getattr(cur, "_schema", None)
        if kind == "event_source" and schema_dict:
            out: dict[str, str] = {}
            for fname, ftype in schema_dict.items():
                out[fname] = _python_type_to_wire(ftype)
            return out
        cur = getattr(cur, "_parent", None)
    return {}


def _parent_wire_schema_from_root(root: Any) -> dict[str, str]:
    """Variant of ``_parent_wire_schema`` that takes the root EventSource directly."""
    if root is None:
        return {}
    schema_dict = getattr(root, "_schema", {}) or {}
    out: dict[str, str] = {}
    for fname, ftype in schema_dict.items():
        out[fname] = _python_type_to_wire(ftype)
    return out


def _infer_event_derivation_schema(
    chain: list[dict[str, Any]],
    parent_schema_wire: dict[str, str],
) -> dict[str, str]:
    """Compute schema.fields for an event-derivation (no agg) chain.

    Starts with the upstream event-source's schema and applies
    ``with_columns`` / ``rename`` / ``drop`` / ``select`` op steps to
    narrow / extend / rename the field set. Best-effort — server's own
    schema propagation is the authoritative source; this just produces
    a schema dict with ``len > 0`` to satisfy the registration
    deserializer's ``derivation schema must have at least one field``
    invariant.
    """
    fields: dict[str, str] = dict(parent_schema_wire)
    for step in chain:
        op = step.get("op")
        if op == "with_columns" or op == "map":
            # Add new columns; type is "str" for literal exprs (best-effort).
            for col in step.get("exprs", {}):
                if col not in fields:
                    fields[col] = "str"
        elif op == "rename":
            mapping = step.get("mapping", {})
            for old, new in mapping.items():
                if old in fields:
                    fields[new] = fields.pop(old)
        elif op == "drop":
            for col in step.get("cols", []):
                fields.pop(col, None)
        elif op == "select":
            cols = step.get("cols", [])
            fields = {c: fields.get(c, "str") for c in cols}
        elif op == "cast":
            type_map = step.get("type_map", {})
            for col, ttype in type_map.items():
                if col in fields:
                    # Map cast targets to wire-type strings.
                    fields[col] = {
                        "str": "str",
                        "int": "i64",
                        "float": "f64",
                        "bool": "bool",
                    }.get(ttype, "str")
        elif op == "fillna":
            # No schema change — only fills missing values.
            pass
        elif op == "filter":
            # No schema change.
            pass
        elif op == "rename_self":
            # Python-side noop.
            pass
    # Always ensure at least one field — fallback to upstream key (best-effort).
    if not fields and parent_schema_wire:
        # Take the first upstream field as a placeholder.
        first_field = next(iter(parent_schema_wire))
        fields[first_field] = parent_schema_wire[first_field]
    return fields


def _chain_to_ops(chain: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert chain steps (list of ``{"op": ..., ...}`` dicts) to wire-shape
    ops. The aggregation step shape uses ``{"op": "group_by", "keys": [...],
    "agg": {name: {"op": <op>, "params": {...}}}}``.

    ``rename_self`` is a Python-side chain-noop introduced by ``.named(...)``
    (events.py:99-106) — it tags the derivation with a stable name but is
    not a recognized server-side op variant. Skip it here so the wire
    payload only carries server-recognized ops.
    """
    out: list[dict[str, Any]] = []
    for step in chain:
        op = step.get("op")
        if op == "rename_self":
            # Python-side noop; ``_name`` is already populated on the
            # derivation by ``.named(...)`` and emitted in
            # ``_descriptor_to_node``. Strip from the wire payload.
            continue
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
