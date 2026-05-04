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
        return {
            "kind": "derivation",
            "name": getattr(d, "_name", ""),
            "output_kind": "table",
            "upstreams": upstreams,
            "ops": ops,
            "table_primary_key": list(key_cols),
        }
    if kind in ("event_derivation", "aggregation"):
        chain = getattr(d, "_chain", []) or []
        parent = getattr(d, "_parent", None)
        upstreams = [getattr(parent, "_name", "")] if parent is not None else []
        ops = _chain_to_ops(chain)
        return {
            "kind": "derivation",
            "name": getattr(d, "_name", ""),
            "output_kind": "table",
            "upstreams": upstreams,
            "ops": ops,
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
    ) -> dict[str, Any]:
        """Get a single feature row by entity key.

        Per ADR-003 global-aggregation semantics, calling ``get(table)``
        without a key (or with key=None) routes to the global table
        sentinel (empty-string entity_id).
        """
        t = self._require_transport()
        effective_key: str | list[Any] = "" if key is None else key
        result: dict[str, Any] = t.send_get(table=table, key=effective_key)
        return result

    def batch_get(
        self,
        requests: list[tuple[str, str | list[str | int | bool]]],
    ) -> list[dict[str, Any]]:
        """Batch GET — N requests, returns a list of dicts in the same order.

        Args:
            requests: A list of ``(table, key)`` tuples. Each entry yields
                one dict in the response list at the matching index.
        """
        t = self._require_transport()
        coerced: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ] = [(tbl, k) for tbl, k in requests]
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
