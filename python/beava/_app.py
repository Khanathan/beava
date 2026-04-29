"""``bv.App`` — sync client composing transport + local DAG validation.

Public API (re-exported from beava.__init__):
  - App: user-facing sync client

URL dispatch:
  ``http://...`` / ``https://...``  → :class:`HttpTransport`
  ``tcp://...``                     → :class:`TcpTransport`
  ``None``                          → embed mode: auto-spawns a local beava binary

Lifecycle:
  - Explicit URL mode (``bv.App('http://...')``): context manager optional.
  - Embed mode (``bv.App()``): MUST use as context manager; ``register()`` raises
    ``RuntimeError`` before ``__enter__`` is called.

Context manager:
  - ``__enter__`` initialises the embed transport (if needed) and returns ``self``.
  - ``__exit__`` calls ``close()``.
  - ``close()`` is idempotent — safe to call multiple times.
"""

from __future__ import annotations

import json
from typing import Any

from beava._errors import RegistrationError, ValidationError
from beava._transport import Transport, parse_url_to_transport
from beava._validate import topo_sort, validate_descriptors


class App:
    """Sync client for the Beava feature server.

    Usage (explicit URL)::

        with bv.App("http://localhost:7379") as app:
            app.register(Transaction, UserProfile)

        # or without context manager (explicit URL mode)
        app = bv.App("tcp://localhost:7380")
        app.register(Transaction)
        app.close()

    Usage (embed mode — spawns a local beava subprocess)::

        with bv.App() as app:
            app.register(Transaction)
        # subprocess is terminated on exit

    Args:
        url: Server URL.  Accepted schemes: ``http://``, ``https://``, ``tcp://``.
             Pass ``None`` (the default) for embed mode.
        timeout: Transport-level I/O timeout in seconds (default 30.0).
    """

    def __init__(self, url: str | None = None, *, timeout: float = 30.0) -> None:
        self._url = url
        self._timeout = timeout
        self._transport: Transport | None = None
        self._entered: bool = False
        self._closed: bool = False

        if url is not None:
            # Explicit URL mode: create the transport eagerly.
            # Embed mode transport is deferred until __enter__.
            self._transport = parse_url_to_transport(url)

    # ------------------------------------------------------------------ #
    # Context manager
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "App":
        self._entered = True
        if self._transport is None:
            # Embed mode: spawn the subprocess now.
            self._transport = parse_url_to_transport(None)
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def __del__(self) -> None:
        # Safety net: close transport if the App is garbage-collected without an
        # explicit close().  __del__ is not guaranteed to run (e.g. circular refs,
        # interpreter shutdown), but it prevents common forget-to-close bugs from
        # leaking sockets/connections indefinitely.  Uses _closed flag to ensure
        # at-most-once cleanup and swallows all exceptions (GC must not raise).
        if not self._closed and self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._closed = True

    def close(self) -> None:
        """Close the underlying transport (idempotent)."""
        if self._closed:
            return
        if self._transport is not None:
            self._transport.close()
        self._closed = True

    def _require_transport(self) -> Transport:
        """Return the live transport or raise an appropriate error.

        Raises:
            RuntimeError: If closed or embed mode without entering context manager.
        """
        if self._closed:
            raise RuntimeError("bv.App instance is already closed")
        if self._transport is None:
            raise RuntimeError(
                "bv.App() embed mode requires 'with bv.App() as app:' pattern; "
                "use the context manager so the subprocess lifecycle is managed correctly"
            )
        return self._transport

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def validate(self, *descriptors: Any) -> list[ValidationError]:
        """Run local DAG/schema checks without any network I/O.

        Performs all Phase 3 local validation rules:
          - Duplicate names within the batch
          - Missing upstream references
          - Cycle detection (DFS three-color)
          - Schema field type checks
          - event_time_field validity
          - Table primary_key validity
          - Bad return type

        Args:
            *descriptors: Descriptor objects returned by ``@bv.event`` / ``@bv.table``.

        Returns:
            ``list[ValidationError]`` — empty list means the batch is valid.
        """
        return validate_descriptors(list(descriptors))

    def ping(self) -> dict[str, Any]:
        """Ping the server and return its version info.

        Delegates to ``transport.send_ping()``.

        Returns:
            dict with ``server_version`` (str) and ``registry_version`` (int).

        Raises:
            NotImplementedError: If the transport is HTTP (ping is TCP-only in v0).
            RuntimeError: If called on a closed App or in embed mode without entering.
        """
        transport = self._require_transport()
        result: dict[str, Any] = transport.send_ping()
        return result

    def register(self, *descriptors: Any) -> dict[str, Any]:
        """Validate locally, topo-sort, and dispatch one REGISTER call.

        Pipeline:
          1. Run ``validate_descriptors(descriptors)`` — zero network I/O.
          2. If any errors: raise ``RegistrationError`` (first error as the
             exception's code/path/message; all errors on ``.errors``).
             **No wire I/O happens.**
          3. Topo-sort the descriptors (upstreams before dependents).
          4. Compile the REGISTER JSON payload
             (``{"nodes": [desc._to_register_json() for desc in sorted_descs]}``).
          5. Dispatch via ``transport.send_register(payload_bytes)``.
          6. Return the server's response dict (contains ``registry_version``,
             ``status``, ``added``, ``already_present``, etc.).

        Args:
            *descriptors: Descriptor objects returned by ``@bv.event`` / ``@bv.table``.

        Returns:
            Server response dict (e.g. ``{"registry_version": 1, "status": "ok", ...}``).

        Raises:
            RegistrationError: Local validation failed (code from first error, `.errors`
                                contains all ValidationError entries) OR server returned
                                a 4xx/5xx error.
            RuntimeError: Called on a closed App or embed mode without context manager.
        """
        transport = self._require_transport()

        # Step 1: local validation (zero network I/O)
        errs = validate_descriptors(list(descriptors))
        if errs:
            first = errs[0]
            raise RegistrationError(
                code=first.kind,
                path=first.path,
                message=first.message,
                errors=errs,
            )

        # Step 2: topo-sort (raises ValidationError on cycle — caught above already,
        # but topo_sort is the authoritative sort after validation passes)
        sorted_descs = topo_sort(list(descriptors))

        # Step 3: compile payload
        nodes = [d._to_register_json() for d in sorted_descs]
        payload_bytes = json.dumps({"nodes": nodes}, ensure_ascii=False).encode("utf-8")

        # Step 4: dispatch
        result: dict[str, Any] = transport.send_register(payload_bytes)
        return result

    def upsert(self, table_type: Any, row_dict: dict[str, Any]) -> dict[str, Any]:
        """POST /upsert/{table_name} — write a row to a source table.

        Args:
            table_type: Descriptor returned by ``@bv.table``.
            row_dict: Row fields as a plain dict.

        Returns:
            Server response dict (e.g. ``{"ack_lsn": 42, "registry_version": 1}``).

        Raises:
            RuntimeError: Called on a closed App or embed mode without context manager.
        """
        transport = self._require_transport()
        table_name = table_type._name
        payload_bytes = json.dumps(row_dict, ensure_ascii=False).encode("utf-8")
        return transport._client.post(
            f"/upsert/{table_name}",
            content=payload_bytes,
            headers={"Content-Type": "application/json"},
        ).json()

    def delete(self, table_type: Any, *, key: Any) -> dict[str, Any]:
        """POST /delete/{table_name} — tombstone a row by primary key.

        Args:
            table_type: Descriptor returned by ``@bv.table``.
            key: Primary key value to delete.

        Returns:
            Server response dict.

        Raises:
            RuntimeError: Called on a closed App or embed mode without context manager.
        """
        transport = self._require_transport()
        table_name = table_type._name
        payload_bytes = json.dumps({"key": key}, ensure_ascii=False).encode("utf-8")
        return transport._client.post(
            f"/delete/{table_name}",
            content=payload_bytes,
            headers={"Content-Type": "application/json"},
        ).json()

    def get(self, feature: str, key: str) -> Any:
        """Plan 12-09: read one feature for one entity key.

        Dispatches based on the underlying transport:
          - ``tcp://`` (or embed mode, which wraps TCP): uses ``OP_GET`` over
            the binary-framed TCP wire with **msgpack body+response by default**
            (locked decision D-A/D-B; the server now supports CT_MSGPACK on
            the read path).
          - ``http://`` / ``https://``: uses ``GET /get/{feature}/{key}`` with
            JSON only (locked decision D-D — HTTP /get is JSON-only).

        Args:
            feature: Feature name (e.g. ``"cnt"``).
            key: Entity key value (e.g. ``"alice"``).

        Returns:
            The unwrapped feature value (the contents of the server response's
            ``"value"`` field). Returns ``None`` if the server returned a
            QueryNotFound shape — the transport may surface that as the
            literal value ``None`` (caller should disambiguate via business
            logic; v0 doesn't separate "no key" from "value is null").

        Raises:
            RuntimeError: Called on a closed App or embed mode without context manager.
        """
        transport = self._require_transport()
        # TCP / embed path: msgpack default per locked decision D-A/D-B.
        if hasattr(transport, "tcp_get_single"):
            return transport.tcp_get_single(feature, key)
        # HTTP path: JSON-only per locked decision D-D.
        if hasattr(transport, "http_get_single"):
            return transport.http_get_single(feature, key)
        raise RuntimeError(
            "transport does not support .get() — expected TcpTransport / "
            "HttpTransport / EmbedTransport"
        )
