"""Transport layer for the Beava Python SDK.

Provides:
  - :class:`Transport` protocol (abstract interface)
  - :class:`HttpTransport` — ``httpx.Client`` over HTTP/HTTPS
  - :class:`TcpTransport` — stdlib ``socket`` with the Phase 2.5 frame codec
  - :class:`EmbedTransport` — wraps TcpTransport + a subprocess handle
  - :func:`parse_url_to_transport` — URL scheme dispatch + embed-mode entry point

URL dispatch:
  ``http://...`` or ``https://...``  → :class:`HttpTransport`
  ``tcp://...``                       → :class:`TcpTransport`
  ``None``                            → :class:`EmbedTransport` (spawn subprocess)
  anything else                       → :exc:`ValueError`
"""

from __future__ import annotations

import json
import socket
import subprocess
import urllib.parse
from typing import TYPE_CHECKING, Any

import httpx

from beava._errors import RegistrationError
from beava._wire import (
    CT_JSON,
    CT_MSGPACK,
    MAX_FRAME_BYTES,
    OP_BATCH_GET,
    OP_GET,
    OP_GET_RESPONSE,
    OP_PING,
    OP_PUSH,
    OP_REGISTER,
    OP_RESET,
    encode_frame,
    parse_register_response,
    read_frame,
)

if TYPE_CHECKING:
    from types import TracebackType


# ─── Transport protocol (structural typing) ──────────────────────────────────


class Transport:
    """Abstract transport interface (duck-typed — use Protocol for strict checks).

    Both :class:`HttpTransport` and :class:`TcpTransport` implement:
      - ``send_register(payload_json: bytes) -> dict``
      - ``send_ping() -> dict``
      - ``close() -> None``
      - context manager (``__enter__`` / ``__exit__``)

    Phase 13.5 Plan 11: extended with the App-facing methods (``send_push``,
    ``send_get``, ``send_batch_get``, ``send_reset``) so ``App`` can call them
    without ``# type: ignore[attr-defined]``. The base class raises
    ``NotImplementedError`` for backends that don't yet wire the underlying
    op; subclasses override. The actual wire payload is constructed by the
    transport (it owns the wire-format choice — JSON for HTTP, msgpack for
    TCP/Embed).
    """

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
        raise NotImplementedError

    def send_push(
        self,
        *,
        event_name: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def send_get(
        self,
        *,
        table: str,
        key: str | list[Any],
        features: list[str] | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def send_batch_get(
        self,
        *,
        requests: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ],
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def send_reset(self) -> None:
        raise NotImplementedError

    def send_ping(self) -> dict:  # type: ignore[type-arg]
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> "Transport":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        self.close()


# ─── HTTP transport ──────────────────────────────────────────────────────────


class HttpTransport(Transport):
    """Send register requests via HTTP/JSON using ``httpx.Client``.

    The client is long-lived; reuse across multiple ``send_register`` calls
    on the same App instance is safe (httpx connection-pools under the hood).

    Args:
        base_url: Server base URL, e.g. ``"http://localhost:7379"``.
        timeout: Request timeout in seconds.
    """

    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
        """POST /register with JSON payload.

        Args:
            payload_json: UTF-8 JSON bytes matching the Phase 2 wire contract.

        Returns:
            Parsed success body dict (``status='ok'``, ``registry_version=N``, …).

        Raises:
            RegistrationError: Server returned 4xx or 5xx with a JSON error body.
        """
        r = self._client.post(
            "/register",
            content=payload_json,
            headers={"Content-Type": "application/json"},
        )
        body = r.json()
        if r.status_code == 200:
            return body  # type: ignore[no-any-return]
        error = body.get("error", {})
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    def send_push(
        self,
        *,
        event_name: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """POST /push with verb-style body ``{event, data}``.

        Targets the post-Phase-13.4 verb-style route — NOT the legacy
        path-encoded ``/push/{event_name}`` form. Body shape matches the
        live server parser at
        ``crates/beava-runtime-core/src/http_listener.rs::parse_verb_push``
        which expects ``{"event": "<name>", "data": {<fields>}}``. The
        docs at ``docs/http-api.md:176`` say ``{event_name, fields}`` but
        the impl never migrated; SDK matches the impl (a docs erratum will
        be filed in v0.0.x — Phase 13.4 missed-rename).

        Returns:
            Parsed success body dict (``{"ack_lsn": int, ...}``).

        Raises:
            RegistrationError: Server returned non-2xx with a JSON error body.
        """
        body_bytes = json.dumps(
            {"event": event_name, "data": fields}, ensure_ascii=False
        ).encode("utf-8")
        r = self._client.post(
            "/push",
            content=body_bytes,
            headers={"Content-Type": "application/json"},
        )
        body = r.json()
        if r.status_code == 200:
            return body  # type: ignore[no-any-return]
        error = body.get("error", {})
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    def send_get(
        self,
        *,
        table: str,
        key: str | list[Any],
        features: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /get with verb-style body ``{table, key, features?}``.

        Per Phase 13.4.1 wire-spec lockdown, the response is a row-shape
        flat dict (cold-start = ``{}``). Features filter (D-03 USER-LOCKED)
        is included when non-None and omitted when None (full row).

        Returns:
            Parsed row-shape flat dict (``{}`` on cold-start).

        Raises:
            RegistrationError: Server returned non-2xx with a JSON error body.
        """
        payload: dict[str, Any] = {"table": table, "key": key}
        if features is not None:
            payload["features"] = features
        body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        r = self._client.post(
            "/get",
            content=body_bytes,
            headers={"Content-Type": "application/json"},
        )
        body = r.json()
        if r.status_code == 200:
            return body  # type: ignore[no-any-return]
        error = body.get("error", {})
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    def send_batch_get(
        self,
        *,
        requests: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ],
    ) -> list[dict[str, Any]]:
        """POST /batch_get with verb-style body ``{requests:[{table,key,features?}, ...]}``.

        Phase 13.4.1 returns ``body["results"]`` as a list of FLAT row dicts
        (no wrapping envelope), so this method returns ``body["results"]``
        verbatim. Per-entry ``features`` filter is supported via the
        3-tuple form (D-03 USER-LOCKED).

        Args:
            requests: list of per-entry tuples — either ``(table, key)`` or
                ``(table, key, features)``. ``features`` may be None to
                request the full row.

        Returns:
            list of flat row dicts in request order.

        Raises:
            RegistrationError: Server returned non-2xx with a JSON error body.
            TypeError: A request entry is neither a 2-tuple nor a 3-tuple.
        """
        wire_requests: list[dict[str, Any]] = []
        for entry in requests:
            if len(entry) == 2:
                tbl, k = entry
                wire_requests.append({"table": tbl, "key": k})
            elif len(entry) == 3:
                tbl, k, feats = entry
                wire_entry: dict[str, Any] = {"table": tbl, "key": k}
                if feats is not None:
                    wire_entry["features"] = feats
                wire_requests.append(wire_entry)
            else:
                raise TypeError(
                    f"batch_get request entry must be a 2- or 3-tuple "
                    f"(table, key) or (table, key, features); "
                    f"got {len(entry)}-tuple"
                )
        body_bytes = json.dumps(
            {"requests": wire_requests}, ensure_ascii=False
        ).encode("utf-8")
        r = self._client.post(
            "/batch_get",
            content=body_bytes,
            headers={"Content-Type": "application/json"},
        )
        body = r.json()
        if r.status_code == 200:
            results: list[dict[str, Any]] = body.get("results", [])
            return results
        error = body.get("error", {})
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    def send_reset(self) -> None:
        """POST /reset (test_mode-gated per Phase 13.4 D-03).

        On non-test-mode servers the engine returns 403 ``reset_disabled``;
        this method surfaces that as ``RegistrationError(code="reset_disabled")``.

        Raises:
            RegistrationError: Server returned non-200; in particular,
                ``code="reset_disabled"`` if the server is not in test_mode.
        """
        r = self._client.post(
            "/reset",
            content=b"{}",
            headers={"Content-Type": "application/json"},
        )
        if r.status_code == 200:
            return
        try:
            body = r.json()
            error = body.get("error", {})
        except Exception:
            error = {"code": "unparseable_error", "message": r.text[:200]}
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    def send_ping(self) -> dict:  # type: ignore[type-arg]
        """Not implemented for HTTP transport.

        HTTP has no /ping endpoint in v0.  Use ``tcp://`` transport for ping.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "HTTP transport has no /ping endpoint in v0; "
            "use tcp:// transport for ping"
        )

    def _http_get_single(self, feature: str, key: str) -> object:
        """Plan 12-09: GET /get/{feature}/{key} → returns the unwrapped value.

        HTTP /get is JSON-only per locked decision D-D — regardless of whether
        the TCP transport defaults to msgpack on the read path, the HTTP path
        always speaks JSON.

        Args:
            feature: Feature name (e.g. "cnt").
            key: Entity key value (e.g. "alice"; URL-encoded if needed).

        Returns:
            The unwrapped feature value (i.e. the contents of the response's
            ``"value"`` field). Raises if the server returned a non-2xx.
        """
        r = self._client.get(f"/get/{feature}/{key}")
        r.raise_for_status()
        body = r.json()
        return body.get("value")

    def close(self) -> None:
        """Close the underlying httpx client connection pool."""
        self._client.close()

    def __enter__(self) -> "HttpTransport":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        self.close()


# ─── TCP transport ───────────────────────────────────────────────────────────


class TcpTransport(Transport):
    """Send register/ping requests over the Phase 2.5 binary-framed TCP protocol.

    Connection is lazy — opened on first use, reused for the lifetime of the
    transport.  Strict-FIFO: one in-flight request per connection (v0).

    Args:
        host: Server hostname or IP address.
        port: Server TCP port.
        max_frame_bytes: Maximum frame size (must match server config; default 4 MiB).
        timeout: Socket connect/recv timeout in seconds.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.max_frame_bytes = max_frame_bytes
        self._timeout = timeout
        self._socket: socket.socket | None = None

    def _ensure_connected(self) -> socket.socket:
        """Return the existing socket or open a new connection."""
        if self._socket is None:
            self._socket = socket.create_connection(
                (self.host, self.port), timeout=self._timeout
            )
        return self._socket

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
        """Send an OP_REGISTER frame and return the parsed response dict.

        Args:
            payload_json: UTF-8 JSON bytes matching the Phase 2.5 wire contract.

        Returns:
            Parsed success body dict.

        Raises:
            RegistrationError: Server responded with OP_ERROR_RESPONSE.
        """
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_REGISTER, CT_JSON, payload_json))
        frame = read_frame(sock, self.max_frame_bytes)
        return parse_register_response(frame)

    def send_push(
        self,
        *,
        event_name: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Send an OP_PUSH frame with the given event name and body fields.

        Encodes the envelope ``{"event": event_name, "body": fields}`` as JSON
        and sends it as an OP_PUSH frame. Default JSON encoding for v0
        compatibility with the server's CT_JSON path.

        Returns:
            Parsed JSON ACK dict from the server (e.g. ``{"ack_lsn": 42}``).
        """
        envelope = json.dumps(
            {"event": event_name, "body": fields}, ensure_ascii=False
        ).encode("utf-8")
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_PUSH, CT_JSON, envelope))
        frame = read_frame(sock, self.max_frame_bytes)
        result: dict[str, Any] = json.loads(frame.payload.decode("utf-8"))
        return result

    def send_get(
        self,
        *,
        table: str,
        key: str | list[Any],
        features: list[str] | None = None,
    ) -> dict[str, Any]:
        """Send OP_GET (0x0020) frame; expect OP_GET_RESPONSE (0x0023).

        D-02 USER-LOCKED: JSON default on the read path (matches send_push).
        D-03 USER-LOCKED: ``features`` is included in the body when non-None
        and omitted when None (full row).

        Wire body (verb-style, Phase 13.4.1 contract):
            ``{"table": ..., "key": ..., "features"?: [...]}``

        Returns:
            Parsed row-shape flat dict (cold-start = ``{}``).

        Raises:
            RegistrationError: Server returned OP_ERROR_RESPONSE or an
                unexpected response opcode.
        """
        payload: dict[str, Any] = {"table": table, "key": key}
        if features is not None:
            payload["features"] = features
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_GET, CT_JSON, body))
        frame = read_frame(sock, self.max_frame_bytes)
        if frame.op != OP_GET_RESPONSE:
            try:
                err_body = json.loads(frame.payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                err_body = {"error": {"code": "unparseable_error"}}
            raise RegistrationError(
                code=err_body.get("error", {}).get("code", "unexpected_frame"),
                message=(
                    f"expected OP_GET_RESPONSE (0x0023), "
                    f"got op={frame.op:#06x} ct={frame.ct:#04x} body={err_body!r}"
                ),
            )
        result: dict[str, Any] = json.loads(frame.payload.decode("utf-8"))
        return result

    def send_batch_get(
        self,
        *,
        requests: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ],
    ) -> list[dict[str, Any]]:
        """Send OP_BATCH_GET (0x0024) frame; expect OP_GET_RESPONSE (0x0023).

        D-02 USER-LOCKED: JSON default. D-03 USER-LOCKED: per-entry features
        filter via 3-tuple form is supported and included on the wire when
        non-None.

        Wire body (verb-style, Phase 13.4.1 contract):
            ``{"requests": [{"table", "key", "features"?}, ...]}``
        Wire response: ``{"results": [<flat row>, ...]}`` — flat-row contract
        per Phase 13.4.1; this method returns ``body["results"]`` verbatim.

        Returns:
            list of flat row dicts in request order.

        Raises:
            RegistrationError: Server returned OP_ERROR_RESPONSE or an
                unexpected response shape.
            TypeError: A request entry is neither a 2-tuple nor a 3-tuple.
        """
        wire_requests: list[dict[str, Any]] = []
        for entry in requests:
            if len(entry) == 2:
                tbl, k = entry
                wire_requests.append({"table": tbl, "key": k})
            elif len(entry) == 3:
                tbl, k, feats = entry
                wire_entry: dict[str, Any] = {"table": tbl, "key": k}
                if feats is not None:
                    wire_entry["features"] = feats
                wire_requests.append(wire_entry)
            else:
                raise TypeError(
                    f"batch_get request entry must be a 2- or 3-tuple "
                    f"(table, key) or (table, key, features); "
                    f"got {len(entry)}-tuple"
                )
        body = json.dumps(
            {"requests": wire_requests}, ensure_ascii=False
        ).encode("utf-8")
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_BATCH_GET, CT_JSON, body))
        frame = read_frame(sock, self.max_frame_bytes)
        if frame.op != OP_GET_RESPONSE:
            try:
                err_body = json.loads(frame.payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                err_body = {"error": {"code": "unparseable_error"}}
            raise RegistrationError(
                code=err_body.get("error", {}).get("code", "unexpected_frame"),
                message=(
                    f"expected OP_GET_RESPONSE (0x0023) for OP_BATCH_GET, "
                    f"got op={frame.op:#06x} ct={frame.ct:#04x} body={err_body!r}"
                ),
            )
        decoded = json.loads(frame.payload.decode("utf-8"))
        if not isinstance(decoded, dict) or "results" not in decoded:
            raise RegistrationError(
                code="unexpected_frame",
                message=f"expected dict with 'results' key, got {decoded!r}",
            )
        results: list[dict[str, Any]] = decoded["results"]
        return results

    def send_reset(self) -> None:
        """Send OP_RESET (0x0040) frame; gated on server-side test_mode (Phase 13.4 D-03).

        Successful reset: server returns OP_GET_RESPONSE (0x0023) — the
        generic JSON success frame — with body
        ``{"reset": true, "registry_version": N}`` (per
        ``crates/beava-server/src/server.rs:2338-2344``). The plan-document /
        PATTERNS.md initially predicted OP_RESET-on-success but the actual
        server uses OP_GET_RESPONSE; SDK matches the live contract.

        Disabled (non-test_mode): server returns OP_ERROR_RESPONSE with
        ``code="reset_disabled_in_production"``.

        Raises:
            RegistrationError: OP_RESET denied (non-test_mode) or
                unexpected response opcode.
        """
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_RESET, CT_JSON, b"{}"))
        frame = read_frame(sock, self.max_frame_bytes)
        if frame.op == OP_GET_RESPONSE:
            # Success path — body is `{"reset": true, "registry_version": N}`.
            return
        try:
            err_body = json.loads(frame.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            err_body = {"error": {"code": "unparseable_error"}}
        raise RegistrationError(
            code=err_body.get("error", {}).get("code", "unexpected_frame"),
            message=(
                f"OP_RESET denied or unexpected response: "
                f"op={frame.op:#06x} body={err_body!r}"
            ),
        )

    def send_ping(self) -> dict:  # type: ignore[type-arg]
        """Send an OP_PING frame and return the parsed response dict.

        Returns:
            Dict with ``server_version`` and ``registry_version`` keys.

        Raises:
            RegistrationError: Unexpected response opcode.
        """
        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_PING, CT_JSON, b"{}"))
        frame = read_frame(sock, self.max_frame_bytes)
        if frame.op != OP_PING:
            raise RegistrationError(
                code="unexpected_frame",
                message=f"expected OP_PING (0x0000), got op={frame.op:#06x}",
            )
        result: dict[str, object] = json.loads(frame.payload.decode("utf-8"))
        return result

    def _tcp_get_single(
        self,
        feature: str,
        key: str,
        *,
        wire_format: str = "msgpack",
    ) -> object:
        """Plan 12-09: send OP_GET frame and return the unwrapped feature value.

        Defaults to **msgpack** on the wire (the production read fast-path per
        locked decision D-A/D-B). Pass ``wire_format="json"`` to force the
        legacy CT_JSON path (regression coverage / debugging).

        Args:
            feature: Feature name (e.g. "cnt").
            key: Entity key value (e.g. "alice").
            wire_format: ``"msgpack"`` (default) or ``"json"``.

        Returns:
            The unwrapped feature value (``response["value"]``).

        Raises:
            ValueError: ``wire_format`` is not ``"msgpack"`` or ``"json"``.
            ImportError: ``wire_format="msgpack"`` but ``msgpack`` not installed.
            RegistrationError: Server returned OP_ERROR_RESPONSE or unexpected op.
        """
        body_dict = {"feature": feature, "key": key}
        if wire_format == "msgpack":
            try:
                import msgpack  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "wire_format='msgpack' requires the 'msgpack' package: "
                    "pip install msgpack"
                ) from exc
            body = msgpack.packb(body_dict, use_bin_type=True)
            ct = CT_MSGPACK
        elif wire_format == "json":
            body = json.dumps(body_dict, ensure_ascii=False).encode("utf-8")
            ct = CT_JSON
        else:
            raise ValueError(
                f"wire_format must be 'msgpack' or 'json', got {wire_format!r}"
            )

        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_GET, ct, body))
        frame = read_frame(sock, self.max_frame_bytes)
        if frame.op != OP_GET_RESPONSE:
            # Could be OP_ERROR_RESPONSE (0xFFFF) — surface server's reason.
            try:
                err_body = json.loads(frame.payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                err_body = {"error": {"code": "unparseable_error"}}
            raise RegistrationError(
                code=err_body.get("error", {}).get("code", "unexpected_frame"),
                message=(
                    f"expected OP_GET_RESPONSE (0x0023), "
                    f"got op={frame.op:#06x} ct={frame.ct:#04x} body={err_body!r}"
                ),
            )

        # Decode response per its content_type byte (server uses same-format-as-
        # request; if we sent msgpack, response is msgpack).
        if frame.ct == CT_MSGPACK:
            try:
                import msgpack
            except ImportError as exc:
                raise ImportError(
                    "received CT_MSGPACK response but 'msgpack' package not "
                    "installed: pip install msgpack"
                ) from exc
            decoded = msgpack.unpackb(frame.payload, raw=False)
        else:
            decoded = json.loads(frame.payload.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise RegistrationError(
                code="unexpected_frame",
                message=f"expected dict response body, got {type(decoded).__name__}",
            )
        return decoded.get("value")

    def close(self) -> None:
        """Close the underlying socket if open."""
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

    def __enter__(self) -> "TcpTransport":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        self.close()


# ─── Embed transport ─────────────────────────────────────────────────────────


class EmbedTransport(Transport):
    """Wraps a :class:`TcpTransport` and a subprocess handle for embed mode.

    Created by :func:`parse_url_to_transport` when ``url=None``.
    ``close()`` terminates the subprocess after closing the socket.

    Args:
        tcp: The TcpTransport connected to the embedded server.
        proc: The subprocess.Popen handle for the embedded server.
    """

    def __init__(
        self,
        tcp: TcpTransport,
        proc: "subprocess.Popen[bytes]",
        *,
        spawn_env: dict[str, str] | None = None,
    ) -> None:
        self._tcp = tcp
        self._proc = proc
        # Phase 13.5 Plan 02 D-05: expose the env dict that was passed to the
        # spawned binary so tests can assert BEAVA_TEST_MODE=1 propagation.
        self._spawn_env: dict[str, str] = spawn_env or {}

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
        return self._tcp.send_register(payload_json)

    def send_push(
        self,
        *,
        event_name: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Delegate to the embedded TcpTransport."""
        return self._tcp.send_push(event_name=event_name, fields=fields)

    def send_get(
        self,
        *,
        table: str,
        key: str | list[Any],
        features: list[str] | None = None,
    ) -> dict[str, Any]:
        """Delegate to the embedded TcpTransport (D-03 features filter)."""
        return self._tcp.send_get(table=table, key=key, features=features)

    def send_batch_get(
        self,
        *,
        requests: list[
            tuple[str, str | list[Any]]
            | tuple[str, str | list[Any], list[str] | None]
        ],
    ) -> list[dict[str, Any]]:
        """Delegate to the embedded TcpTransport (D-03 per-entry features filter)."""
        return self._tcp.send_batch_get(requests=requests)

    def send_reset(self) -> None:
        """Delegate to the embedded TcpTransport (test_mode-gated)."""
        self._tcp.send_reset()

    def send_ping(self) -> dict:  # type: ignore[type-arg]
        return self._tcp.send_ping()

    def _tcp_get_single(
        self,
        feature: str,
        key: str,
        *,
        wire_format: str = "msgpack",
    ) -> object:
        """Delegate to the embedded TcpTransport (Plan 12-09; private per D-04)."""
        return self._tcp._tcp_get_single(feature, key, wire_format=wire_format)

    def close(self) -> None:
        """Close the TCP socket then terminate the embedded server process."""
        self._tcp.close()
        from beava._embed import teardown_process

        teardown_process(self._proc)

    def __enter__(self) -> "EmbedTransport":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        self.close()


# ─── URL dispatch ────────────────────────────────────────────────────────────


def parse_url_to_transport(url: str | None) -> Transport:
    """Return the appropriate transport for the given URL or None (embed mode).

    Args:
        url: One of:
            - ``"http://..."`` or ``"https://..."`` → :class:`HttpTransport`
            - ``"tcp://host:port"`` → :class:`TcpTransport`
            - ``None`` → embed mode: spawn a local beava binary and return
              an :class:`EmbedTransport` connected to it over TCP.

    Returns:
        A concrete :class:`Transport` instance.

    Raises:
        ValueError: URL scheme is not ``http``, ``https``, ``tcp``, or ``None``.
        BinaryNotFoundError: Embed mode but binary cannot be located.
    """
    if url is None:
        # Embed mode: spawn local binary, connect via TCP.
        from beava._embed import spawn_embedded_server

        proc, _http_url, tcp_url, env = spawn_embedded_server()
        parsed = urllib.parse.urlparse(tcp_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7380
        tcp = TcpTransport(host=host, port=port)
        return EmbedTransport(tcp=tcp, proc=proc, spawn_env=env)

    if url.startswith("http://") or url.startswith("https://"):
        return HttpTransport(url)

    if url.startswith("tcp://"):
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7380
        return TcpTransport(host=host, port=port)

    raise ValueError(
        f"unsupported URL scheme in {url!r}; "
        f"supported schemes: http://, https://, tcp://, or None for embed mode"
    )


# ─── Phase 13.5 Plan 02: make_transport factory ─────────────────────────────


def make_transport(
    url: str | None = None,
    *,
    timeout: float = 30.0,
    test_mode: bool = False,
) -> Transport:
    """Phase 13.5 Plan 02 factory — URL-scheme dispatch + test_mode propagation.

    Routes ``url=None`` → embed-mode subprocess spawn (with optional
    BEAVA_TEST_MODE=1 env var per D-05), ``http(s)://`` → HttpTransport,
    ``tcp://`` → TcpTransport.

    Args:
        url: Server URL or None for embed mode.
        timeout: HTTP / socket timeout in seconds.
        test_mode: When True + url=None, spawns the binary with
            BEAVA_TEST_MODE=1. Has no effect in network mode (caller is
            expected to emit a UserWarning before calling).

    Returns:
        A concrete :class:`Transport` instance.
    """
    if url is None:
        from beava._embed import spawn_embedded_server

        proc, _http_url, tcp_url, env = spawn_embedded_server(test_mode=test_mode)
        parsed = urllib.parse.urlparse(tcp_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7380
        tcp = TcpTransport(host=host, port=port, timeout=timeout)
        return EmbedTransport(tcp=tcp, proc=proc, spawn_env=env)

    if url.startswith("http://") or url.startswith("https://"):
        return HttpTransport(base_url=url, timeout=timeout)

    if url.startswith("tcp://"):
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7380
        return TcpTransport(host=host, port=port, timeout=timeout)

    raise ValueError(
        f"unsupported URL scheme in {url!r}; "
        f"supported schemes: http://, https://, tcp://, or None for embed mode"
    )
