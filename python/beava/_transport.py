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
from typing import TYPE_CHECKING

import httpx

from beava._errors import RegistrationError
from beava._wire import (
    CT_JSON,
    CT_MSGPACK,
    MAX_FRAME_BYTES,
    OP_PING,
    OP_PUSH,
    OP_REGISTER,
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
    """

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
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
        event_name: str,
        body_dict: dict,  # type: ignore[type-arg]
        *,
        wire_format: str = "json",
    ) -> dict:  # type: ignore[type-arg]
        """Send an OP_PUSH frame with the given event name and body.

        Encodes the envelope ``{"event": event_name, "body": body_dict}``
        using the requested wire format and sends it as an OP_PUSH frame.

        Args:
            event_name: Name of the registered event type.
            body_dict: Event fields as a plain Python dict.
            wire_format: ``"json"`` (default) or ``"msgpack"``.
                         ``"json"`` uses stdlib :mod:`json` + CT_JSON.
                         ``"msgpack"`` requires the ``msgpack`` package + CT_MSGPACK.

        Returns:
            Parsed JSON ACK dict from the server (e.g. ``{"ack_lsn": 42}``).

        Raises:
            ValueError: ``wire_format`` is not ``"json"`` or ``"msgpack"``.
            ImportError: ``wire_format="msgpack"`` but ``msgpack`` is not installed.
        """
        if wire_format == "json":
            envelope = json.dumps(
                {"event": event_name, "body": body_dict}, ensure_ascii=False
            ).encode("utf-8")
            ct = CT_JSON
        elif wire_format == "msgpack":
            try:
                import msgpack  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "wire_format='msgpack' requires the 'msgpack' package: "
                    "pip install msgpack"
                ) from exc
            envelope = msgpack.packb(
                {"event": event_name, "body": body_dict}, use_bin_type=True
            )
            ct = CT_MSGPACK
        else:
            raise ValueError(
                f"wire_format must be 'json' or 'msgpack', got {wire_format!r}"
            )

        sock = self._ensure_connected()
        sock.sendall(encode_frame(OP_PUSH, ct, envelope))
        frame = read_frame(sock, self.max_frame_bytes)
        # Server ACK is JSON regardless of push wire format.
        result: dict[str, object] = json.loads(frame.payload.decode("utf-8"))
        return result

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
    ) -> None:
        self._tcp = tcp
        self._proc = proc

    def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
        return self._tcp.send_register(payload_json)

    def send_ping(self) -> dict:  # type: ignore[type-arg]
        return self._tcp.send_ping()

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

        proc, _http_url, tcp_url = spawn_embedded_server()
        parsed = urllib.parse.urlparse(tcp_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 7380
        tcp = TcpTransport(host=host, port=port)
        return EmbedTransport(tcp=tcp, proc=proc)

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
