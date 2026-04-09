"""TCP client for communicating with the Tally server.

Manages a persistent TCP connection with lazy connect, auto-reconnect on
server disconnect, frame-level send/receive, and configurable timeout.

Usage::

    client = TallyClient("127.0.0.1", 6400, timeout=5.0)
    status, payload = client.send_command(OP_PUSH, encoded_payload)
    client.close()

Or as a context manager::

    with TallyClient("127.0.0.1", 6400) as client:
        status, payload = client.send_command(OP_PUSH, encoded_payload)
"""

from __future__ import annotations

import socket
import struct

from tally._protocol import encode_frame, MAX_FRAME_SIZE
from tally._types import ConnectionError, ProtocolError


class TallyClient:
    """Low-level TCP client for the Tally binary protocol.

    Connects lazily on first ``send_command`` call. Auto-reconnects
    transparently if the server closes the connection.

    Args:
        host: Server hostname or IP address.
        port: Server TCP port.
        timeout: Socket timeout in seconds for both connect and read (default 5.0).
    """

    def __init__(self, host: str, port: int, *, timeout: float = 5.0) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: socket.socket | None = None

    def _connect(self) -> None:
        """Open a new TCP connection to the server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._timeout)
        try:
            sock.connect((self._host, self._port))
        except OSError as exc:
            sock.close()
            raise ConnectionError(
                f"failed to connect to {self._host}:{self._port}: {exc}"
            ) from exc
        self._sock = sock

    def _ensure_connected(self) -> None:
        """Connect if not already connected."""
        if self._sock is None:
            self._connect()

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly *n* bytes from the socket.

        Raises ``ConnectionError`` if the server closes the connection
        before *n* bytes have been received.
        """
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                self._sock = None
                raise ConnectionError("server closed connection")
            buf.extend(chunk)
        return bytes(buf)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        """Send one wire frame: [4-byte BE length][opcode][payload]."""
        frame = encode_frame(opcode, payload)
        self._sock.sendall(frame)

    def _recv_frame(self) -> tuple[int, bytes]:
        """Read one response frame: [4-byte BE length][status][payload].

        Validates that the frame length does not exceed ``MAX_FRAME_SIZE``.
        Returns ``(status, payload)``.
        """
        header = self._recv_exact(4)
        length = struct.unpack(">I", header)[0]

        if length == 0:
            raise ProtocolError("response frame length is zero")
        if length > MAX_FRAME_SIZE:
            raise ProtocolError(f"response frame too large: {length} bytes")

        body = self._recv_exact(length)
        status = body[0]
        payload = body[1:]
        return status, payload

    def send_command(self, opcode: int, payload: bytes) -> tuple[int, bytes]:
        """Send a command and return the response ``(status, payload)``.

        Connects lazily on first call. If the connection is broken,
        auto-reconnects once and retries the send transparently.
        """
        self._ensure_connected()
        try:
            self._send_frame(opcode, payload)
            return self._recv_frame()
        except ConnectionError:
            # Connection dropped -- reconnect and retry once.
            self._sock = None
            self._connect()
            self._send_frame(opcode, payload)
            return self._recv_frame()

    def close(self) -> None:
        """Close the TCP connection (if open)."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def __enter__(self) -> TallyClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
