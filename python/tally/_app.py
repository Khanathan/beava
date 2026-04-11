"""High-level App class that wires together the TCP client, protocol encoding,
and DSL layer into the user-facing API.

Usage::

    import tally as st

    app = st.App("localhost:6400")
    app.register(Transactions)
    features = app.push(Transactions, {"user_id": "u1", "amount": 50.0})
    print(features.tx_count_1h)
"""

from __future__ import annotations

import json

from tally._client import TallyClient
from tally._protocol import (
    OP_FLUSH,
    OP_GET,
    OP_MGET,
    OP_MSET,
    OP_PUSH,
    OP_PUSH_ASYNC,
    OP_REGISTER,
    OP_SET,
    STATUS_ERROR,
    encode_get,
    encode_mget,
    encode_mset,
    encode_push_binary,
    encode_register,
    encode_set,
)
from tally._types import FeatureResult, ProtocolError


class App:
    """Tally application client.

    Connects to a running Tally server and exposes ``register``, ``push``,
    ``get``, ``set``, and ``mset`` methods for pipeline management and
    feature operations.

    Args:
        address: Server address as ``"host:port"`` or ``"host"`` (default port 6400).
        timeout: Socket timeout in seconds (default 5.0).
    """

    def __init__(self, address: str, *, timeout: float = 5.0) -> None:
        host, port = self._parse_address(address)
        self._client = TallyClient(host, port, timeout=timeout)

    @staticmethod
    def _parse_address(address: str) -> tuple[str, int]:
        """Parse ``"host:port"`` into ``(host, port)``; default port is 6400."""
        if ":" in address:
            host, port_str = address.rsplit(":", 1)
            return host, int(port_str)
        return address, 6400

    def _send(self, opcode: int, payload: bytes) -> bytes:
        """Send a command and return the response payload.

        Raises ``ProtocolError`` if the server returns an error status.
        """
        status, resp = self._client.send_command(opcode, payload)
        if status == STATUS_ERROR:
            raise ProtocolError(resp.decode("utf-8", errors="replace"))
        return resp

    def register(self, *stream_classes: type) -> None:
        """Register one or more stream/view classes with the server.

        Each class must have been decorated with ``@tally.stream`` or
        ``@tally.view`` and therefore have a ``_to_register_json()`` method.
        """
        self._client.drain_errors_nonblock()
        for cls in stream_classes:
            definition = cls._to_register_json()
            payload = encode_register(definition)
            self._send(OP_REGISTER, payload)

    def push(self, stream_class: type, event: dict) -> None:
        """Push an event to a stream (fire-and-forget).

        Returns immediately without waiting for the server to process
        the event. Errors from this push (or any prior async push) surface
        on the NEXT ``push``, ``push_sync``, ``flush``, ``get``, ``set``,
        ``mget``, ``mset``, or ``register`` call on this :class:`App`.

        Call :meth:`push_sync` if you need the resulting
        :class:`FeatureResult` inline. Call :meth:`flush` before program exit
        to guarantee all pending pushes are drained and any remaining server
        errors are surfaced.

        Args:
            stream_class: The ``@tally.stream``-decorated class.
            event: Event dict (must contain the key field).
        """
        self._client.drain_errors_nonblock()
        stream_name = stream_class._tally_stream_name
        payload = encode_push_binary(stream_name, event)
        self._client.send_frame_no_recv(OP_PUSH_ASYNC, payload)

    def push_sync(self, stream_class: type, event: dict) -> FeatureResult:
        """Push an event and wait for the updated feature map (v1.1 semantics).

        Slower than :meth:`push` but returns the features computed for the
        event's entity key in the same round trip. Uses the Phase 11 binary
        encoder for the request payload.
        """
        self._client.drain_errors_nonblock()
        stream_name = stream_class._tally_stream_name
        payload = encode_push_binary(stream_name, event)
        resp = self._send(OP_PUSH, payload)
        data = json.loads(resp) if resp else {}
        return FeatureResult(data)

    def flush(self) -> None:
        """Block until all prior fire-and-forget pushes are processed.

        Sends ``OP_FLUSH`` and waits for the server's acknowledgment frame.
        Raises :class:`ProtocolError` if any prior async push produced an
        error that has not yet been drained.
        """
        self._client.drain_errors_nonblock()
        self._send(OP_FLUSH, b"")

    def get(self, key: str) -> FeatureResult:
        """Read all current features for an entity key.

        Returns ``FeatureResult`` (empty if the key is unknown to the server).
        """
        self._client.drain_errors_nonblock()
        payload = encode_get(key)
        resp = self._send(OP_GET, payload)
        data = json.loads(resp) if resp else {}
        return FeatureResult(data)

    def mget(self, keys: list[str]) -> dict[str, FeatureResult]:
        """Fetch features for multiple keys in a single round trip.

        Args:
            keys: List of entity keys to fetch.

        Returns:
            Dict mapping each key to a ``FeatureResult``. Unknown keys
            map to an empty ``FeatureResult``.
        """
        self._client.drain_errors_nonblock()
        payload = encode_mget(keys)
        resp = self._send(OP_MGET, payload)
        data = json.loads(resp) if resp else {}
        return {k: FeatureResult(v) for k, v in data.items()}

    def set(self, key: str, features: dict) -> None:
        """Directly write feature values for a key (batch features).

        Args:
            key: Entity key.
            features: Dict of feature_name to value.
        """
        self._client.drain_errors_nonblock()
        payload = encode_set(key, features)
        self._send(OP_SET, payload)

    def mset(self, entries: dict[str, dict]) -> None:
        """Bulk direct write of feature values for multiple keys.

        Args:
            entries: Dict mapping entity keys to feature dicts.
        """
        self._client.drain_errors_nonblock()
        payload = encode_mset(entries)
        self._send(OP_MSET, payload)

    def close(self) -> None:
        """Close the underlying TCP connection."""
        self._client.close()

    def __enter__(self) -> App:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
