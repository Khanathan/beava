"""High-level App class that wires together the TCP client, protocol encoding,
and DSL layer into the user-facing API.

Usage::

    import tally as tl

    app = tl.App("localhost:6400")
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
    OP_PUSH_BATCH,
    OP_REGISTER,
    OP_SET,
    STATUS_ERROR,
    encode_get,
    encode_mget,
    encode_mset,
    encode_push_batch,
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
        self._batch_id_counter: int = 0

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

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, *stream_classes) -> None:
        """Register one or more pipeline definitions with the server.

        Accepts v0 Stream/Table descriptors. Before sending anything, runs
        :func:`tally._validate_v0.validate` on the full descriptor set — if
        any validation errors surface, raises the first one (with a tail
        count in the message) and sends no REGISTER frames.

        On success, walks the DAG in topological order, calls
        ``_collect_registrations()`` on each descriptor, dedupes REGISTER
        frames by ``name``, and forwards each to the server.
        """
        from tally._dag import build_dag
        from tally._validate_v0 import ValidationError, validate

        self._client.drain_errors_nonblock()

        descriptors = list(stream_classes)
        errors = validate(*descriptors)
        if errors:
            head = errors[0]
            if len(errors) > 1:
                raise ValidationError(
                    kind=head.kind,
                    path=head.path,
                    message=(
                        f"{head.message}\n\n…and {len(errors) - 1} more "
                        f"validation errors — call tally.validate() to see all"
                    ),
                )
            raise head

        dag = build_dag(descriptors)
        order = dag.topological_order()
        seen: set[str] = set()
        for node_name in order:
            desc = dag.nodes[node_name]
            if hasattr(desc, "_collect_registrations"):
                for reg in desc._collect_registrations():
                    if reg["name"] in seen:
                        continue
                    seen.add(reg["name"])
                    payload = encode_register(reg)
                    self._send(OP_REGISTER, payload)
            elif hasattr(desc, "_to_register_json"):
                definition = desc._to_register_json()
                if definition["name"] in seen:
                    continue
                seen.add(definition["name"])
                payload = encode_register(definition)
                self._send(OP_REGISTER, payload)

    def validate(self, *descriptors) -> list:
        """Run local validation without any TCP contact.

        Returns a list of :class:`tally.ValidationError` (empty on success).
        Useful in tests to assert a pipeline is valid without catching
        exceptions from :meth:`register`.
        """
        from tally._validate_v0 import validate as _v
        return _v(*descriptors)

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

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
            stream_class: The pipeline definition (SourceDef or decorated class).
            event: Event dict (must contain the key field).
        """
        self._client.drain_errors_nonblock()
        stream_name = stream_class._tally_stream_name
        payload = encode_push_binary(stream_name, event)
        self._client.send_frame_no_recv(OP_PUSH_ASYNC, payload)

    def _next_batch_id(self) -> int:
        """Return a monotonic batch_id (u32 wrap-around)."""
        bid = self._batch_id_counter
        self._batch_id_counter = (self._batch_id_counter + 1) & 0xFFFFFFFF
        return bid

    def push_many(self, stream_class: type, events) -> None:
        """Push a batch of events in one wire frame (fire-and-forget).

        Wraps all events into a single OP_PUSH_BATCH (0x0A) frame,
        reducing per-event Python overhead from ~7us to ~0.3us.
        Errors surface via drain_errors_nonblock on the next call,
        attributed as (batch_id, event_index) per D-09.

        Args:
            stream_class: The pipeline definition (SourceDef or decorated class).
            events: Iterable of event dicts. Must contain <= 16,384 events
                    (server hard cap H-7).
        """
        self._client.drain_errors_nonblock()
        stream_name = stream_class._tally_stream_name
        batch_id = self._next_batch_id()
        payload = encode_push_batch(stream_name, events, batch_id)
        self._client.send_frame_no_recv(OP_PUSH_BATCH, payload)

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

    # ------------------------------------------------------------------
    # Read / Write
    # ------------------------------------------------------------------

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
