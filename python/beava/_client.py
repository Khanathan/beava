"""TCP client for communicating with the Beava server.

Manages a persistent TCP connection with lazy connect, auto-reconnect on
server disconnect, frame-level send/receive, and configurable timeout.

Usage::

    client = BeavaClient("127.0.0.1", 6400, timeout=5.0)
    status, payload = client.send_command(OP_PUSH, encoded_payload)
    client.close()

Or as a context manager::

    with BeavaClient("127.0.0.1", 6400) as client:
        status, payload = client.send_command(OP_PUSH, encoded_payload)
"""

from __future__ import annotations

import select
import socket
import struct

from beava._protocol import MAX_FRAME_SIZE, STATUS_ERROR, STATUS_OK, encode_frame
from beava._types import ConnectionError, ProtocolError


class BeavaClient:
    """Low-level TCP client for the Beava binary protocol.

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
        # Phase 11: first-error sink for deferred async errors. Populated by
        # drain_errors_nonblock when one or more STATUS_ERROR frames are
        # buffered from prior OP_PUSH_ASYNC calls; raised by the next drain
        # or by send_command before its own send/recv pair.
        self._pending_error: ProtocolError | None = None
        # Non-blocking drain scratch buffer: accumulates bytes that arrive
        # partially between drain calls so we never block the hot path on
        # a kernel-buffered partial frame.
        self._drain_buf: bytearray = bytearray()
        # Phase 59 Wave 3 D-B1 / D-E4: cache of the server's advertised
        # capability bits + version tag. ``None`` until the client calls
        # ``negotiate_wire_format()`` (explicit or via BEAVA_WIRE_NEGOTIATE=1).
        # ``(0, 0)`` sentinel = pre-59 server (negotiate returned
        # STATUS_ERROR) — client keeps emitting binary without handshake.
        self.server_capability_bits: int | None = None
        self.server_version_tag: int | None = None
        # Phase 59.6 Wave 6 (TPC-PERF-11): per-stream schema_id cache. The
        # REGISTER ack JSON carries the assigned ``schema_id`` field when
        # the stream has a typed schema. ``App.register`` parses the ack
        # and populates this map so subsequent ``_pack_typed_batch`` calls
        # can send the real id instead of the Wave-2 ``schema_id=0``
        # shortcut (which Wave 6 removed server-side).
        self._schema_ids: dict[str, int] = {}

        # Phase 59 Wave 3 D-B4: BEAVA_WIRE_NEGOTIATE=1 opt-in handshake on
        # connect. Default off — backwards compat for users on pre-59
        # servers (they can set the flag manually once their server is
        # upgraded). Safe to call before the first send_command: if the
        # socket errors, we silently defer; the next real command will
        # reassert the error via its own connect + send path.
        import os as _os

        if _os.environ.get("BEAVA_WIRE_NEGOTIATE") == "1":
            try:
                self.negotiate_wire_format()
            except (OSError, ConnectionError):
                # Socket-level errors surface as usual on the next real
                # command. Handshake failures from pre-59 servers (STATUS_ERROR
                # "unknown opcode 0x18") are swallowed by negotiate_wire_format
                # itself (D-E4); they never raise here.
                pass

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

    def drain_errors_nonblock(self) -> None:
        """Drain ALL pending server frames in a truly non-blocking fashion.

        Called by App before every user-facing operation (push, push_sync,
        flush, get, set, mset, register). This method must NEVER block —
        the Phase 11 PERF-01 hot path requires a sub-microsecond fast path
        when no data is pending.

        Implementation (H-1 / H-2 fix):

        1. If a deferred ``_pending_error`` is already set from a prior
           drain, raise it immediately (FIFO first-error semantics).
        2. Flip the socket to non-blocking, drain everything currently in
           the kernel buffer into ``_drain_buf`` until ``BlockingIOError``.
        3. Restore blocking mode + original timeout BEFORE any raise.
        4. Parse ALL complete frames out of the buffer. Partial frames stay
           buffered for the next drain — never blocks on short reads.
        5. Collect every STATUS_ERROR into ``_pending_error`` (first wins).
           STATUS_OK frames are stray ACKs from prior paths and are silently
           consumed.
        6. If any error was collected, raise the first one.

        Fast path: when ``_pending_error is None`` and the first recv raises
        ``BlockingIOError`` with an empty ``_drain_buf``, we return in O(1)
        with no allocations beyond the single recv probe.
        """
        if self._pending_error is not None:
            err, self._pending_error = self._pending_error, None
            raise err

        if self._sock is None:
            return

        sock = self._sock

        # PERF fast path (Phase 11 hot-path repair): if there are no buffered
        # partial-frame bytes from a prior drain AND select reports the socket
        # has nothing readable right now, return in a single syscall. The Phase
        # 11 benchmark loop calls this on EVERY async push, so the happy-path
        # cost must be ~1 syscall, not 5 (gettimeout + setblocking(False) +
        # recv + setblocking(True) + settimeout).
        if not self._drain_buf:
            try:
                readable, _, _ = select.select([sock], [], [], 0)
            except (OSError, ValueError):
                # Socket in a bad state; let the next real op surface it.
                return
            if not readable:
                return

        # Slow path: there's data to drain (or a partial frame carried over).
        # Preserve current blocking/timeout state, flip to non-blocking.
        try:
            prev_timeout = sock.gettimeout()
            sock.setblocking(False)
        except OSError:
            # Socket is in a bad state; let the next real op surface it.
            return

        disconnected = False
        try:
            # 1. Drain everything currently in the kernel buffer.
            while True:
                try:
                    chunk = sock.recv(8192)
                except BlockingIOError:
                    break
                except OSError:
                    # Treat any other socket error as disconnect-pending.
                    disconnected = True
                    break
                if not chunk:
                    # Server closed the connection cleanly.
                    disconnected = True
                    break
                self._drain_buf.extend(chunk)
        finally:
            # 2. Restore prior blocking/timeout state even if recv raised.
            try:
                sock.setblocking(True)
                sock.settimeout(prev_timeout)
            except OSError:
                pass

        if disconnected:
            self._sock = None

        # 3. Parse all complete frames; partial frames stay buffered.
        first_error: ProtocolError | None = None
        buf = self._drain_buf
        while True:
            if len(buf) < 4:
                break
            length = struct.unpack(">I", bytes(buf[:4]))[0]
            if length == 0:
                # Protocol violation — drop the header and surface an error.
                del buf[:4]
                if first_error is None:
                    first_error = ProtocolError("response frame length is zero")
                continue
            if length > MAX_FRAME_SIZE:
                # Protocol violation — reset buffer and surface an error.
                buf.clear()
                if first_error is None:
                    first_error = ProtocolError(
                        f"response frame too large: {length} bytes"
                    )
                break
            if len(buf) < 4 + length:
                # Partial frame — keep it and wait for the next drain.
                break
            status = buf[4]
            payload = bytes(buf[5 : 4 + length])
            del buf[: 4 + length]
            if status == STATUS_ERROR:
                if first_error is None:
                    first_error = ProtocolError(
                        payload.decode("utf-8", errors="replace")
                    )
                # Additional errors are dropped; we only surface the first
                # in FIFO order. This matches the "at-least-one error
                # surfaced per bad async batch" contract.
            # STATUS_OK: stray ACK, discard.

        if first_error is not None:
            # Store NOTHING in _pending_error here — we are raising this one
            # right now. Any subsequent errors in the same batch were dropped
            # above by design.
            raise first_error

    def send_frame_no_recv(self, opcode: int, payload: bytes) -> None:
        """Send one wire frame with NO response read (fire-and-forget).

        Used by ``App.push()`` for ``OP_PUSH_ASYNC`` and ``App.flush()`` for
        ``OP_FLUSH``. Auto-reconnects once on broken pipe, mirroring
        :meth:`send_command`.

        **Delivery semantic: at-least-once.** If ``sendall`` raises
        ``OSError`` mid-write (for example, a broken pipe after the kernel
        has already shipped some bytes to the server), this method
        reconnects and re-sends the full frame. Under that failure mode the
        original event may have reached the server on the old connection
        AND a duplicate event will arrive on the new connection. For
        idempotent operators (``last``, ``set`` of a static feature) this
        is harmless; for accumulating operators (``count``, ``sum``) a
        duplicate event doubles its contribution. If your pipeline cannot
        tolerate at-least-once async push, use :meth:`send_command` with
        ``OP_PUSH`` (sync push) instead — sync push uses request/response
        ordering to expose the failure to the caller, who can decide
        whether to retry. Server-side de-duplication is deferred to a
        future phase (see T-11-12).
        """
        self._ensure_connected()
        try:
            self._send_frame(opcode, payload)
        except (OSError, ConnectionError):
            self._sock = None
            self._connect()
            self._send_frame(opcode, payload)

    def send_command(self, opcode: int, payload: bytes) -> tuple[int, bytes]:
        """Send a command and return the response ``(status, payload)``.

        Connects lazily on first call. If the connection is broken,
        auto-reconnects once and retries the send transparently.

        Phase 11 H-2 fix: drains any pending async error frames from the
        kernel buffer BEFORE sending, so a stale error from a prior
        OP_PUSH_ASYNC cannot be mis-paired with this sync response. If
        the drain surfaces an error, it is raised before the send — the
        caller sees the async failure first, and this sync call can be
        retried afterwards without frame desync.
        """
        # H-2: drain any buffered async errors before sending. If drain
        # raises, the sync send never happens and frame pairing stays
        # consistent. Also consumes any STATUS_OK stragglers so our own
        # _recv_frame below pairs with the response to THIS command.
        self.drain_errors_nonblock()
        self._ensure_connected()
        # Any bytes left in _drain_buf must be partial frames (less than a
        # full header+body). We splice them in front of the next read so
        # sync recv stays byte-aligned with the server's frame stream.
        prefix = bytes(self._drain_buf)
        self._drain_buf.clear()
        try:
            self._send_frame(opcode, payload)
            return self._recv_frame_with_prefix(prefix)
        except ConnectionError:
            # Connection dropped -- reconnect and retry once. Any leftover
            # drain prefix is from the dead connection and must be dropped.
            self._sock = None
            self._connect()
            self._send_frame(opcode, payload)
            return self._recv_frame()

    def _recv_frame_with_prefix(self, prefix: bytes) -> tuple[int, bytes]:
        """Like ``_recv_frame`` but prepends ``prefix`` to the socket stream.

        Used by ``send_command`` after the drain path has already consumed
        some bytes into ``_drain_buf`` that turned out to be a partial
        frame. Those bytes are the head of the server's response stream,
        so we stitch them back in before the blocking recv.
        """
        if not prefix:
            return self._recv_frame()
        buf = bytearray(prefix)
        # Fill the header if needed.
        while len(buf) < 4:
            chunk = self._sock.recv(4 - len(buf))
            if not chunk:
                self._sock = None
                raise ConnectionError("server closed connection")
            buf.extend(chunk)
        length = struct.unpack(">I", bytes(buf[:4]))[0]
        if length == 0:
            raise ProtocolError("response frame length is zero")
        if length > MAX_FRAME_SIZE:
            raise ProtocolError(f"response frame too large: {length} bytes")
        # Fill the body if needed.
        needed = 4 + length
        while len(buf) < needed:
            chunk = self._sock.recv(needed - len(buf))
            if not chunk:
                self._sock = None
                raise ConnectionError("server closed connection")
            buf.extend(chunk)
        body = bytes(buf[4:needed])
        # Any bytes past `needed` would mean the server pipelined a second
        # frame into our response buffer. Stash them back in _drain_buf so
        # the next drain/send can process them.
        if len(buf) > needed:
            self._drain_buf.extend(buf[needed:])
        return body[0], body[1:]

    # ------------------------------------------------------------------
    # Phase 55-02 Task 3 (TPC-SOURCE-01): @bv.source_table wire methods.
    # ------------------------------------------------------------------

    @staticmethod
    def _write_varint_string(buf: bytearray, s: str) -> None:
        data = s.encode("utf-8")
        n = len(data)
        while True:
            byte = n & 0x7F
            n >>= 7
            if n == 0:
                buf.append(byte)
                break
            buf.append(byte | 0x80)
        buf.extend(data)

    def _source_table_table_name(self, table) -> str:
        """Accept either a SourceTable instance or a raw string."""
        return getattr(table, "_name", None) or str(table)

    def upsert_table_row(
        self,
        table,
        key: str,
        fields: dict,
        source_lsn: int,
    ) -> int:
        """Upsert a row into a ``@bv.source_table``. Returns the echoed
        ``source_lsn`` (u64, opaque). Uses OP_UPSERT_TABLE_ROW (0x14).
        """
        import json

        name = self._source_table_table_name(table)
        fields_json = json.dumps(fields, separators=(",", ":")).encode("utf-8")
        buf = bytearray()
        self._write_varint_string(buf, name)
        self._write_varint_string(buf, key)
        buf.extend(int(source_lsn).to_bytes(8, "little", signed=False))
        buf.extend(len(fields_json).to_bytes(4, "little", signed=False))
        buf.extend(fields_json)
        status, payload = self.send_command(0x14, bytes(buf))
        if status != 0x00:
            raise ProtocolError(f"upsert_table_row failed: {payload!r}")
        if len(payload) < 8:
            raise ProtocolError("upsert_table_row: ack body missing source_lsn")
        return int.from_bytes(payload[:8], "little", signed=False)

    def delete_table_row(self, table, key: str, source_lsn: int) -> int:
        """Delete a row from a ``@bv.source_table`` (hard-delete + retraction
        marker). Returns the echoed ``source_lsn``. OP_DELETE_TABLE_ROW (0x15).
        """
        name = self._source_table_table_name(table)
        buf = bytearray()
        self._write_varint_string(buf, name)
        self._write_varint_string(buf, key)
        buf.extend(int(source_lsn).to_bytes(8, "little", signed=False))
        status, payload = self.send_command(0x15, bytes(buf))
        if status != 0x00:
            raise ProtocolError(f"delete_table_row failed: {payload!r}")
        return int.from_bytes(payload[:8], "little", signed=False)

    def upsert_table_batch(
        self,
        table,
        rows,
    ) -> list[int]:
        """Batch upsert. ``rows`` is a sequence of
        ``(key: str, fields: dict, source_lsn: int)`` tuples. All-or-nothing
        (D-B4): the first validation failure aborts the whole batch. Returns
        the echoed ``source_lsns`` in INPUT order. OP_UPSERT_TABLE_BATCH (0x16).
        """
        import json

        name = self._source_table_table_name(table)
        buf = bytearray()
        self._write_varint_string(buf, name)
        rows_list = list(rows)
        buf.extend(len(rows_list).to_bytes(4, "little", signed=False))
        for (k, fields, lsn) in rows_list:
            self._write_varint_string(buf, k)
            buf.extend(int(lsn).to_bytes(8, "little", signed=False))
            body = json.dumps(fields, separators=(",", ":")).encode("utf-8")
            buf.extend(len(body).to_bytes(4, "little", signed=False))
            buf.extend(body)
        status, payload = self.send_command(0x16, bytes(buf))
        if status != 0x00:
            raise ProtocolError(f"upsert_table_batch failed: {payload!r}")
        if len(payload) < 4:
            raise ProtocolError("upsert_table_batch: ack missing count header")
        count = int.from_bytes(payload[:4], "little", signed=False)
        out = []
        for i in range(count):
            start = 4 + 8 * i
            out.append(int.from_bytes(payload[start : start + 8], "little", signed=False))
        return out

    def delete_table_batch(self, table, rows) -> list[int]:
        """Batch delete. ``rows`` is ``(key: str, source_lsn: int)`` tuples.
        All-or-nothing. Returns echoed source_lsns in INPUT order.
        OP_DELETE_TABLE_BATCH (0x17).
        """
        name = self._source_table_table_name(table)
        buf = bytearray()
        self._write_varint_string(buf, name)
        rows_list = list(rows)
        buf.extend(len(rows_list).to_bytes(4, "little", signed=False))
        for (k, lsn) in rows_list:
            self._write_varint_string(buf, k)
            buf.extend(int(lsn).to_bytes(8, "little", signed=False))
        status, payload = self.send_command(0x17, bytes(buf))
        if status != 0x00:
            raise ProtocolError(f"delete_table_batch failed: {payload!r}")
        count = int.from_bytes(payload[:4], "little", signed=False)
        out = []
        for i in range(count):
            start = 4 + 8 * i
            out.append(int.from_bytes(payload[start : start + 8], "little", signed=False))
        return out

    def negotiate_wire_format(self) -> tuple[int, int]:
        """Phase 59 Wave 3 D-B1 (TPC-PERF-09): handshake with the server.

        Sends ``OP_NEGOTIATE_WIRE_FORMAT`` with this client's capability bits
        + version tag; receives the server's bits + version. Caches the
        result in ``self.server_capability_bits`` + ``self.server_version_tag``
        for the connection lifetime.

        Returns ``(server_bits, server_version)``. On a pre-Phase-59 server
        that rejects 0x18 as unknown opcode, returns ``(0, 0)`` sentinel
        and caches the same — indicating the client should fall back to
        "emit binary without handshake" (D-E4). Safe to ignore the return
        value; the cached attributes are the primary consumer.

        The opcode is OPTIONAL per D-B2 — Phase 59+ servers accept OP_PUSH
        with either binary or JSON body regardless of whether negotiation
        happened. Calling this method lets the client stop paying the
        server's auto-detect first-byte discriminator cost (a few ns/event)
        and confirms the version tag.
        """
        # Local imports: keep module-load cost low; constants live in
        # _protocol alongside the rest of the wire vocabulary.
        from beava._protocol import (
            OP_NEGOTIATE_WIRE_FORMAT,
            WIRE_BINARY_PASSTHROUGH,
            WIRE_VERSION_TAG_CLIENT,
        )

        payload = struct.pack(">IH", WIRE_BINARY_PASSTHROUGH, WIRE_VERSION_TAG_CLIENT)
        status, body = self.send_command(OP_NEGOTIATE_WIRE_FORMAT, payload)
        if status != STATUS_OK:
            # Pre-Phase-59 server (STATUS_ERROR "unknown opcode: 0x18") or
            # any malformed response. D-E4: silently fall back. The
            # connection stays open per the Rust-side guarantee that
            # STATUS_ERROR does not tear down the stream.
            self.server_capability_bits = 0
            self.server_version_tag = 0
            return (0, 0)
        if len(body) < 6:
            # Malformed response from a broken server — don't crash; fall
            # back same as pre-59 server case.
            self.server_capability_bits = 0
            self.server_version_tag = 0
            return (0, 0)
        server_bits = struct.unpack(">I", body[:4])[0]
        server_version = struct.unpack(">H", body[4:6])[0]
        self.server_capability_bits = server_bits
        self.server_version_tag = server_version
        return (server_bits, server_version)

    # ------------------------------------------------------------------
    # Phase 59.6 Wave 2 (TPC-PERF-11): typed-row batch packer.
    # ------------------------------------------------------------------

    @staticmethod
    def _pack_typed_batch(
        stream_name: str,
        schema_json: dict,
        events,
        ack_token: int,
        schema_id: int = 0,
    ) -> bytes:
        """Pack events into an OP_PUSH_TYPED_BATCH wire body (D-B1).

        Mirrors the server-side decoder in
        ``src/wire/typed.rs::decode_typed_row_push_batch`` byte-for-byte:

        - ``[u16 BE stream_name_len][stream_name utf-8]``
        - ``[u32 BE schema_id]`` — 0 is the Wave-2 "trust stream_name
          lookup" shortcut (server accepts and uses the registered
          schema's id).
        - ``[u32 BE row_count]``
        - ``[row_count × schema.row_size bytes]`` — fixed-layout payload
          per field; string / bytes fields store ``(start: u32, len: u32)``
          pointers into the shared arena.
        - ``[u32 BE arena_len][arena_bytes]``
        - ``[u64 BE ack_token]``

        ``schema_json`` is the dict produced by ``CompiledSchema.to_json()``
        — the same shape the server's ``RegisterSchemaJson`` consumes. The
        function reads ``inline_str_cap``, ``fields``, and ``row_size`` from
        it.

        Raises ``ValueError`` for unsupported FieldTy names.
        """
        out = bytearray()
        # [u16 stream_name_len][stream_name utf-8]
        name_bytes = stream_name.encode("utf-8")
        out += struct.pack(">H", len(name_bytes))
        out += name_bytes
        # [u32 schema_id][u32 row_count]
        out += struct.pack(">I", schema_id)
        inline_cap = int(schema_json["inline_str_cap"])
        fields = schema_json["fields"]
        row_size = int(schema_json["row_size"])
        # Materialize events now so we have an accurate row_count upfront.
        events_list = list(events)
        out += struct.pack(">I", len(events_list))
        arena_bytes = bytearray()
        for evt in events_list:
            row = bytearray(row_size)  # zero-filled
            for f in fields:
                name = f["name"]
                ty = f["ty"]
                off = int(f["offset"])
                val = evt.get(name)
                if val is None:
                    # Nullable or missing — zero bytes are the
                    # well-defined "absent" representation.
                    continue
                if ty == "i64":
                    struct.pack_into("<q", row, off, int(val))
                elif ty == "f64":
                    struct.pack_into("<d", row, off, float(val))
                elif ty == "bool":
                    row[off] = 1 if val else 0
                elif ty == "inline_str":
                    b = str(val).encode("utf-8")
                    copy_len = min(len(b), inline_cap)
                    row[off : off + copy_len] = b[:copy_len]
                    # Trailing bytes (including the NUL terminator slot at
                    # off+inline_cap) are already zero from bytearray init.
                elif ty == "string":
                    b = str(val).encode("utf-8")
                    start = len(arena_bytes)
                    arena_bytes += b
                    struct.pack_into("<II", row, off, start, len(b))
                elif ty == "bytes":
                    b = bytes(val)
                    start = len(arena_bytes)
                    arena_bytes += b
                    struct.pack_into("<II", row, off, start, len(b))
                else:
                    raise ValueError(f"unknown FieldTy {ty!r} for field {name!r}")
            out += bytes(row)
        out += struct.pack(">I", len(arena_bytes))
        out += bytes(arena_bytes)
        out += struct.pack(">Q", int(ack_token))
        return bytes(out)

    def close(self) -> None:
        """Close the TCP connection (if open)."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def __enter__(self) -> BeavaClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
