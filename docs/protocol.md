# Tally Binary Protocol Specification

## 1. Overview

Tally uses a custom binary protocol over persistent TCP connections for all hot-path operations (PUSH, GET, SET, MSET, MGET). The protocol is designed for minimal overhead: length-prefixed frames, binary opcodes, and no HTTP/JSON framing on the wire.

Connections are persistent. The client opens a TCP socket once and sends multiple commands over the same connection. The server spawns one async task per connection and reads frames in a loop until the client disconnects.

The protocol supports two modes of operation:
- **Synchronous** -- client sends a command frame, server replies with a response frame (PUSH, GET, SET, MSET, MGET, REGISTER).
- **Asynchronous (fire-and-forget)** -- client sends a command frame with no immediate response (PUSH_ASYNC, PUSH_BATCH, FLUSH). Errors from async commands are surfaced as error response frames before the next synchronous response on the same connection.

## 2. Wire Format

### 2.1 Request Frame

```
Offset  Size     Field
------  ----     -----
0       4 bytes  Message length (u32, big-endian)
4       1 byte   Opcode
5       N bytes  Payload (opcode-specific)
```

**Message length** = 1 (opcode byte) + len(payload). It does NOT include the 4-byte length field itself.

Minimum valid frame is 5 bytes on the wire: 4 bytes length + 1 byte opcode (with length = 1 and zero-length payload).

Maximum frame size is 64 MB (67,108,864 bytes). Frames exceeding this limit are rejected before buffer allocation (DoS protection).

### 2.2 Response Frame

```
Offset  Size     Field
------  ----     -----
0       4 bytes  Message length (u32, big-endian)
4       1 byte   Status code
5       N bytes  Payload (status-specific)
```

**Message length** = 1 (status byte) + len(payload). Same accounting as request frames.

### 2.3 Status Codes

| Code | Name         | Description |
|------|--------------|-------------|
| 0x00 | STATUS_OK    | Command succeeded. Payload is the command-specific result. |
| 0x01 | STATUS_ERROR | Command failed. Payload is a UTF-8 error message string. |

### 2.4 Error Response

On any error (protocol violation, unknown opcode, application error):

```
[4 bytes: length = 1 + len(error_message)]
[1 byte:  0x01 (STATUS_ERROR)]
[N bytes: UTF-8 error message]
```

The error message is a human-readable string, not structured. Clients should treat the entire payload as an opaque error description.

## 3. String Encoding

Strings within command payloads use a length-prefixed encoding:

```
Offset  Size     Field
------  ----     -----
0       2 bytes  String length (u16, big-endian)
2       N bytes  UTF-8 string data
```

Maximum string length is 65,535 bytes (u16::MAX). Strings exceeding this limit cause a protocol error. This limit applies to stream names, entity keys, and field keys within binary event payloads.

## 4. Commands

### 4.1 Opcodes

| Opcode | Name       | Mode   | Description |
|--------|------------|--------|-------------|
| 0x01   | PUSH       | Sync   | Push event to a stream (synchronous) |
| 0x02   | GET        | Sync   | Read current features for a key |
| 0x03   | SET        | Sync   | Direct write of feature values for a key |
| 0x04   | MSET       | Sync   | Bulk direct write |
| 0x05   | REGISTER   | Sync   | Register pipeline definitions |
| 0x06   | MGET       | Sync   | Bulk read features for multiple keys |
| 0x07   | PUSH_ASYNC | Async  | Push event (fire-and-forget) |
| 0x08   | FLUSH      | Sync   | Flush buffered async pushes |
| 0x0A   | PUSH_BATCH | Async  | Batch push of multiple events for one stream |

---

### 4.2 PUSH (0x01) -- Synchronous Push

Pushes an event to a named stream. The server processes the event through the pipeline engine (updates operators, evaluates derives, triggers fan-out) and returns a response.

#### Request Payload

Uses the binary event encoding format:

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Stream name length (u16 BE)
2       N bytes  Stream name (UTF-8)
2+N     ...      Binary event body (see Section 5)
```

#### Response Payload (STATUS_OK)

JSON bytes: a JSON object mapping feature names to their current values. In the current implementation, sync PUSH returns an empty JSON object `{}` as an acknowledgment. Callers that need feature values should issue a subsequent GET.

#### Example

Push to stream "Transactions" with event `{"user_id": "u123", "amount": 50.0}`:

```
Frame (total = 4-byte length prefix + 50-byte payload = 54 bytes on the wire):

  00 00 00 32                              length = 50 (payload bytes after this)
  01                                        opcode = PUSH
  00 0C                                     stream name length = 12
  54 72 61 6E 73 61 63 74 69 6F 6E 73      "Transactions"
  00 02                                     field count = 2
  00 07                                     key length = 7
  75 73 65 72 5F 69 64                     "user_id"
  04                                        TYPE_STR
  00 04                                     string value length = 4
  75 31 32 33                              "u123"
  00 06                                     key length = 6
  61 6D 6F 75 6E 74                        "amount"
  03                                        TYPE_F64
  40 49 00 00 00 00 00 00                  50.0 as IEEE754 double (big-endian)

Response:
  00 00 00 03                               length = 3
  00                                        STATUS_OK
  7B 7D                                     "{}" (JSON)
```

Payload byte count: 1 (opcode) + 2 + 12 (stream name) + 2 (field count) + 2 + 7 (key) + 1 + 2 + 4 (string value) + 2 + 6 (key) + 1 + 8 (f64) = 50 bytes.

---

### 4.3 GET (0x02) -- Read Features

Reads all current features for an entity key across all registered streams and views.

#### Request Payload

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Key length (u16 BE)
2       N bytes  Key string (UTF-8)
```

#### Response Payload (STATUS_OK)

JSON bytes: a JSON object mapping feature names to their current computed values.

```json
{"tx_count_1h": 7, "tx_sum_1h": 350.0, "failure_rate": 0.14}
```

---

### 4.4 SET (0x03) -- Direct Write

Writes feature values directly for an entity key. These are stored as static features, bypassing the pipeline engine. Used for batch-computed features from offline systems.

#### Request Payload

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Key length (u16 BE)
2       N bytes  Key string (UTF-8)
2+N     M bytes  JSON object (remaining bytes)
```

The JSON object maps feature names to values. Example: `{"lifetime_value": 4500.0, "segment": "high_value"}`.

#### Response Payload (STATUS_OK)

Empty payload (zero bytes after the status byte). The 4-byte length field will be `00 00 00 01`.

---

### 4.5 MSET (0x04) -- Bulk Direct Write

Writes feature values for multiple entity keys in a single command. Processed server-side in chunks of 1,024 entries with cooperative yielding between chunks, so PUSH and GET requests are not starved.

#### Request Payload

```
Offset  Size     Field
------  ----     -----
0       4 bytes  Entry count (u32 BE)
4       ...      Entries (repeated `count` times)
```

Each entry:

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Key length (u16 BE)
2       N bytes  Key string (UTF-8)
2+N     4 bytes  JSON length (u32 BE)
6+N     M bytes  JSON object bytes
```

#### Response Payload (STATUS_OK)

Empty payload (zero bytes after the status byte).

---

### 4.6 REGISTER (0x05) -- Register Pipeline

Registers a stream or view definition with the pipeline engine. Definitions are serialized as JSON from the Python SDK.

#### Request Payload

The entire payload is a JSON object. No string prefix -- the full payload bytes are parsed as JSON.

```json
{
  "name": "Transactions",
  "key_field": "user_id",
  "type": "stream",
  "features": [
    {"name": "tx_count_1h", "feature_type": "count", "window": "1h"},
    {"name": "tx_sum_1h", "feature_type": "sum", "field": "amount", "window": "1h"}
  ],
  "entity_ttl": "2h",
  "history_ttl": "72h"
}
```

Fields:
- `name` (string, required): Stream or view name.
- `key_field` (string, optional): The event field used as the entity key.
- `type` (string, optional): `"stream"` (default) or `"view"`.
- `features` (array, required): Feature definitions.
- `depends_on` (array, optional): Stream names this definition depends on.
- `filter` (string, optional): Expression to filter incoming events.
- `entity_ttl` (string, optional): TTL for entity key eviction (e.g., `"5m"`, `"1h"`).
- `history_ttl` (string, optional): TTL for event log retention (e.g., `"72h"`, `"7d"`).
- `projection` (object, optional): `{"select": [...]}` or `{"drop": [...]}` for feature filtering.
- `ephemeral` (bool, optional): If true, features are not persisted in snapshots.
- `ttl` (string, optional): Pipeline-level TTL.
- `max_keys` (integer, optional): Maximum number of entity keys to track.

Duration strings support suffixes: `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

#### Response Payload (STATUS_OK)

Empty payload (zero bytes after the status byte).

---

### 4.7 MGET (0x06) -- Bulk Read

Reads features for multiple entity keys in a single command.

#### Request Payload

```
Offset  Size     Field
------  ----     -----
0       4 bytes  Key count (u32 BE)
4       ...      Keys (repeated `count` times)
```

Each key is a protocol string:

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Key length (u16 BE)
2       N bytes  Key string (UTF-8)
```

#### Response Payload (STATUS_OK)

JSON bytes: a JSON object mapping each requested key to its feature map. Keys with no features return an empty object.

```json
{
  "u123": {"tx_count_1h": 7, "tx_sum_1h": 350.0},
  "u456": {"tx_count_1h": 2, "tx_sum_1h": 80.0},
  "u999": {}
}
```

Internally-qualified feature names (those containing `.`) are stripped from the response.

---

### 4.8 PUSH_ASYNC (0x07) -- Asynchronous Push

Identical wire format to PUSH (0x01). The server buffers the event in a per-connection accumulator and processes it in a batch. No response frame is sent for individual async pushes.

#### Request Payload

Same as PUSH (Section 4.2).

#### Response

No response frame is sent on success. If the buffered event causes an error during batch processing, the error is queued and delivered as a STATUS_ERROR response frame before the next synchronous response on the same connection.

#### Server-Side Batching

The server accumulates PUSH_ASYNC frames per connection with these parameters:
- **Batch size**: up to 64 events per batch.
- **Deadline**: 200 microseconds from the first buffered event.
- **Force flush**: any non-async command (GET, SET, PUSH, FLUSH, etc.) arriving on the same connection triggers an immediate flush of all buffered events before the sync command is processed.

Events are processed under a single state lock acquisition per batch for efficiency.

---

### 4.9 FLUSH (0x08) -- Flush Async Buffer

Forces the server to process all buffered PUSH_ASYNC events on this connection immediately.

#### Request Payload

Empty (zero bytes). The frame is just the opcode with no payload.

#### Response Payload (STATUS_OK)

Empty payload (zero bytes after the status byte). Any async errors accumulated during the flush are delivered as separate STATUS_ERROR frames before this STATUS_OK response.

---

### 4.10 PUSH_BATCH (0x0A) -- Batch Push

Sends multiple events for a single stream in one frame. Events are processed as fire-and-forget (no per-event responses). Errors are queued and delivered before the next synchronous response.

#### Request Payload

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Stream name length (u16 BE)
2       N bytes  Stream name (UTF-8)
2+N     4 bytes  Batch ID (u32 BE) -- client-assigned identifier
6+N     4 bytes  Event count (u32 BE)
10+N    ...      Events (repeated `count` times)
```

Each event:

```
Offset  Size     Field
------  ----     -----
0       4 bytes  Event byte length (u32 BE)
4       M bytes  Binary event body (see Section 5)
```

Maximum event count per batch: 16,384. The server rejects batches exceeding this limit.

#### Response

No response frame is sent. Errors from individual events within the batch are queued with the format `[batch:<batch_id> event:<index>] <error_message>` and delivered as STATUS_ERROR frames before the next synchronous response on the same connection.

## 5. Binary Event Body

Events use a compact binary encoding instead of JSON for the hot path. This format is used by PUSH (0x01), PUSH_ASYNC (0x07), and PUSH_BATCH (0x0A).

### 5.1 Structure

```
Offset  Size     Field
------  ----     -----
0       2 bytes  Field count (u16 BE)
2       ...      Fields (repeated `field_count` times)
```

Each field:

```
Offset  Size       Field
------  ----       -----
0       2 bytes    Key length (u16 BE)
2       K bytes    Key string (UTF-8)
2+K     1 byte     Type tag
3+K     V bytes    Value bytes (type-dependent)
```

### 5.2 Type Tags

| Tag  | Name      | Value Size | Encoding |
|------|-----------|------------|----------|
| 0x00 | TYPE_NULL | 0 bytes    | No value bytes. |
| 0x01 | TYPE_BOOL | 1 byte     | `0x00` = false, non-zero = true. |
| 0x02 | TYPE_I64  | 8 bytes    | Signed 64-bit integer, big-endian. |
| 0x03 | TYPE_F64  | 8 bytes    | IEEE 754 double, big-endian. NaN and Infinity are rejected. |
| 0x04 | TYPE_STR  | 2 + N bytes | `[u16 BE string length][UTF-8 bytes]`. Same encoding as protocol strings. |

Unknown type tags cause a protocol error.

### 5.3 Duplicate Fields

Duplicate field keys within a single event are allowed. The last occurrence wins, matching JSON object semantics.

### 5.4 Python Type Mapping

When encoding from Python:
- `None` maps to TYPE_NULL.
- `bool` maps to TYPE_BOOL. **Important**: `bool` must be checked before `int` because `bool` is a subclass of `int` in Python.
- `int` maps to TYPE_I64. Integers outside the signed 64-bit range (-2^63 to 2^63-1) are rejected.
- `float` maps to TYPE_F64. Non-finite values (NaN, Infinity, -Infinity) are rejected.
- `str` maps to TYPE_STR.
- All other types are rejected.

## 6. Connection Lifecycle

### 6.1 Connection Establishment

The client opens a plain TCP connection to the server (default port 6400). No handshake, no version negotiation, no authentication. The connection is ready for commands immediately after the TCP three-way handshake completes.

### 6.2 Persistent Connections

Connections are persistent. The client reuses the same TCP socket for all commands. The server reads frames in a loop until the client disconnects or a fatal protocol error occurs.

### 6.3 Auto-Reconnect (Client)

The Python client auto-reconnects transparently on broken connections:
- **Sync commands** (`send_command`): if the send or receive fails, the client reconnects once and retries the full send + receive.
- **Async commands** (`send_frame_no_recv`): if `sendall` raises an `OSError`, the client reconnects and re-sends the frame. This provides at-least-once delivery -- the event may have reached the server on the old connection AND be re-sent on the new connection.

### 6.4 Connection Closure

**Client-initiated**: the client closes the TCP socket. The server detects the EOF, flushes any buffered async events, delivers any queued errors, and drops the connection handler.

**Server-initiated**: on a fatal protocol error (invalid frame length, unknown opcode, malformed payload), the server sends a STATUS_ERROR response and closes the connection.

**Timeout**: the Python client sets a configurable socket timeout (default 5 seconds) for both connect and read operations.

### 6.5 Error Draining (Client)

Before every synchronous command, the Python client performs a non-blocking drain of the socket receive buffer. This consumes any STATUS_ERROR frames from prior PUSH_ASYNC calls and any stray STATUS_OK acknowledgments. If an error is found, it is raised before the sync command is sent, preserving frame alignment between requests and responses.

The drain uses `select()` with a zero timeout to avoid blocking the hot path when no data is pending.

## 7. Async Push Coalescing

The server implements connection-level batching of PUSH_ASYNC frames to reduce lock contention and improve throughput.

### 7.1 Accumulator

Each TCP connection maintains a stack-local `ConnAccumulator` that buffers incoming PUSH_ASYNC frames. The accumulator is flushed (all buffered events processed under a single state lock) when any of the following conditions is met:

1. **Batch size reached**: 64 events accumulated.
2. **Deadline elapsed**: 200 microseconds since the first buffered event (wall clock, not timer wheel).
3. **Sync command arrives**: any non-async opcode (GET, SET, PUSH, FLUSH, MSET, MGET, REGISTER) triggers an immediate flush before the sync command is processed. This guarantees that the sync command observes all prior async mutations.
4. **Client disconnects**: remaining buffered events are flushed before the connection handler exits.

### 7.2 Tight Read Loop

Under sustained async load, the server reads frames from the TCP buffer in a tight loop without `select!` overhead. It only falls back to the `select!`-based deadline path when the internal buffer is exhausted (fewer than 4 bytes available), meaning the next read would block on the kernel.

### 7.3 Error Ordering

Each async event is assigned a per-connection monotonic sequence number at accumulate time. When batch processing produces errors, they are queued with their sequence number and delivered to the client in order before the next synchronous response. This preserves push ordering regardless of internal stream-grouping reshuffles.

### 7.4 PUSH_BATCH vs PUSH_ASYNC

PUSH_BATCH (0x0A) is a client-side optimization that packs multiple events into a single TCP frame, reducing syscall overhead. On the server side, PUSH_BATCH events are unpacked and processed identically to accumulated PUSH_ASYNC events. The key difference is wire efficiency: one frame header instead of N.

PUSH_BATCH includes a `batch_id` (u32) assigned by the client, which is included in error messages for debugging. The event count within a single PUSH_BATCH frame is limited to 16,384.
