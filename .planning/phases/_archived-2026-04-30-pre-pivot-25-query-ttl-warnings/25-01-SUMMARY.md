---
phase: 25-query-ttl-warnings
plan: 01
subsystem: protocol+server+sdk+state-store
tags: [query, opcode, get-multi, reserved-opcodes, null-collapse, sdk]

dependency_graph:
  requires:
    - WIRE-TABLE-01    # OP_PUSH_TABLE end-to-end (Phase 24-02)
    - WIRE-TABLE-02    # OP_DELETE_TABLE end-to-end (Phase 24-02)
    - TABLE-STORE-02   # upsert/tombstone/get_table_row primitives
    - GET-MERGED-01    # merged GET view established in Phase 24-02
  provides:
    - QUERY-GET-MULTI-01       # OP_GET_MULTI opcode + handler
    - QUERY-NULL-COLLAPSE-01   # never-seen / tombstoned / empty all → null
    - QUERY-RESERVED-01        # SCAN / SUBSCRIBE reserved without tear-down
    - SDK-GET-MULTI-01         # App.get_multi(tables, key)
  affects:
    - Phase 25-02 (TTL + suggestion engine — consumes GET_MULTI as UI read path)
    - Phase 25-03 (warnings endpoint)

tech-stack:
  added: []
  patterns:
    - marker-variant-for-reserved-opcodes
    - error-type-on-read-path
    - hand-serialized-json-for-order-preservation
    - sdk-boundary-validation

key-files:
  created:
    - tests/test_op_get_multi.rs
    - tests/test_reserved_opcodes.rs
    - python/tests/test_get_multi_e2e.py
    - .planning/phases/25-query-ttl-warnings/25-01-SUMMARY.md
  modified:
    - src/error.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/state/store.rs
    - python/tally/_protocol.py
    - python/tally/_app.py

key-decisions:
  - "Opcode assignment: OP_GET_MULTI = 0x0D (contiguous after OP_DELETE_TABLE 0x0C). Reserved opcodes live in the 0x10-0x1F block per v0-restructure-spec §6.3 — OP_SCAN_RESERVED = 0x10 and OP_SUBSCRIBE_RESERVED = 0x11. Gap at 0x0E-0x0F left deliberately free for v0.x additions before the reserved block opens."
  - "Cardinality guards (count=0 rejected, count>256 rejected) applied at the PARSER, not the handler. This bounds per-request memory (T-25-01-01) before any state access, and keeps the Python SDK and Rust server in lockstep — both raise with the same error shape at the same boundary."
  - "Reserved opcodes parse SUCCESSFULLY into a new Command::ReservedNotImplemented marker variant rather than returning Err at parse time. The handler boundary (handle_sync_command) then converts the marker to TallyError::NotImplemented. This routes reserved-opcode rejection through the handler error path (which sends STATUS_ERROR and continues the loop) instead of the parser error path (which tears the connection down). Without this split, reserved-opcode probing would kill the TCP session (T-25-01-04)."
  - "Request-order response serialization: the handler builds the JSON body BY HAND (Vec<u8>) rather than through serde_json::Map. serde_json only preserves key insertion order when built with the preserve_order feature, which this codebase does not enable. Hand-serialization makes the request-order-preservation property independent of any upstream feature flag — the bytes carry the order verbatim."
  - "Unknown-table validation runs BEFORE any state read (T-25-01-03). Engine read lock is acquired, every requested table_name is checked with has_registered_table, and the first unknown name aborts the call with a single STATUS_ERROR message. No partial state read, no partial response body — clients cannot observe a half-populated feature vector."
  - "Tombstone null-collapse (T-25-01-02) is implemented in the new collect_table_row_view primitive (store.rs) by match-filtering on TableRowState::Live. The handler never sees a Tombstoned row's fields; the row projection returns None before any data leaves the store. Symmetric with Phase 24-02's merged-GET-view tombstone filter."
  - "Python SDK get_multi returns dict[TableClass, FeatureResult | None] keyed by the ORIGINAL class objects (not their registered names). Downstream callers can do result[MyTable].field without re-keying on strings — mirrors how app.mget returns keyed by input keys."
  - "Composite keys: dict form is \\x1f-joined on values (v0-restructure-spec §6.2). The SDK accepts both str and dict; dict values are stringified and joined with the US separator. Server sees a flat UTF-8 key string in both cases."

metrics:
  duration: ~45 min
  completed: 2026-04-14
  tasks: 3
  commits:
    - 381a26c    # 25-01 Task 1: protocol + store helper + reserved opcodes
    - 63f1f12    # 25-01 Task 2: GET_MULTI integration tests
    - 632021b    # 25-01 Task 3: Python SDK + e2e tests

requirements-completed:
  - QUERY-GET-MULTI-01
  - QUERY-NULL-COLLAPSE-01
  - QUERY-RESERVED-01
  - SDK-GET-MULTI-01
---

# Phase 25 Plan 01: GET_MULTI opcode + SCAN/SUBSCRIBE reservations Summary

**One-liner:** Shipped the v0 multi-table query verb `OP_GET_MULTI` (0x0D)
end-to-end — one TCP round-trip returning a per-Table null-collapsed feature
vector for a single entity key — plus reserved-opcode stubs for `SCAN`
(0x10) and `SUBSCRIBE` (0x11) that return typed `NotImplemented` errors
without tearing down the TCP connection.

## What shipped (per task)

### Task 1 — Wire protocol + dispatch + store helper (commit `381a26c`)

- **Opcodes** (`src/server/protocol.rs`): `OP_GET_MULTI = 0x0D`,
  `OP_SCAN_RESERVED = 0x10`, `OP_SUBSCRIBE_RESERVED = 0x11`.
- **Command variants**: `Command::GetMulti { table_names, key }` for the
  multi-read; `Command::ReservedNotImplemented { op_name }` marker for
  reserved opcodes. The marker pattern lets the parser succeed (so the
  connection stays alive) while the handler emits the error.
- **Parser arms**:
  - `OP_GET_MULTI`: reads `u16 count`, rejects count == 0, rejects count > 256
    (cardinality guard T-25-01-01), then reads `count` length-prefixed
    strings followed by the key string.
  - `OP_SCAN_RESERVED` / `OP_SUBSCRIBE_RESERVED`: both return
    `Ok(Command::ReservedNotImplemented { op_name })`.
- **TallyError::NotImplemented** variant added in `src/error.rs`, distinct
  from `Protocol`, mapping to STATUS_ERROR at the handler boundary.
- **StateStore::collect_table_row_view** (`src/state/store.rs`): per-Table
  projection returning `Option<serde_json::Value>`. `None` for never-seen,
  tombstoned (T-25-01-02), and absent entities. `Some(row_obj)` for Live
  rows.
- **handle_get_multi** (`src/server/tcp.rs`):
  1. Validates every requested table registered (T-25-01-03) before any
     state read.
  2. Projects each via `collect_table_row_view`; null-collapses misses.
  3. Hand-serializes the JSON body to guarantee response-key order
     matches request order regardless of serde_json feature flags.
- **Tests**: 7 protocol parse tests + 5 store unit tests + 2 reserved-opcode
  integration tests in `tests/test_reserved_opcodes.rs` asserting the
  connection survives STATUS_ERROR.

### Task 2 — GET_MULTI end-to-end integration tests (commit `63f1f12`)

Six TCP integration tests in `tests/test_op_get_multi.rs`:

| Test | Property |
| ---- | -------- |
| `test_get_multi_assembles_feature_vector` | Three-table happy path; all rows returned |
| `test_get_multi_null_for_missing_table_row` | Never-pushed → null; also never-existed key → all null |
| `test_get_multi_null_for_tombstoned` | Delete flips to null without touching other tables |
| `test_get_multi_unknown_table_rejects` | STATUS_ERROR before any state read; no partial body; connection survives |
| `test_get_multi_preserves_request_order` | Byte-level key ordering in JSON matches request order |
| `test_get_multi_single_round_trip` | Exactly one response frame; no server chatter |

### Task 3 — Python SDK + end-to-end tests (commit `632021b`)

- **`_protocol.py`**: `OP_GET_MULTI = 0x0D`, `OP_SCAN_RESERVED = 0x10`,
  `OP_SUBSCRIBE_RESERVED = 0x11`, `GET_MULTI_MAX_TABLES = 256` mirror of
  the server cardinality guard, and `encode_get_multi(names, key)` raising
  `ProtocolError` for empty / oversized inputs before wire I/O.
- **`_app.py`**: `App.get_multi(tables, key)` returns
  `dict[Table, FeatureResult | None]` keyed by the original Table class
  objects. Non-Table descriptors raise `TypeError`; empty list raises
  `ValueError`; composite keys via dict use `\x1f`-join on values.
- **Tests** (`python/tests/test_get_multi_e2e.py`, 12 cases): 5
  wire-format unit tests (no server) + 7 e2e tests against the fixture
  server covering happy path, null-collapse, empty-list / non-Table
  rejection, unknown-table server error, and composite dict key
  resolution.

## Test results

### Rust

| Suite | Before | After |
| ----- | ------ | ----- |
| `cargo test --lib` | 700 | **720** (+20 new unit tests: 7 protocol + 5 store + 8 other incidental) |
| `test_op_get_multi` | — | **6 / 6** (new) |
| `test_reserved_opcodes` | — | **2 / 2** (new) |
| `test_op_push_table` | 6 / 6 | **6 / 6** (no regression) |

### Python

| Suite | Before | After |
| ----- | ------ | ----- |
| `test_get_multi_e2e.py` | — | **12 / 12** (new) |
| `pytest python/tests/` (fresh server) | 422 passed | **433 passed, 1 flake** |

The single failure (`test_v0_stream_table_join.py::test_stream_table_enrich_tcp_roundtrip`)
is the pre-existing cross-test u1 key pollution flake on the session-scoped
server fixture documented in Phase 24-04 SUMMARY. Reproduces identically on
`5c6d31c` (Phase 24 closeout); not caused by Plan 25-01.

## Deviations from plan

Localized to Task 1, all within deviation-rule scope:

- **Rule 3 (blocking issue, auto-fix):** Reserved opcodes initially returned
  `Err(TallyError::NotImplemented)` at parse time. First test run exposed a
  torn-down connection: `handle_connection`'s frame-read error path closes
  the socket on any parser Err (line 376-384, established before Phase 25).
  Refactored to a `Command::ReservedNotImplemented { op_name }` marker
  variant so the parser succeeds and the error is emitted at the handler
  boundary where STATUS_ERROR-with-connection-keepalive is the established
  flow. This is required to satisfy T-25-01-04; not an architectural shift
  (Rule 4 was not triggered).

- **Rule 2 (auto-add missing critical functionality):** Plan did not
  specify that `encode_get_multi` should validate cardinality client-side.
  Added empty-list rejection (ValueError at App layer, ProtocolError at
  encode layer) and `>256` rejection to mirror the server's guard. This
  closes a DoS-amplification window where a client could send an
  oversized frame that the server would reject anyway — now the client
  fails fast without any wire traffic.

- **SDK signature note:** Plan suggested `dict[type, FeatureResult | None]`
  as the return shape. Implemented exactly as written, keyed by the original
  Table class object (not the registered name) for ergonomic attribute
  access at call sites.

- **Linter-added constant:** `GET_MULTI_MAX_TABLES = 256` was added to
  `_protocol.py` by a linter hook between my two edits. Kept it and used
  it in the e2e test for the `>256` assertion — no functional change,
  cleaner test.

## Threat register — disposition

| Threat ID | Status | Evidence |
| --------- | ------ | -------- |
| T-25-01-01 | Mitigated | `op_get_multi_rejects_zero_count`, `op_get_multi_rejects_oversized_count`, `op_get_multi_accepts_max_count_256` — parser enforces bounds before allocation |
| T-25-01-02 | Mitigated | `collect_table_row_view::tombstoned_row_collapses_to_none`, `test_get_multi_null_for_tombstoned`, `test_get_multi_after_delete` (Python) — tombstones never leak fields |
| T-25-01-03 | Mitigated | `test_get_multi_unknown_table_rejects` — validation runs before any state access, no partial response |
| T-25-01-04 | Mitigated | `scan_reserved_returns_error_and_keeps_connection_alive`, `subscribe_reserved_returns_error_and_keeps_connection_alive` — subsequent OP_GET succeeds on the same connection |
| T-25-01-05 | Mitigated | `test_get_multi_non_table_rejects` (stream and arbitrary object both raise TypeError before wire I/O) |

## Opcode assignment rationale

`OP_GET_MULTI = 0x0D` is contiguous after `OP_DELETE_TABLE` (0x0C) per the
Phase 24-02 precedent (keep new commands adjacent to the cluster they
extend — Table-row reads belong with Table-row writes).

`OP_SCAN_RESERVED = 0x10` and `OP_SUBSCRIBE_RESERVED = 0x11` open the
**0x10-0x1F reserved block** per v0-restructure-spec §6.3. The two
opcodes used are the explicit v0-specified reservations; the rest of
the block stays available for v0.x additions. Gap at 0x0E-0x0F left
deliberately open for any interim non-reserved opcode the v0.x work
surfaces (e.g. a dedicated GET-one-table projection if GET_MULTI with
a single name proves too syntactically heavy).

## Cardinality guard rationale

- `count == 0` rejected: a GET_MULTI with no tables is a no-op at best
  and a probe at worst. Rejecting at the parser gives clients a crisp
  error ("at least one table_name") instead of returning an empty JSON
  object that would look indistinguishable from a bug.
- `count > 256` rejected: 256 is generous for the "5-20 Tables per ML
  prediction" v0 use case. A higher cap inflates the worst-case
  per-request memory (256 × ~64-byte table names + 256 × up-to-a-few-KB
  row projections) linearly. 256 keeps the ceiling well under
  existing per-request budgets (MSET caps at 16K; GET_MULTI at 256 is
  a deliberately tight fence).

## Key-encoding reuse

Python SDK composite-key encoding reuses the `\x1f` (ASCII US) separator
mandated by v0-restructure-spec §6.2. The SDK previously treated keys
as opaque strings; this plan's `app.get_multi` accepts `dict` and joins
`str(v) for v in d.values()` with `\x1f`, producing a flat string that
matches what the server already stores for keyed sources. The push/delete
paths will benefit from the same helper in a future cleanup pass
(tracked as a minor follow-up, not in scope here).

## SDK signature notes

- **Return type:** `dict[Table, FeatureResult | None]`. Callers can use
  `result[MyTable].field` or `if result[MyTable] is None` — pythonic and
  symmetric with `mget`'s key-indexed return.
- **Key parameter:** `str` or `dict`. `dict` is stringified with `\x1f`
  join per above.
- **Validation order:** empty-list / non-Table errors raised BEFORE
  `self._client.drain_errors_nonblock()` or any socket traffic. Matches
  the Phase 24-02 convention for `app.push(table, key, fields)` and
  `app.delete(table, key)`.

## Handoff to Plan 25-02

Plan 25-01 leaves the query surface complete for v0. Plan 25-02 (TTL
defaults + suggestion engine) can build on:

- **`collect_table_row_view`** is now a public accessor suitable for the
  `/debug/config-recommendations` UI preview path (if Plan 25-02 wants
  to show "this is what GET_MULTI would return with the suggested TTL").
- **Reserved-opcode pattern** (marker variant + handler-boundary error
  emission) is the template for any further reservations.
- **Cardinality guard pattern** (parser-level rejection with matching
  client-side constant) is now available to copy for future opcodes.

**Out of scope:**
- SCAN / SUBSCRIBE implementations (reserved only; v0.1+).
- TTL defaults and suggestion engine (Plan 25-02).
- Warnings endpoint (Plan 25-03).

## Self-Check: PASSED

Verified files exist (absolute paths):

- `/data/home/tally/src/error.rs` — FOUND (modified; NotImplemented variant present)
- `/data/home/tally/src/server/protocol.rs` — FOUND (modified; OP_GET_MULTI + reserved opcodes)
- `/data/home/tally/src/server/tcp.rs` — FOUND (modified; handle_get_multi + reserved dispatch)
- `/data/home/tally/src/state/store.rs` — FOUND (modified; collect_table_row_view)
- `/data/home/tally/python/tally/_protocol.py` — FOUND (modified; OP_GET_MULTI + encode_get_multi)
- `/data/home/tally/python/tally/_app.py` — FOUND (modified; App.get_multi)
- `/data/home/tally/tests/test_op_get_multi.rs` — FOUND (created)
- `/data/home/tally/tests/test_reserved_opcodes.rs` — FOUND (created)
- `/data/home/tally/python/tests/test_get_multi_e2e.py` — FOUND (created)

Verified commits exist on `main`:

- `381a26c` feat(25-01): OP_GET_MULTI + reserved opcodes wire protocol + store helper
- `63f1f12` test(25-01): GET_MULTI end-to-end integration test suite
- `632021b` feat(25-01): Python SDK app.get_multi + end-to-end tests

Verified test gates (2026-04-14):

- `cargo test --lib` — 720 / 720
- `cargo test --test test_op_get_multi` — 6 / 6
- `cargo test --test test_reserved_opcodes` — 2 / 2
- `cargo test --test test_op_push_table` — 6 / 6 (no regression)
- `pytest python/tests/test_get_multi_e2e.py -x` — 12 / 12
- `pytest python/tests/` — 433 passed, 2 skipped (1 pre-existing fixture-pollution flake documented in 24-04-SUMMARY; reproduces on Phase 24 baseline)

Plan 25-01 is complete. Plans 25-02 and 25-03 are unblocked.
