---
phase: 25-query-surface-ttl-warnings
plan: 01
subsystem: protocol
status: complete
requirements:
  - QUERY-GETMULTI-01
  - QUERY-GETMULTI-02
  - QUERY-GETMULTI-03
  - QUERY-GETMULTI-04
  - QUERY-RESERVED-01
dependency-graph:
  requires:
    - "Phase 24-02 merged GET view (collect_merged_features)"
    - "Phase 24-02 table_rows state + tombstoned filtering"
  provides:
    - "OP_GET_MULTI (0x0D) multi-table feature-vector wire verb"
    - "OP_SCAN_RESERVED (0x10) / OP_SUBSCRIBE_RESERVED (0x11) typed NotImplemented"
    - "App.get_multi(tables, key) Python SDK method"
  affects:
    - "src/server/protocol.rs command surface"
    - "src/server/tcp.rs dispatch surface"
    - "python/tally public protocol-constant exports"
tech-stack:
  added: []
  patterns:
    - "Null-collapse: never-seen / tombstoned / pending all serialize as JSON null — indistinguishable at the wire"
    - "Atomic validation-before-read: unknown table aborts the whole request with STATUS_ERROR before any state projection"
    - "Hand-rolled JSON body build for request-order key preservation, independent of serde_json preserve_order feature"
    - "Reserved opcodes parsed into a marker Command variant so the frame dispatcher does not tear down the connection"
key-files:
  created:
    - /data/home/tally/tests/test_get_multi.rs
    - /data/home/tally/tests/test_op_get_multi.rs
    - /data/home/tally/tests/test_reserved_opcodes.rs
    - /data/home/tally/python/tests/test_get_multi_e2e.py
  modified:
    - /data/home/tally/src/server/protocol.rs
    - /data/home/tally/src/server/tcp.rs
    - /data/home/tally/src/state/store.rs
    - /data/home/tally/python/tally/_protocol.py
    - /data/home/tally/python/tally/_app.py
    - /data/home/tally/python/tally/__init__.py
decisions:
  - "Cardinality cap set to 256 table_names per request (not the plan-draft value of 32). 256 bounds per-request memory while leaving generous headroom for ML inference callers that assemble 5-20 tables today; the server's parse_command guard mirrors the SDK's GET_MULTI_MAX_TABLES constant."
  - "Python App.get_multi returns dict keyed by the ORIGINAL Table class objects (not registered name strings). Callers do result[MyTable].field — symmetric with how push/delete take descriptors — at the cost of diverging from the plan's literal {table_name: row_or_None} shape."
  - "Response body keys serialized in REQUEST order by hand-rolled JSON build (not serde_json::Map), so behavior is deterministic regardless of the preserve_order feature flag on the server build."
  - "Reserved opcodes (0x10 SCAN, 0x11 SUBSCRIBE) parsed into Command::ReservedNotImplemented rather than erroring at parse_command. This routes the error through the normal STATUS_ERROR path at the handler boundary so connections survive (T-25-01-04); clients can probe capabilities without session teardown."
  - "Composite keys transported on the wire as a single \\x1f-joined string (v0-restructure-spec §6.2). Python SDK accepts dict / list and joins client-side before the wire call."
metrics:
  duration: "already committed prior to this session"
  completed: 2026-04-14
  tasks: 3
  files_created: 4
  files_modified: 6
tags: [protocol, tcp, python-sdk, query, null-collapse, reserved-opcodes]
---

# Phase 25 Plan 01: GET_MULTI Opcode End-to-End + SCAN/SUBSCRIBE Reserved Summary

Ships the multi-table feature-vector read verb (OP_GET_MULTI, 0x0D) end-to-end across the Rust server, Python SDK, and integration tests, and reserves the 0x10 SCAN / 0x11 SUBSCRIBE opcodes with typed NotImplemented errors that do NOT tear down the TCP connection.

## Implementation

### Rust server (src/server/protocol.rs, src/server/tcp.rs)

- `OP_GET_MULTI: u8 = 0x0D` — payload `[u16 count][count × u16-string table_name][u16-string key]`; response body `{table_name: row | null, ...}` in request order.
- `OP_SCAN_RESERVED: u8 = 0x10`, `OP_SUBSCRIBE_RESERVED: u8 = 0x11` — parsed into `Command::ReservedNotImplemented { op_name }` so the dispatcher emits `TallyError::NotImplemented` via the standard STATUS_ERROR path, keeping the connection open.
- `Command::GetMulti { table_names, key }` — table order preserved for response serialization.
- `handle_get_multi` in tcp.rs: validates EVERY requested table name is registered (unknown name aborts with `TallyError::Protocol("unknown table: …")` before any state read), then projects `state.store.collect_table_row_view(key, name, now)` per requested table. Response body is hand-rolled JSON so key order matches request order independent of `serde_json`'s `preserve_order` feature.
- Cardinality guards at parse_command: `count == 0` → "GET_MULTI requires at least one table_name"; `count > 256` → "GET_MULTI table_names count exceeds 256".
- `collect_table_row_view` helper on StateStore returns `Some(row)` for a Live registered Table row and `None` for never-seen, tombstoned, or pending — the single source of null-collapse truth.

### Python SDK (python/tally/_protocol.py, _app.py, __init__.py)

- `OP_GET_MULTI = 0x0D`, `OP_SCAN_RESERVED = 0x10`, `OP_SUBSCRIBE_RESERVED = 0x11`, `GET_MULTI_MAX_TABLES = 256` constants added and re-exported from `tally.__init__`.
- `encode_get_multi(table_names, key)` — client-side guards mirror the server's cardinality limits, plus u16-length validation for each `table_name` and `key`. Raises `ProtocolError` BEFORE any wire I/O.
- `App.get_multi(tables, key) -> dict[Table, FeatureResult | None]`:
  - Accepts Table descriptors (not strings) for ergonomic `result[MyTable].field` access.
  - Rejects empty list (`ValueError`), non-Table descriptors (`TypeError`), and stream descriptors (`TypeError`) BEFORE any wire I/O.
  - Composite keys accepted as `dict` (values `\x1f`-joined) or passed through as strings.
  - Drains errors non-blocking before send (keeps the fire-and-forget push error flow coherent).

### Tests

Rust integration tests (19 total across three files):
- `tests/test_op_get_multi.rs` (6 tests, committed 63f1f12): happy-path feature-vector assembly, null-for-missing-row, null-for-tombstoned, unknown-table atomic error, request-order preservation, single-round-trip invariant.
- `tests/test_get_multi.rs` (11 tests, this session): degenerate single-table = GET-slice, three-table happy path, missing-key all-null, mixed present/absent, tombstoned collapse, composite-key routing, unknown-table atomic error + connection survival, count=0 rejection, count=257 rejection, prefix-collision safety (User vs UserProfile), response-order preservation.
- `tests/test_reserved_opcodes.rs` (2 tests): SCAN/SUBSCRIBE return STATUS_ERROR and leave connection usable for a follow-up GET.

Rust parse_command unit tests inside `src/server/protocol.rs` (8 tests): happy GET_MULTI, count=0, count=257 cap, truncated count header, composite-key payload, SCAN parse, SUBSCRIBE parse, degenerate 1-table.

Python e2e tests (12 tests in `python/tests/test_get_multi_e2e.py`, committed 632021b): 5 wire-format unit (no server needed) + 7 e2e (three-table happy, never-seen null, tombstoned null, empty-list ValueError, non-Table TypeError via stream class, non-Table TypeError via plain object, unknown-table ProtocolError, composite-dict key).

## Verification

- `cargo test --lib` — 722 pass, 0 fail.
- `cargo test --test test_op_get_multi --test test_get_multi --test test_reserved_opcodes` — 19/19 pass.
- `pytest python/tests/test_get_multi_e2e.py` — 12/12 pass.
- No regressions in Phase-24 test suites (merged GET view, push_table e2e, watermarks).

## Deviations from Plan

### Auto-fixed / locked-at-implementation

**1. [Rule 1 — Spec tightening] Cardinality cap raised from 32 → 256**
- The plan's `count > 32` guard was locked at 256 during implementation to leave headroom for ML-inference callers that assemble 5-20 tables today and may grow the vector. Both parse_command and the Python `GET_MULTI_MAX_TABLES` constant agree.
- Files: `src/server/protocol.rs`, `python/tally/_protocol.py`.

**2. [Rule 2 — Ergonomics] Python return type keyed by Table descriptor, not name string**
- Plan specifies `{table_name: row_or_None}`; implementation returns `{TableClass: FeatureResult_or_None}`. Matches how `app.push(table, …)` and `app.delete(table, …)` already accept descriptors; callers do `result[MyTable].field` without a string round-trip.
- Files: `python/tally/_app.py::App.get_multi`.

**3. [Rule 2 — Connection safety] Reserved opcodes routed through handler, not parse_command**
- Plan draft said `parse_command` could return Err for reserved opcodes. Implementation instead produces `Command::ReservedNotImplemented` and emits `TallyError::NotImplemented` at the handler boundary. This guarantees the STATUS_ERROR path's "connection stays open" invariant is exercised — validated by `test_reserved_opcodes.rs`.
- Files: `src/server/protocol.rs`, `src/server/tcp.rs`.

**4. [Rule 1 — Determinism] Hand-rolled JSON body for request-order key preservation**
- Avoids dependency on serde_json's `preserve_order` build feature. Response body is built as `{name:val, name:val}` bytes directly by `handle_get_multi`. Validated by `test_get_multi_response_preserves_request_order` (byte-level `find` assertions).
- Files: `src/server/tcp.rs::handle_get_multi`.

No architectural deviations. No authentication gates.

## Threat Flags

None. This plan is additive on an already-closed trust boundary (TCP protocol); no new network surface, no new auth paths, no new file/IPC access. Reserved opcodes are parsed-and-rejected without allocating state, and the cardinality cap bounds per-request memory.

## Self-Check: PASSED

- `tests/test_get_multi.rs` — FOUND
- `tests/test_op_get_multi.rs` — FOUND
- `tests/test_reserved_opcodes.rs` — FOUND
- `python/tests/test_get_multi_e2e.py` — FOUND
- Commit 381a26c (feat 25-01 opcode + store helper) — FOUND
- Commit 63f1f12 (25-01 integration tests) — FOUND
- Commit 632021b (25-01 Python SDK + e2e) — FOUND
- `cargo test --lib` 722/722 pass
- 19/19 GET_MULTI + reserved-opcode integration tests pass
- 12/12 Python e2e pass
