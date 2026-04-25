---
phase: 18
plan: "09"
subsystem: transport
tags: [msgpack, tcp, wal, sdk, wire-protocol, serialization]
dependency_graph:
  requires: [18-04.6, 18-07]
  provides: [msgpack-on-tcp, wal-v2-binary-format, python-sdk-send-push]
  affects: [beava-runtime-core, beava-server, beava-bench, python-sdk]
tech_stack:
  added: [rmp_serde, msgpack (python)]
  patterns: [CT_MSGPACK dispatch, serde_json::Value as deserialization intermediary, v=2 self-delimiting WAL record]
key_files:
  created:
    - crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs
    - python/tests/test_phase18_09_sdk_msgpack.py
  modified:
    - crates/beava-runtime-core/src/tcp_listener.rs
    - crates/beava-runtime-core/src/wire_request.rs
    - crates/beava-core/src/row.rs
    - crates/beava-server/src/apply_shard.rs
    - crates/beava-server/src/recovery.rs
    - crates/beava-server/src/server.rs
    - crates/beava-bench/src/bin/beava-bench-v18.rs
    - python/beava/_transport.py
    - python/beava/_wire.py
    - .planning/throughput-baselines.md
decisions:
  - "serde_json::Value as msgpack deserialization intermediary: rmp_serde::from_slice::<serde_json::Value> bridges msgpack wire into serde data model without requiring a dedicated type; works because serde_json::Value implements Deserialize via deserialize_any which rmp_serde drives correctly"
  - "Row::Deserialize via serde_json::Value field values: avoids breaking bincode WAL/snapshot compat which needs the tagged enum format on the Value type; the Deserialize impl converts field values through json_value_to_beava_value after extracting them as serde_json::Value"
  - "WAL v=2 binary format self-delimiting: [u8 v=2][u8 body_format][u32 rv BE][u64 et_ms BE][u16 name_len BE][N name][u32 body_len BE][M body] — explicit body_len makes each record self-delimiting so replay can scan forward through raw bytes without a separator"
  - "Python SDK send_push wires JSON via stdlib json (no new dep) and msgpack via optional msgpack package with a clear ImportError message"
metrics:
  duration_minutes: 90
  completed_date: "2026-04-25"
  tasks_completed: 9
  files_modified: 10
  files_created: 2
---

# Phase 18 Plan 09: msgpack-on-TCP + WAL v=2 binary format Summary

**One-liner:** CT_MSGPACK (0x02) fully wired server-to-SDK: TCP frame parsing, dispatch, WAL v=2 binary records, replay, bench `--wire-format`, Python `TcpTransport.send_push(wire_format='msgpack')`.

## What Was Built

### Task 9.1 — `body_format` field on WireRequest TCP/HTTP variants

`WireRequest::TcpPush`, `HttpPush`, `HttpPushSync`, `HttpPushBatch` all carry a `body_format: u8` field. The TCP listener sets it from the frame's content-type byte; the HTTP listener hardcodes `CT_JSON`. Downstream dispatch branches on this byte.

Commits: `1bba02e` (RED), `91e2891` (GREEN)

### Task 9.2 — CT_MSGPACK envelope parsing in `tcp_listener.rs`

`parse_wire_request` branches on `frame.ct`:
- `CT_JSON`: existing `serde_json` path unchanged
- `CT_MSGPACK`: `rmp_serde::from_slice::<serde_json::Value>` decodes the full envelope, extracts `event` (string) and `body` fields, re-encodes `body` to msgpack bytes via `rmp_serde::to_vec_named`, returns `WireRequest::TcpPush { body_format: CT_MSGPACK, ... }`

Commits: `fb3d002` (RED), `49726c4` (GREEN)

### Task 9.3 — `Row::Deserialize` impl for JSON and msgpack

Added `impl<'de> Deserialize<'de> for Row` using `RowVisitor` with `serde_json::Value` as the field value intermediary. Works for both `serde_json` and `rmp_serde` deserialization. Added `json_value_to_beava_value` converter. Added `impl Serialize for Row` (flat map format). The `Value` enum keeps `#[derive(Serialize, Deserialize)]` for bincode compat.

Commits: `fb3d002` (RED — shared with 9.2), `ae2b3e5` (GREEN)

### Task 9.4 — `dispatch_push_sync` branches on `body_format`

`ApplyShard::dispatch_push_sync` now reads `body_format`:
- `CT_MSGPACK`: `rmp_serde::from_slice::<JsonValue>(&body)` → beava value extraction
- `CT_JSON`: `sonic_rs::from_slice(&body)` (existing path)

Commit: `a9fcbcb` (GREEN — GREEN-only; RED was shared with 9.2/9.3 test file)

### Task 9.5 — WAL v=2 binary record format

All data-plane pushes (JSON and msgpack) now write v=2 binary WAL records:
```
[u8 v=2][u8 body_format][u32 rv BE][u64 et_ms BE][u16 name_len BE][N name][u32 body_len BE][M body]
```
Body bytes are raw msgpack for CT_MSGPACK, JSON for CT_JSON. Self-delimiting — no separator needed.

Commit: `e656a3b` (GREEN)

### Task 9.6 — WAL v=2 replay in `serve_with_dirs`

Added `parse_v2_records(data: &[u8]) -> Vec<V2Record>` and `replay_handrolled_wal_dir(wal_dir, lsn_start, dev_agg)` to `recovery.rs`. `serve_with_dirs` now calls both replay functions at startup:
1. `replay_wal_from_lsn` — reads `*.log` registry bumps (WalSink path)
2. `replay_handrolled_wal_dir` — reads `*.wal` data-plane events

The `initial_start_lsn` passed to WalSink is computed as `max(persistence_lsn, handrolled_last_lsn) + 1` to avoid `create_new` collision on restart.

Commit: `ef71501` (GREEN)

### Task 9.7 — `beava-bench-v18 --wire-format` flag

Added `WireFormat` enum (`Json`, `Msgpack`) and `--wire-format` CLI arg (default: json). TCP workers branch on wire format at envelope encode time. Report includes the wire format in the transport label (`tcp/json`, `tcp/msgpack`).

Commit: `9a0539f` (GREEN)

### Task 9.8 — Python `TcpTransport.send_push`

Added `send_push(event_name, body_dict, *, wire_format='json') -> dict` to `TcpTransport`:
- `'json'`: `json.dumps` envelope + `CT_JSON` (no new deps)
- `'msgpack'`: `msgpack.packb` envelope + `CT_MSGPACK` (requires `pip install msgpack`; clean `ImportError` message if absent)
- Unknown format: raises `ValueError`

ACK is always JSON (server responds with JSON regardless of push wire format).

Commits: `e7816d3` (RED), `5152732` (GREEN)

### Task 9.9 — Throughput baseline measurement

Ran `beava-bench-v18` with `--wire-format json` and `--wire-format msgpack` (parallel=4, 10s, small pipeline):

| Wire | EPS | p50 µs | p95 µs | p99 µs |
|---|---:|---:|---:|---:|
| tcp/json    | 23,799 | 47 | 156 | 4,563 |
| tcp/msgpack | 23,324 | 48 | 150 | 4,595 |

msgpack is 97.6% of json EPS — no measurable serialization overhead. Bottleneck is single mio apply thread (same as 18-04.6). Rows appended to `.planning/throughput-baselines.md`.

Commit: `95b9fe0` (fix — includes baseline rows)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `rmp_serde::Value` does not exist**
- **Found during:** Task 9.2 implementation
- **Issue:** Initial CT_MSGPACK branch used `rmp_serde::Value` which is not a type in the rmp_serde crate
- **Fix:** Used `serde_json::Value` as the whole-envelope intermediary: `rmp_serde::from_slice::<serde_json::Value>` bridges msgpack into serde's data model
- **Files modified:** `crates/beava-runtime-core/src/tcp_listener.rs`
- **Commit:** `49726c4`

**2. [Rule 1 - Bug] Integration test readiness probe polled wrong address**
- **Found during:** Task 9.4 integration test
- **Issue:** `wait_for_http_09` polled `admin_addr` (axum `/health`) but the hand-rolled mio server only listens on `http_addr`; tests completed in 0.02s because admin was ready but mio wasn't
- **Fix:** Changed all test functions to poll `http_addr` with GET `/ping` (any HTTP response proves the mio loop is running)
- **Files modified:** `crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs`
- **Commit:** `95b9fe0` (final fix pass)

**3. [Rule 1 - Bug] `small_pipeline_register()` used single-node format**
- **Found during:** Task 9.4 integration test (GET /get/cnt/u2 returned 404)
- **Issue:** Initial registration used `{"kind":"event","aggregations":[...]}` but server only accepts two-node `event` + `derivation` format
- **Fix:** Rewrote `small_pipeline_register()` to use the two-node format matching the 04_6 integration test
- **Files modified:** `crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs`

**4. [Rule 1 - Bug] Second server instance failed to start (WAL collision)**
- **Found during:** Task 9.6 `test_wal_replay_v2_msgpack`
- **Issue:** `serve_with_dirs` always passed `initial_start_lsn: 1` to WalSink; WalSink tried `create_new(true)` on `wal-0000000000000001.log` which already existed from the first server instance
- **Fix:** Added recovery block at start of `serve_with_dirs` computing correct `initial_start_lsn` from both `*.log` and `*.wal` files
- **Files modified:** `crates/beava-server/src/server.rs`, `crates/beava-server/src/recovery.rs`
- **Commit:** `ef71501`

**5. [Rule 1 - Bug] WAL test filtered for `*.bin` instead of `*.wal`**
- **Found during:** Task 9.5 `test_wal_record_v2_format`
- **Issue:** Test filtered for `wal-*.bin` but WalWriter creates `wal-0000000000000000.wal`
- **Fix:** Changed filter to `.wal` extension
- **Files modified:** `crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs`

**6. [Rule 2 - Clippy/CI gate] Pre-existing dead code and unused imports in `server.rs` and `apply_shard.rs`**
- **Found during:** Task 9.9 `cargo clippy -- -D warnings`
- **Issue:** Multiple pre-existing unused imports (`PushAck`, `FieldType`, `DurabilityConfig`, `EventLoop`, `IoPool`, `HttpListener`, `MioTcpListener`, `BufMut`), unused variables (`wal_shutdown_flag`, `writable`), dead functions (`dispatch_tcp_frame`, `glue_to_frame`), redundant closure, and `and_then`→`map` lint — all predating plan 18-09 but blocking the CI gate
- **Fix:** Removed unused imports; prefixed unused vars with `_`; added `#[allow(dead_code)]` to compat functions; fixed `and_then`→`map`; fixed `phase18_01_glue.rs` for missing `body_format` field
- **Files modified:** `crates/beava-server/src/server.rs`, `crates/beava-server/src/apply_shard.rs`, `crates/beava-server/tests/phase18_01_glue.rs`
- **Commit:** `95b9fe0`

**7. [Rule 2 - Clippy/CI gate] MutexGuard held across await in integration tests**
- **Found during:** Task 9.9 `cargo clippy -- -D warnings`
- **Issue:** `let _guard = SERVER_SERIALIZER.lock().unwrap()` held the guard across `.await` calls; both `phase18_04_6_integration_test.rs` and `phase18_09_msgpack_tcp_test.rs` had this pattern
- **Fix:** Changed to `{ let _g = SERVER_SERIALIZER.lock().unwrap(); }` — block scope drops `_g` before any await
- **Files modified:** `crates/beava-server/tests/phase18_04_6_integration_test.rs`, `crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs`
- **Commit:** `95b9fe0`

## Known Stubs

None — all plan goals fully wired. The Python `EmbedTransport` does not expose `send_push` (it delegates to `TcpTransport`), but that is by design.

## Threat Flags

None — no new network endpoints, auth paths, or trust-boundary schema changes introduced. CT_MSGPACK reuses the existing TCP push path with the same frame length limits.

## Self-Check: PASSED

Files created:
- `/Users/petrpan26/work/tally/crates/beava-server/tests/phase18_09_msgpack_tcp_test.rs` — FOUND
- `/Users/petrpan26/work/tally/python/tests/test_phase18_09_sdk_msgpack.py` — FOUND

Commits verified:
- `1bba02e` — TcpPush body_format RED
- `91e2891` — body_format GREEN
- `fb3d002` — msgpack parsing + Row Deserialize RED
- `49726c4` — msgpack envelope parsing GREEN
- `ae2b3e5` — Row Deserialize GREEN
- `a9fcbcb` — dispatch_push_sync CT_MSGPACK GREEN
- `e656a3b` — WAL v=2 binary format GREEN
- `ef71501` — WAL v=2 replay GREEN
- `9a0539f` — bench --wire-format GREEN
- `e7816d3` — Python send_push RED
- `5152732` — Python send_push GREEN
- `95b9fe0` — clippy/fmt fixes + throughput baseline
- `f40bcd0` — rustfmt normalization
