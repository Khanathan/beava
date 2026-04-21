---
phase: 59
plan: 02
subsystem: server / wire-format handshake
tags:
  - tpc-perf-09
  - wave-2
  - op-negotiate-wire-format
  - handshake
  - capability-bits
  - wire-version-tag
requires:
  - phase-59-wave-1 (Bytes passthrough + OP_PUSH dual-format auto-detect live; commits acffc40, f1a23d7, d9688ca)
  - 59-CONTEXT.md D-B1 (opcode 0x18, wire format, capability bits)
provides:
  - pub const OP_NEGOTIATE_WIRE_FORMAT: u8 = 0x18
  - pub const WIRE_VERSION_TAG_SERVER: u16 = 2
  - Command::NegotiateWireFormat { client_bits, client_version }
  - parse_command OP_NEGOTIATE_WIRE_FORMAT arm with 6-byte truncation guard
  - encode_negotiate_response_body(server_bits, server_version) -> Vec<u8>
  - handle_sync_command dispatch → STATUS_OK + [u32 BE bits][u16 BE ver]
affects:
  - Wave 3 (59-03) Python SDK now has a real server opcode to negotiate
    against — TallyClient.negotiate_wire_format() wraps this surface.
  - Wave 4 (59-04) perf gate: handshake surface is only on connect, not
    the hot path — zero per-event cost. Auto-detect (D-B2, already in
    Wave 1) also bypassed when clients pre-negotiate.
tech-stack:
  added: []
  patterns:
    - "Dispatch-via-handle_sync_command for stateless-ack opcodes (Flush/Mset precedent)"
    - "encode_*_response_body returns just the body; outer match wraps in encode_response(STATUS_OK, ..) — mirrors Phase 55 source_lsn echo pattern"
    - "Server ignores client_bits on the wire (spoof-safe T-59-02-01); client learns server support via the echo"
key-files:
  created:
    - .planning/phases/59-binary-wire-format-for-push/59-02-SUMMARY.md
  modified:
    - src/server/protocol.rs (const + Command variant + parse arm + helper + 3 unit tests)
    - src/server/tcp.rs (handle_sync_command dispatch arm)
    - tests/wire_negotiation_handshake.rs (#[ignore = "59-W2"] removed)
decisions:
  - "Dispatch lives in handle_sync_command (not inline in handle_connection as the plan suggested). Rationale: Flush/Mset precedent — handle_sync_command already returns Vec<u8> body that handle_connection's outer match wraps via encode_response(STATUS_OK, &payload). Adding an inline `continue` branch in handle_connection would duplicate writer/flush plumbing. The inner tight-loop (line 1453) + outer loop (line 1635) both have `other => handle_sync_command(other, &state).await.map(Some)` catch-alls, so NegotiateWireFormat falls through cleanly. Smaller diff, matches established opcode-dispatch pattern."
  - "`encode_negotiate_response_body` renamed vs plan's `encode_negotiate_response`. Body-only helper fits the outer wrap-with-encode_response pattern (encode_response already owns the length+status framing). Name disambiguates: plan's `encode_negotiate_response` would be full-frame including length header, which the caller already produces via encode_response."
  - "D-B2 auto-detect was ALREADY wired in Wave 1 via parse_push_body (src/server/protocol.rs:909). Wave 1's Rule-1 deviation delivered the behavior ahead of Wave 2's planned schedule. Wave 2 action §4 (OP_PUSH auto-detect) therefore a no-op for this wave — verified by inspecting OP_PUSH, OP_PUSH_ASYNC, and OP_PUSH_BATCH arms: OP_PUSH + OP_PUSH_ASYNC both call parse_push_body (binary-first with JSON fallback on `{`/`[` first byte); OP_PUSH_BATCH still calls decode_event_binary directly — intentional per the plan's 'auto-detect lives at parse_command entry' model vs per-event in the batch inner loop (batch events are always binary by construction; no JSON fallback on the inner loop). Decision: do not add per-event JSON fallback inside OP_PUSH_BATCH since there's no legacy Python SDK path that emits JSON as a batch element."
  - "Inline unit tests added to src/server/protocol.rs::tests (3 new): happy path with 6-byte payload; truncated 3-byte payload returns Protocol error with 'truncated' keyword; encode_negotiate_response_body roundtrip. Total lib count: 822 → 825."
  - "Server advertises SERVER_SUPPORTED_BITS unconditionally — does NOT mask by client_bits. Rationale from CONTEXT.md D-B1: 'server echoes ONLY actually-supported bits in its response so a negotiation round-trip tells the client exactly what the server supports.' Masking by client_bits would defeat capability discovery for clients that forget to set bits they care about."
metrics:
  duration: ~10min
  completed: 2026-04-21
  tasks: 1
  commits: 1
  files_created: 1
  files_modified: 3
  ignore_markers_removed: 1  # 59-W2 in wire_negotiation_handshake.rs
  lib_tests_passing: "825/0/35"
---

# Phase 59 Plan 02: OP_NEGOTIATE_WIRE_FORMAT Handshake Summary

Wave 2 wires the `OP_NEGOTIATE_WIRE_FORMAT = 0x18` handshake opcode so clients
can explicitly learn server capabilities (bit 0 = `WIRE_BINARY_PASSTHROUGH`) +
the server's wire-version tag (= 2). The dual-format auto-detect on OP_PUSH
(D-B2) was already wired in Wave 1 via `parse_push_body` — Wave 2 is purely
net-additive: 1 opcode + 1 Command variant + 1 parse arm + 1 dispatch arm +
1 encode helper + 3 unit tests.

## Wire Round-Trip Evidence (GREEN test)

Captured from `tests/wire_negotiation_handshake.rs::op_negotiate_wire_format_round_trips_capability_bits`:

```
# Request bytes (client → server):
  [u32 BE 0x00000007]         frame_len (= 1 opcode + 6 body)
  [u8 0x18]                   opcode = OP_NEGOTIATE_WIRE_FORMAT
  [u32 BE 0x00000001]         client_cap_bits = WIRE_BINARY_PASSTHROUGH
  [u16 BE 0x0002]             client_version_tag = 2

# Response bytes (server → client):
  [u32 BE 0x00000007]         resp_len (= 1 status + 6 body)
  [u8 0x00]                   STATUS_OK
  [u32 BE 0x00000001]         server_cap_bits = WIRE_BINARY_PASSTHROUGH
  [u16 BE 0x0002]             server_version_tag = 2
```

## D-B2 Dual-Format Auto-Detect Behavior Table (already live from Wave 1)

| OP_PUSH body first byte | parse_push_body behavior                          | Shard-side |
|-------------------------|---------------------------------------------------|------------|
| `0x00..0x0F` (typical binary u16 field_count high byte) | binary decode via decode_event_binary | PayloadFmt::Binary (Bytes passthrough) |
| `0x7B` (`{`)            | JSON decode via serde_json::from_slice             | PayloadFmt::Json (legacy re-serialize path) |
| `0x5B` (`[`)            | JSON decode via serde_json::from_slice             | PayloadFmt::Json |
| other `0x10..0xFF` non-JSON | binary decode attempt; error if malformed      | error bubbles as STATUS_ERROR |
| empty                   | decode_event_binary sees 0-byte buffer → truncation error | STATUS_ERROR |

Wave 2 does NOT change this table — it was written at Wave 1 landing.

## Test Disposition Matrix

| Test                                                  | Wave 1 status             | Wave 2 status                     |
|-------------------------------------------------------|---------------------------|-----------------------------------|
| `tests/wire_negotiation_handshake`                    | `#[ignore = "59-W2"]`     | **GREEN** (1/0/0) — marker removed |
| `tests/binary_push_bytes_passthrough`                 | GREEN (1/0/0)             | GREEN (1/0/0) preserved           |
| `tests/json_over_tcp_still_accepted`                  | GREEN (1/0/0)             | GREEN (1/0/0) preserved (D-B3 guard) |
| `tests/http_push_still_works`                         | GREEN                     | GREEN (D-A4 preserved)            |
| `tests/tcp_ingest_routing`                            | GREEN                     | GREEN (Phase 50 preserved)        |
| `cargo test --release --lib`                          | 822/0/35                  | **825/0/35** — +3 new unit tests  |
| `scripts/verify-no-tcp-json-reserialize.sh`           | exit 0                    | exit 0 preserved                  |
| `scripts/verify-no-dashmap.sh`                        | exit 0                    | exit 0                            |
| `scripts/verify-no-statestore.sh`                     | exit 0                    | exit 0                            |
| `scripts/verify-no-legacy-push.sh`                    | exit 0                    | exit 0                            |
| `scripts/verify-retraction-metrics.sh`                | exit 0                    | exit 0                            |

## Grep-Invariant Evidence

```
$ grep -c "OP_NEGOTIATE_WIRE_FORMAT" src/server/protocol.rs
6  (const + variant doc + parse arm + 3 unit tests)

$ grep -c "Command::NegotiateWireFormat" src/server/tcp.rs
1  (dispatch arm)

$ grep -c "encode_negotiate_response_body" src/server/protocol.rs
2  (def + use in unit test)

$ grep -c "WIRE_VERSION_TAG_SERVER" src/server/protocol.rs
2  (const + use in dispatch via protocol::)
```

## Deviations from Plan

### Rule 3 — Dispatch location: handle_sync_command, not inline handle_connection

- **Found during:** Task 1 wiring.
- **Issue:** Plan action §3 said to add a `Command::NegotiateWireFormat { .. } => { writer.write_all(&resp).await?; writer.flush().await?; continue; }` arm inline in `handle_connection`. But the handle_connection dispatch already has a catch-all `other => handle_sync_command(other, &state).await.map(Some)` that wraps with encode_response(STATUS_OK, ..) via the outer match. Adding an inline branch would duplicate writer/flush plumbing AND would need to handle both the inner tight-loop (line 1453) and outer loop (line 1635) sites.
- **Fix:** Dispatched via `handle_sync_command` (which returns `Vec<u8>`). The variant falls through the catch-all cleanly; outer match wraps with STATUS_OK. Zero duplication.
- **Files modified:** src/server/tcp.rs (added 1 arm in handle_sync_command).
- **Commit:** e64b85c.

### Rule 2 — Added 3 inline unit tests (plan behavior list required but not in action scope)

- **Found during:** Task 1 TDD decomposition.
- **Issue:** Plan `<behavior>` listed Test 4 (unit, parse_command truncation) and Test 5 (unit, encode_response roundtrip) but `<action>` did not explicitly list the test code. Wave 2 is TDD — these need to land.
- **Fix:** Added `test_parse_command_negotiate_wire_format_happy_path`, `test_parse_command_negotiate_wire_format_truncated`, `test_encode_negotiate_response_body_roundtrip` in `src/server/protocol.rs::tests`. Lib count 822 → 825.
- **Files modified:** src/server/protocol.rs.
- **Commit:** e64b85c.

### Rule 3 — Plan action §4 already satisfied by Wave 1

- **Found during:** Task 1 inspection of parse_command OP_PUSH arm.
- **Issue:** Plan action §4 prescribed adding D-B2 auto-detect (binary-first, JSON-fallback) to OP_PUSH / OP_PUSH_ASYNC / per-event OP_PUSH_BATCH. Inspection showed Wave 1 had already wired this via `parse_push_body` helper for OP_PUSH and OP_PUSH_ASYNC (Rule-1 deviation in Wave 1).
- **Fix:** No Wave 2 action needed on §4. OP_PUSH_BATCH inner loop intentionally NOT extended with per-event JSON fallback — batch events are always binary by construction (Python SDK `_encode_event_body` only); no legacy JSON-batch path exists to support.
- **Files modified:** None (verification only).
- **Commit:** N/A.

## Auth Gates Encountered

None — Wave 2 is server-side opcode surface addition. OP_NEGOTIATE_WIRE_FORMAT
is unauthenticated (matches OP_PUSH posture per T-59-02-04); no admin_token
required.

## Next Wave Handoff

### Wave 3 (plan 59-03) MUST

1. **Python SDK constants:** Add `OP_NEGOTIATE_WIRE_FORMAT = 0x18`,
   `WIRE_BINARY_PASSTHROUGH = 1`, `WIRE_VERSION_TAG_CLIENT = 2` to
   `python/beava/_protocol.py`.
2. **Handshake helper:** `TallyClient.negotiate_wire_format()` that sends
   OP_NEGOTIATE and parses the 6-byte body + caches
   `server_capability_bits` / `server_version_tag`. On STATUS_ERROR
   (pre-59 server): return `(0, 0)` sentinel without raising (D-E4).
3. **Env opt-in:** `BEAVA_WIRE_NEGOTIATE=1` triggers auto-handshake in
   `__init__` (default off per D-B4).
4. **Version bump:** `python/pyproject.toml` minor bump.
5. **Rust integration test:** `tests/python_sdk_pre_59_server_fallback.rs`
   with 3 scenarios (unknown opcode 0x19 STATUS_ERROR + connection
   persistence; truncated OP_NEGOTIATE STATUS_ERROR; subsequent OP_PUSH
   on same connection after error).

### Wave 4 (plan 59-04) scope

Unchanged from Wave 1 SUMMARY: perf gate + samply probe re-run + PERF-GATE.md
+ VERIFICATION.md + ROADMAP / STATE updates + phase close.

## Known Stubs

None — Wave 2's single opcode + dispatch arm is fully wired end-to-end. The
test proves the round-trip works; the unit tests prove parse + encode
correctness; the dispatch arm is executed on every OP_NEGOTIATE_WIRE_FORMAT
request (reachable via `other` catch-all in handle_connection's match).

## Threat Flags

None — plan `<threat_model>` T-59-02-01..T-59-02-05 covers all new surface:

- T-59-02-01 (client_bits tampering) — mitigated: server returns
  SERVER_SUPPORTED_BITS unconditionally, no reflection of client bits.
- T-59-02-02 (auto-detect DoS) — accepted: at most 2-attempt decode
  (binary → JSON); bounded by D-E1 payload cap from Wave 1.
- T-59-02-03 (server version info leak) — accepted: non-secret.
- T-59-02-04 (unauthenticated handshake) — accepted: matches OP_PUSH
  posture; no authority to grant.
- T-59-02-05 (MitM downgrade) — accepted: out of scope (TLS is operator).

## Commits

| Task | Commit    | Message                                                         |
|------|-----------|-----------------------------------------------------------------|
| Task 1 | `e64b85c` | `feat(59-W2): OP_NEGOTIATE_WIRE_FORMAT + dual-format accept (TPC-PERF-09 D-B1)` |

## Self-Check

- [x] `src/server/protocol.rs` contains `pub const OP_NEGOTIATE_WIRE_FORMAT: u8 = 0x18;` — **FOUND**
- [x] `src/server/protocol.rs` contains `pub const WIRE_VERSION_TAG_SERVER: u16 = 2;` — **FOUND**
- [x] `src/server/protocol.rs` contains `Command::NegotiateWireFormat` variant — **FOUND**
- [x] `src/server/protocol.rs` contains OP_NEGOTIATE_WIRE_FORMAT parse_command arm — **FOUND**
- [x] `src/server/protocol.rs` contains `encode_negotiate_response_body` — **FOUND**
- [x] `src/server/tcp.rs` contains `Command::NegotiateWireFormat` dispatch arm in handle_sync_command — **FOUND**
- [x] `tests/wire_negotiation_handshake.rs` no longer has `#[ignore = "59-W2"]` — **VERIFIED**
- [x] Commit `e64b85c` present in git log — **FOUND**
- [x] `cargo test --release --test wire_negotiation_handshake` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --lib` → 825/0/35 — **VERIFIED**
- [x] `cargo test --release --test binary_push_bytes_passthrough` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test json_over_tcp_still_accepted` → 1/0/0 GREEN (D-B3 preserved) — **VERIFIED**
- [x] `cargo test --release --test http_push_still_works` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test tcp_ingest_routing` → 1/0/0 GREEN — **VERIFIED**
- [x] `bash scripts/verify-no-tcp-json-reserialize.sh` → exit 0 — **VERIFIED**
- [x] `bash scripts/verify-no-dashmap.sh` / `verify-no-statestore.sh` / `verify-no-legacy-push.sh` / `verify-retraction-metrics.sh` → all exit 0 — **VERIFIED**

## Self-Check: PASSED
