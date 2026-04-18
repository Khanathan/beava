---
phase: 46-correctness-audit-fixes
plan: 08
subsystem: docs, state/event_log, tests
tags: [event-time, crash-replay, backfill, fsync, ship-gate, correctness, docs]
dependency_graph:
  requires: [46-01, 46-02, 46-03, 46-04, 46-05, 46-06, 46-07]
  provides: [OBS-03, SHIP-01, D-27]
  affects: [docs/event-time.md, docs/http-api.md, src/state/event_log.rs, tests/ship_gate.rs]
tech_stack:
  added: []
  patterns: [tokio::task::spawn_blocking for fsync, LOG_FMT_JSON event log path in test harness]
key_files:
  created:
    - tests/ship_gate.rs (full implementation replacing scaffold)
  modified:
    - docs/event-time.md
    - docs/http-api.md
    - src/state/event_log.rs
decisions:
  - "docs/event-time.md: 7 H2 content sections (Contents + 6 D-24 sections + Related); 340 lines exceeds 200-line floor"
  - "EventLog::append_with_fsync uses spawn_blocking + fdatasync (macOS: fsync fallback); raw fd extracted before await point to avoid holding DashMap guard across await"
  - "ship_gate.rs: raw_payload=[] in PendingAsync forces LOG_FMT_JSON in event log — LOG_FMT_BINARY with plain JSON bytes would be silently skipped by decode_event_binary in run_backfill"
  - "ship_gate.rs: store.get_all_features (not engine.get_features) to read recovered entities — avoids derive overhead and reads operators directly"
  - "ship_gate.rs: events span last 90 minutes with 2h window — all events are within window at read_time ensuring non-zero counts for the parity assertion"
metrics:
  duration: ~40 minutes
  completed: 2026-04-18
  tasks: 3
  files_changed: 4
---

# Phase 46 Plan 08: Full docs/event-time.md + fsync scaffold + SHIP-01 Summary

One-liner: Expanded docs/event-time.md to 340-line authoritative reference (6 sections), scaffolded EventLog::append_with_fsync for future durable-ack, and implemented the SHIP-01 crash-replay parity integration test closing all 14 Phase 46 requirements.

## What Was Built

### Task 1 — docs/event-time.md expansion (OBS-03, D-24, D-25)

`docs/event-time.md` expanded from a 31-line stub (Plan 02) to a 340-line authoritative reference with 6 content sections:

1. **Bucket Assignment** (~60 lines): `parse_event_time` priority order, UNIX-epoch-relative bucket boundaries, CORR-01 per-event batch bucketing rationale, pitfall note.
2. **Watermark Lateness** (~50 lines): 5s default (`WATERMARK_LATENESS`), per-stream `@bv.stream(watermark_lateness=...)` override (CORR-03), backward-compat snapshot migration (CORR-04), γ-propagation rules.
3. **Crash-Replay Determinism** (~50 lines): write-before-extract ordering, D-15 `parse_event_time` fix in `run_backfill`, at-least-once note, D-27 fsync upgrade path, SHIP-01 test reference.
4. **TTL Semantics** (~40 lines): event-time clock via `WatermarkTracker::observed_max` (CORR-07/D-17), backfill eviction correctness, wall-clock fallback.
5. **Backfill** (preserved from Plan 02) + **Join Idle-Input** (preserved from Plan 02, DX-06 v1.1 note added).
6. **Fork Watermark Propagation** (~40 lines): `replica_ingest_batch` watermarks.observe per event (CORR-08/D-19), fork demo workflow.

Cross-link added to `docs/http-api.md` Durable-ack section (D-25):
> See [docs/event-time.md § Crash-replay determinism](event-time.md#crash-replay-determinism) for the at-least-once vs durable-ack tradeoff.

### Task 2 — EventLog::append_with_fsync scaffold (D-27)

New async method at `src/state/event_log.rs`:

```rust
/// D-27 / A7: scaffolded hook for durable-ack HTTP push (future `?sync=1,durable=1`).
pub async fn append_with_fsync(
    &self, stream_name: &str, event_bytes: &[u8], now: SystemTime,
) -> std::io::Result<bool>
```

Implementation: calls synchronous `self.append()` then `tokio::task::spawn_blocking` wrapping `fdatasync(fd)` (macOS: `fsync` fallback). Raw fd extracted before the await point so DashMap is not held across threads.

Not wired to any HTTP or TCP handler (D-27 design intent). Two unit tests added: `append_with_fsync_writes_and_fsyncs` and `append_with_fsync_unregistered_returns_false`.

### Task 3 — SHIP-01 ship-gate integration test (CORR-06)

`tests/ship_gate.rs` fully implemented:

```
Phase A: boot with EventLog → register "Txns" (count_2h, backfill=true) →
         push 200 events (last 90 min, per-event _event_time) via handle_push_batch →
         fsync → read features → drop state (simulated kill -9)

Phase B: boot fresh state from same data_dir → register same stream →
         trigger run_backfill → wait for completion →
         read features at same read_time

Phase C: assert bit-identical parity for all 10 keys (u0..u9)
```

Key implementation detail: `raw_payload: vec![]` in `PendingAsync` forces `make_log_payload` to use `LOG_FMT_JSON` path. If `raw_payload` is non-empty, `make_log_payload` uses `LOG_FMT_BINARY`, which `run_backfill` decodes via `decode_event_binary` (TCP binary wire format). Plain JSON bytes in a `LOG_FMT_BINARY` frame would be silently skipped by `run_backfill`, causing all entries to be dropped on replay.

## Commits

| Hash | Message |
|------|---------|
| `6f9ef79` | docs(46-08): expand docs/event-time.md to full OBS-03 reference (6 sections, D-24/D-25) |
| `1105257` | feat(46-08): scaffold EventLog::append_with_fsync (D-27 A7 hook, unwired) |
| `5a48809` | test(46-08): un-ignore + fully implement SHIP-01 ship-gate integration test (D-16, CORR-06) |

## Verification Results

| Check | Result |
|-------|--------|
| `cargo build --release --bin beava` | green |
| `cargo test --release --lib` | 790 tests, 0 failures |
| `cargo test --test ship_gate --release` × 3 | 3/3 pass |
| ship_gate wall-clock runtime | <0.1s per run (target: <30s) |
| `wc -l docs/event-time.md` | 340 lines (floor: 200) |
| `grep -c '^## ' docs/event-time.md` | 9 sections (floor: 6) |
| `grep 'Bucket Assignment' docs/event-time.md` | PASS |
| `grep 'Join Idle-Input' docs/event-time.md` | PASS |
| `grep -q 'single-event ingest path' docs/event-time.md` | PASS (D-14 preserved) |
| `grep -q 'idle markers' docs/event-time.md` | PASS (CORR-09 preserved) |
| `grep -q 'event-time.md' docs/http-api.md` | PASS (D-25 cross-link) |
| `grep -c 'fn append_with_fsync' src/state/event_log.rs` | 1 |
| `grep -c 'append_with_fsync' src/server/http_ingest.rs` | 0 (not wired) |
| `grep -c 'append_with_fsync' src/server/tcp.rs` | 0 (not wired) |
| `grep -c '#\[ignore' tests/ship_gate.rs` | 0 |

## Phase 46 Final Requirements Audit

| Requirement | Status | Closed by |
|-------------|--------|-----------|
| CORR-01 | Closed (Plan 46-02) | push_batch_with_cascade_no_features per-event bucketing |
| CORR-02 | Closed (Plan 46-02) | 9-cell bench gate <-5% |
| CORR-03 | Closed (Plan 46-04) | per-stream watermark_lateness in StreamDefinition |
| CORR-04 | Closed (Plan 46-04) | snapshot migration — Option<Duration> backward compat |
| CORR-05 | Closed (Plan 46-01) | test_backfill_uses_single_event_path.rs green |
| CORR-06 | Closed (Plan 46-05 + this plan) | D-15 parse_event_time in run_backfill; SHIP-01 verifies |
| CORR-07 | Closed (Plan 46-05) | eviction clock from WatermarkTracker::observed_max |
| CORR-08 | Closed (Plan 46-05) | replica_ingest_batch watermarks.observe per event |
| CORR-09 | Closed (Plan 46-02) | docs/event-time.md join idle-input note (preserved this plan) |
| CORR-10 | Closed (Plan 46-07) | ArcSwap dirty-set swap, <2% bench regression |
| OBS-01 | Closed (Plan 46-06) | beava_ring_buffer_drops_total counter wired |
| OBS-02 | Closed (Plan 46-06) | mutual-exclusivity gate test green |
| OBS-03 | **Closed this plan** | docs/event-time.md 340 lines, 6 sections |
| SHIP-01 | **Closed this plan** | tests/ship_gate.rs passes 3x <0.1s each |

**Phase 46 total: 14/14 requirements CLOSED.**

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] LOG_FMT_BINARY encoding mismatch in ship_gate.rs**
- **Found during:** Task 3 (test run — recovered features all `{}`)
- **Issue:** `PendingAsync.raw_payload` set to `serde_json::to_vec(payload)` caused `make_log_payload` to emit `LOG_FMT_BINARY` prefix + JSON bytes. `run_backfill` decodes `LOG_FMT_BINARY` entries via `decode_event_binary` (TCP binary wire format), which failed on plain JSON bytes and `continue`'d, skipping all 200 entries.
- **Fix:** Set `raw_payload: vec![]` so `make_log_payload` uses the `LOG_FMT_JSON` path. Comment added explaining the invariant.
- **Files modified:** `tests/ship_gate.rs`
- **Commit:** `5a48809`

**2. [Rule 1 - Bug] Live features were `Missing` (events outside 1h window at read time)**
- **Found during:** Task 3 (first test run — live features all `{"count_1h": Missing}`)
- **Issue:** Original design used 1000 events spanning -30 days..now with a 1h window. At read time, events from 30 days ago are outside the 1h window, returning `Missing`. All events land outside the read window.
- **Fix:** Changed to 200 events spanning last 90 minutes with a 2h window. All events are within the window at read time, producing `Int(20)` counts. Explicit `read_time = now + 1s` to bracket all events.
- **Files modified:** `tests/ship_gate.rs`
- **Commit:** `5a48809`

## Known Stubs

None — all three outputs are complete and verified.

## Threat Flags

None — this plan adds docs, a scaffolded (unwired) method, and a test. No new network endpoints, auth paths, file access patterns, or schema changes at trust boundaries.

## Phase 46 Handoff to Phase 47

Phase 46 is COMPLETE. All 14 requirements closed. Phase 47 (ship-gate assembly, Docker, README) may proceed. Key handoff artifacts:

- `docs/event-time.md` — authoritative event-time reference (Phase 47 docs/concepts.md should cross-link)
- `EventLog::append_with_fsync` — hook for future `?sync=1,durable=1` HTTP handler
- `tests/ship_gate.rs` — SHIP-01 regression test, must remain green in Phase 47

## Self-Check: PASSED

- FOUND: docs/event-time.md (340 lines)
- FOUND: docs/http-api.md (cross-link added)
- FOUND: src/state/event_log.rs (`append_with_fsync` present)
- FOUND: tests/ship_gate.rs (370 lines, no #[ignore])
- FOUND: commit 6f9ef79 (docs expansion)
- FOUND: commit 1105257 (fsync scaffold)
- FOUND: commit 5a48809 (ship-gate test)
