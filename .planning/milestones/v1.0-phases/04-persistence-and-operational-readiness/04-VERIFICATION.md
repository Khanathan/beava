---
phase: 04-persistence-and-operational-readiness
verified: 2026-04-09T19:40:00Z
status: human_needed
score: 5/5 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Kill and restart a running Tally server after pushing events, verify GET returns pre-crash feature values"
    expected: "GET returns count/sum values reflecting events pushed before the kill, within the 30s snapshot interval"
    why_human: "Cannot start server process in this environment; requires live server + file I/O cycle"
  - test: "Push events concurrently while snapshot is writing; verify pushes are not blocked"
    expected: "PUSH responses continue to arrive during snapshot window; no observable stall"
    why_human: "Timing/concurrency behavior requires a live running server"
  - test: "Wait 2x the max window duration without pushing events; verify entity is evicted from memory and GET returns empty"
    expected: "After TTL, GET /debug/memory shows decreasing entity count; GET for that key returns empty map"
    why_human: "Requires running server and real clock advancement; eviction timer fires every 60s"
---

# Phase 4: Persistence and Operational Readiness — Verification Report

**Phase Goal:** Tally survives restarts (snapshot persistence + crash recovery), reclaims memory for idle keys (TTL eviction), and exposes enough observability for production use (HTTP management API with pipeline CRUD, metrics, and debug endpoints)
**Verified:** 2026-04-09T19:40:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (from ROADMAP.md Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | After crash-restart, GET returns pre-crash state (within snapshot interval) | VERIFIED | `main.rs:35-67`: reads snapshot file on startup, calls `restore_from_snapshot` + re-registers pipelines from stored JSON; `test_snapshot_roundtrip_preserves_features` proves state round-trips correctly |
| 2 | Snapshot write never blocks PUSH/GET for more than a single event cycle | VERIFIED | `main.rs:119`: `tokio::task::spawn_blocking` offloads serialization; state is cloned under brief lock then released before blocking write; `Instant::now()` captured before `spawn_blocking` |
| 3 | Entity keys receiving no events for 2x largest window are auto-removed | VERIFIED | `main.rs:143-161`: 60s periodic eviction timer calls `evict_expired_keys`; `test_eviction_removes_old_entity` and `test_evict_expired_keys_removes_old` confirm correct behavior |
| 4 | HTTP API: GET /pipelines returns definitions; GET /debug/key/:key returns operator internals; GET /metrics returns Prometheus counters | VERIFIED | `http.rs:309-322`: all 8 routes registered (`/pipelines`, `/pipelines/{name}`, `/metrics`, `/debug/key/{key}`, `/debug/memory`, `/snapshot`); `test_pipelines_register_and_list`, `test_debug_key_after_push`, `test_metrics_endpoint` all pass |
| 5 | Starting with a different snapshot format version → clean empty startup, not panic | VERIFIED | `snapshot.rs:87-99`: `load_snapshot` checks version byte, returns `None` on mismatch; `main.rs:59-61`: None → logs "Snapshot incompatible or corrupt, starting fresh"; `test_snapshot_version_mismatch_returns_none` and `test_load_snapshot_wrong_version_returns_none` pass |

**Score:** 5/5 truths verified

### Deferred Items

None.

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/state/snapshot.rs` | OperatorState enum, SnapshotState, save_snapshot, load_snapshot | VERIFIED | File exists, 310 lines; `pub enum OperatorState` with Count/Sum/Avg variants; `pub fn save_snapshot` returns `Vec<u8>` with version byte 0x01 prefix; `pub fn load_snapshot` returns `Option<SnapshotState>` |
| `src/state/eviction.rs` | `evict_expired_keys` function | VERIFIED | File exists, 149 lines; `pub fn evict_expired_keys` takes `&mut StateStore`, `&PipelineEngine`, `SystemTime`, `u32`; 5 unit tests pass |
| `src/state/store.rs` | EntityState with OperatorState, clone_for_snapshot, restore_from_snapshot, remove_expired_entities | VERIFIED | `EntityState.live_operators: Vec<(String, OperatorState)>` (no Box<dyn Operator>); all three methods present; tests pass |
| `src/engine/pipeline.rs` | PipelineEngine with max_window_duration, get_raw_register_json, OperatorState-based push | VERIFIED | `max_window_duration`, `list_streams`, `remove_stream`, `store_raw_register_json`, `get_raw_register_json` all present; `raw_register_jsons: AHashMap<String, serde_json::Value>` field present |
| `src/main.rs` | Startup snapshot recovery, periodic snapshot timer (30s), periodic eviction timer (60s) | VERIFIED | All three subsystems present; TALLY_SNAPSHOT_PATH and TALLY_TTL_MULTIPLIER env vars parsed; `spawn_blocking` + atomic rename used for snapshot writes |
| `src/server/http.rs` | Full HTTP management API with pipeline CRUD, metrics, debug, snapshot endpoints | VERIFIED | 8 endpoints registered; `list_pipelines`, `get_pipeline`, `create_pipeline`, `delete_pipeline`, `metrics_endpoint`, `debug_key`, `debug_memory`, `trigger_snapshot` all substantive |
| `src/server/tcp.rs` | Metrics struct with events_total, push_latency_seconds, snapshot_duration_ms | VERIFIED | `pub struct Metrics { events_total: u64, push_latency_seconds: f64, snapshot_duration_ms: u64 }` present; PUSH handler records latency via `Instant::now()` |
| `tests/test_snapshot.rs` | Integration tests for snapshot persistence and recovery | VERIFIED | 7 tests covering round-trip, version mismatch, empty bytes, corrupt data, eviction (old/no-event entities), atomic write; all 7 pass |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/state/store.rs` | `src/state/snapshot.rs` | `clone_for_snapshot` returns `Vec<(String, SerializableEntityState)>` | WIRED | `store.rs:140`: calls `entity.live_operators.clone()` and converts AHashMap to Vec |
| `src/state/snapshot.rs` | postcard | `postcard::to_stdvec` and `postcard::from_bytes` | WIRED | `snapshot.rs:78,99`: both calls present in `save_snapshot` and `load_snapshot` |
| `src/engine/pipeline.rs` | `src/state/snapshot.rs` | `OperatorState` enum used in EntityState | WIRED | `pipeline.rs:13`: `use crate::state::snapshot::OperatorState`; `create_operator` returns `Option<OperatorState>` |
| `src/engine/pipeline.rs` | raw_register_jsons | `get_raw_register_json` / `store_raw_register_json` | WIRED | `pipeline.rs:285-292`: both methods present; `tcp.rs:185`: REGISTER handler calls `store_raw_register_json` |
| `src/main.rs` | `src/state/snapshot.rs` | `save_snapshot` and `load_snapshot` calls | WIRED | `main.rs:10,37,120`: both functions imported and called |
| `src/main.rs` | `src/state/eviction.rs` | `evict_expired_keys` call in eviction timer | WIRED | `main.rs:9,156`: imported and called inside 60s interval |
| `src/main.rs` | `tokio::task::spawn_blocking` | Snapshot serialization offloaded to blocking thread | WIRED | `main.rs:119`: `tokio::task::spawn_blocking` wraps `save_snapshot` + `fs::write` + `fs::rename` |
| `src/server/http.rs` | `src/engine/pipeline.rs` | `list_streams`, `get_stream`, `remove_stream`, `get_raw_register_json` | WIRED | `http.rs:24,33,107,130,255`: all four methods called |
| `src/server/http.rs` | `src/state/store.rs` | `entity_count`, `get_all_features`, `get_entity` | WIRED | `http.rs:147,185,219`: all three methods called |
| `src/main.rs snapshot timer Ok(Ok(_)) arm` | `src/server/tcp.rs Metrics.snapshot_duration_ms` | Re-locks `snap_state`, writes `elapsed.as_millis()` | WIRED | `main.rs:130-133`: `app.metrics.snapshot_duration_ms = snap_elapsed.as_millis() as u64` |
| `src/server/http.rs trigger_snapshot Ok(Ok(_)) arm` | `src/server/tcp.rs Metrics.snapshot_duration_ms` | Re-locks state, writes elapsed duration | WIRED | `http.rs:286-289`: same pattern as main.rs timer |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|--------------------|--------|
| `src/server/http.rs` `list_pipelines` | `names: Vec<String>` | `app.engine.list_streams()` → AHashMap iteration | Yes — real registered streams | FLOWING |
| `src/server/http.rs` `metrics_endpoint` | `keys_total`, `events_total`, `push_latency`, `snapshot_duration` | `app.store.entity_count()`, `app.metrics.*` | Yes — real counters from PUSH handler | FLOWING |
| `src/server/http.rs` `debug_key` | `live_ops`, `static_feats`, `features` | `app.store.get_entity(&key)`, `app.store.get_all_features` | Yes — real entity state | FLOWING |
| `src/main.rs` snapshot timer | `snapshot_data: SnapshotState` | `app.store.clone_for_snapshot()` + `engine.list_streams()` | Yes — full entity state clone | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `cargo test --lib` — all unit tests | `~/.cargo/bin/cargo test --lib` | `ok. 245 passed; 0 failed` | PASS |
| `cargo test --test test_snapshot` | `~/.cargo/bin/cargo test --test test_snapshot` | `ok. 7 passed; 0 failed` | PASS |
| `cargo test --test test_server` | `~/.cargo/bin/cargo test --test test_server` | `ok. 28 passed; 0 failed` | PASS |
| Full test suite | `~/.cargo/bin/cargo test` | `291 passed; 0 failed` across all test bins | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| PERS-01 | 04-01, 04-02 | Periodic snapshot serialization every 30s | SATISFIED | `main.rs:87-140`: 30s interval timer with `clone_for_snapshot` + `spawn_blocking` + atomic rename |
| PERS-02 | 04-01 | Snapshot uses postcard + serde with versioned format (version byte per snapshot) | SATISFIED | `snapshot.rs:16,76-100`: `SNAPSHOT_FORMAT_VERSION = 1`; `postcard::to_stdvec`/`from_bytes`; version byte prepended |
| PERS-03 | 04-02 | Server loads latest snapshot on startup for crash recovery | SATISFIED | `main.rs:34-68`: file existence check → `load_snapshot` → `restore_from_snapshot` + pipeline re-registration |
| PERS-04 | 04-01, 04-02 | Snapshot write uses cooperative yielding to avoid blocking event loop | SATISFIED | `main.rs:119`: `tokio::task::spawn_blocking`; state cloned under brief lock before offloading |
| PERS-05 | 04-01, 04-02 | TTL-based key eviction (default: 2x largest window) | SATISFIED | `eviction.rs:15-27`: `evict_expired_keys` uses `max_window * ttl_multiplier`; wired in 60s timer in `main.rs:142-161` |
| SRV-08 | 04-03 | HTTP management API: health, metrics, debug, pipeline CRUD on separate port | SATISFIED | `http.rs:309-342`: 8 endpoints; all integration tests pass; `run_http_server` on configurable port (default 6401) |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/server/http.rs` | 151 | `let memory_bytes = keys_total * 2048` — rough heuristic estimate | Info | Metric `tally_memory_bytes` is an approximation, not actual measurement. Documented in 04-03-SUMMARY.md as intentional v1 decision. Does not affect functional correctness. |

No blockers or warnings found. No TODOs, FIXMEs, stubs, or placeholder returns in phase-4 files.

### Human Verification Required

#### 1. Crash Recovery End-to-End

**Test:** Start the Tally binary, register a stream, push 10 events, wait 30+ seconds for snapshot, kill the process (SIGKILL), restart the binary against the same snapshot file, send a GET for the entity key used in the pushes.
**Expected:** GET response includes `tx_count_1h = 10` (or close to it, reflecting pre-crash events). No panic on startup.
**Why human:** Cannot start server processes in this environment; requires live server + real file I/O cycle.

#### 2. Non-Blocking Snapshot Concurrency

**Test:** While a snapshot is writing (POST /snapshot or during the 30s timer), concurrently push 100 events and measure PUSH latency.
**Expected:** PUSH p99 latency remains < 1ms during snapshot write; no observable stall or queuing.
**Why human:** Timing/concurrency behavior requires a live running server; cannot verify programmatically.

#### 3. TTL Eviction Observable via Metrics

**Test:** Register a stream with a 1-minute window. Push an event. Wait 3 minutes (2x window). Trigger an eviction sweep (or wait for the 60s timer). Check GET /debug/memory and GET for the evicted key.
**Expected:** `entity_count` decreases by 1; GET for the evicted key returns `{}`.
**Why human:** Requires running server and real clock advancement; eviction only fires every 60s.

### Gaps Summary

No gaps found. All 5 ROADMAP success criteria are met by the codebase. All 6 required requirements (PERS-01 through PERS-05, SRV-08) are implemented and tested. The only outstanding items are runtime behavioral checks that require a live server.

---

_Verified: 2026-04-09T19:40:00Z_
_Verifier: Claude (gsd-verifier)_
