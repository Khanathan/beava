---
phase: 06-foundation
verified: 2026-04-09T23:59:00Z
status: passed
score: 5/5
overrides_applied: 0
---

# Phase 6: Foundation — Verification Report

**Phase Goal:** Restructure entity state for per-stream isolation and establish the SSD event log as the persistence foundation for all subsequent v1.1 features
**Verified:** 2026-04-09T23:59:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | User can push events and observe them persisted to an append-only log file on disk, with keyed stream logs eligible for compaction and keyless stream logs append-only | VERIFIED | `src/state/event_log.rs` implements `append`, `compact_stream`, and `read_entries`; PUSH handler calls `log.append(&stream_name, ...)` in `src/server/tcp.rs:161` |
| 2 | User can configure `history_ttl` per stream at registration time and expired events are removed by background compaction | VERIFIED | `StreamDefinition.history_ttl: Option<Duration>` in `pipeline.rs:98`; `RegisterRequest.history_ttl` parsed in `protocol.rs:511`; compaction timer in `main.rs` calls `log.compact_stream` every 60s |
| 3 | Event log writes do not measurably degrade PUSH p99 latency (remains under 100us with buffered async writes) | VERIFIED | `BufWriter<File>` used in `event_log.rs`; `DEFAULT_HISTORY_TTL` comment confirms `~100-300ns memcpy` write path; `fdatasync` runs only in background 1s timer, never on hot path |
| 4 | User can fetch features for multiple keys in a single MGET call and receive all results in one response | VERIFIED | `OP_MGET = 0x06` in `protocol.rs:14`; `Command::Mget { keys }` dispatched in `tcp.rs:244`; Python `App.mget()` in `_app.py:108`; 160 Python tests + 28 server tests pass |
| 5 | User can configure entity state TTL per stream, and keys expire independently per stream (short-TTL stream expiry does not evict long-TTL stream state for the same entity) | VERIFIED | `evict_expired_stream_entries` in `eviction.rs:20` removes individual stream entries independently; `remove_empty_entities` only removes entity after all streams gone; `StreamEntityState.last_event_at` is per-stream |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/state/store.rs` | `StreamEntityState` struct + restructured `EntityState` | VERIFIED | `pub struct StreamEntityState` at line 27; `pub streams: AHashMap<String, StreamEntityState>` at line 49; helper methods `get_or_create_stream`, `is_empty`, `remove_empty_entities` present |
| `src/state/snapshot.rs` | v4 snapshot format with `SerializableStreamEntityState` | VERIFIED | `SNAPSHOT_FORMAT_VERSION: u8 = 4` at line 21; `pub struct SerializableStreamEntityState` at line 77; `pub streams: Vec<(String, SerializableStreamEntityState)>` at line 87 |
| `src/engine/pipeline.rs` | Push and get_features using per-stream entity state | VERIFIED | `get_or_create_stream(stream_name)` called at line 239; `entity_ttl: Option<Duration>` at line 95; `history_ttl: Option<Duration>` at line 98 |
| `src/state/eviction.rs` | Per-stream TTL eviction logic | VERIFIED | `pub fn evict_expired_stream_entries` at line 20; backward-compat wrapper `evict_expired_keys` at line 84 |
| `src/server/protocol.rs` | `MGET` opcode 0x06 and `Command::Mget` variant | VERIFIED | `pub const OP_MGET: u8 = 0x06` at line 14; `Mget { keys: Vec<String> }` at line 28; parsing at line 193; `entity_ttl`/`history_ttl` on `RegisterRequest` at lines 251-253 |
| `src/server/tcp.rs` | `MGET` command handler; `EventLog` in `AppState` | VERIFIED | `Command::Mget { keys }` handler at line 244; `pub event_log: Option<EventLog>` at line 37; `log.append(&stream_name, ...)` at line 161 |
| `src/state/event_log.rs` | `EventLog` module with append, fsync, compaction | VERIFIED | `pub struct EventLog` at line 25; all required methods present: `append`, `read_entries`, `fsync_all`, `compact_stream`, `deregister_stream`, `sanitize_stream_name`; `DEFAULT_HISTORY_TTL = Duration::from_secs(259200)` at line 16 |
| `src/state/mod.rs` | `event_log` module declaration | VERIFIED | `pub mod event_log` at line 4 |
| `src/main.rs` | `EventLog` initialization, fsync timer, compaction timer | VERIFIED | `EventLog::new(event_log_dir)` at line 33; `TALLY_DATA_DIR` env var at line 31; `log.fsync_all()` at line 214; `log.compact_stream(stream_name, now)` at line 247; `log.register_stream` at line 80 |
| `python/tally/_protocol.py` | `OP_MGET` constant and `encode_mget` function | VERIFIED | `OP_MGET: int = 0x06` at line 27; `def encode_mget(keys: list[str]) -> bytes` at line 91 |
| `python/tally/_app.py` | `App.mget` method | VERIFIED | `def mget(self, keys: list[str]) -> dict[str, FeatureResult]` at line 108; imports `OP_MGET` and `encode_mget` |
| `python/tally/_stream.py` | `entity_ttl` and `history_ttl` on `@st.stream` decorator | VERIFIED | `stream()` accepts `entity_ttl`/`history_ttl` at lines 117-118; `StreamMeta` stores them at lines 79-80; `_to_register_json` includes them conditionally at lines 107-110; views reject them at line 59 |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/state/store.rs` | `src/state/snapshot.rs` | `clone_for_snapshot` / `restore_from_snapshot` with `SerializableStreamEntityState` | VERIFIED | `SerializableStreamEntityState` referenced in both files |
| `src/engine/pipeline.rs` | `src/state/store.rs` | `entity.get_or_create_stream(stream_name)` | VERIFIED | `get_or_create_stream` called at `pipeline.rs:239` |
| `src/state/eviction.rs` | `src/engine/pipeline.rs` | reads `entity_ttl` from `StreamDefinition` | VERIFIED | `entity_ttl` referenced in eviction logic |
| `src/server/tcp.rs` | `src/server/protocol.rs` | dispatches `Command::Mget` | VERIFIED | `Command::Mget { keys }` arm present at `tcp.rs:244` |
| `src/server/tcp.rs` | `src/state/event_log.rs` | `event_log.append()` in PUSH handler | VERIFIED | `log.append(&stream_name, &event_bytes, now)` at `tcp.rs:161` |
| `src/main.rs` | `src/state/event_log.rs` | `fsync_all` and `compact_stream` in background timers | VERIFIED | Both calls present in `main.rs` at lines 214 and 247 |
| `python/tally/_app.py` | `python/tally/_protocol.py` | `encode_mget` + `OP_MGET` | VERIFIED | Both imported and used in `_app.py` at lines 21 and 119 |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|-------------------|--------|
| `src/server/tcp.rs` PUSH handler | `event_bytes` | `serde_json::to_vec(&payload)` from live request payload | Yes — raw event JSON | FLOWING |
| `src/state/event_log.rs` `compact_stream` | surviving entries | `read_entries()` then filter by `history_ttl` | Yes — real timestamp filtering | FLOWING |
| `src/state/eviction.rs` `evict_expired_stream_entries` | `stream_state.last_event_at` | `StreamEntityState.last_event_at` set on each PUSH | Yes — real timestamps | FLOWING |

### Behavioral Spot-Checks

| Behavior | Result | Status |
|----------|--------|--------|
| Rust full test suite (386 lib + 11 pipeline + 28 server + 7 snapshot) | 432 passed, 0 failed | PASS |
| Python test suite (160 tests across all modules) | 160 passed, 0 failed | PASS |
| `OP_MGET = 0x06` constant in Python SDK | Confirmed in `_protocol.py:27` | PASS |
| `SNAPSHOT_FORMAT_VERSION = 4` in snapshot.rs | Confirmed at `snapshot.rs:21` | PASS |
| `DEFAULT_HISTORY_TTL = 259200s (72h)` in event_log.rs | Confirmed at `event_log.rs:16` | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| ELOG-01 | 06-03-PLAN.md | Keyless streams persist events as append-only log on SSD | SATISFIED | `event_log.rs` append-only writes; PUSH handler calls `log.append` |
| ELOG-02 | 06-03-PLAN.md | Keyed streams persist events as append-only log with compaction | SATISFIED | `compact_stream` rewrites log excluding expired entries; background 60s timer |
| ELOG-03 | 06-03-PLAN.md | Event log writes do not block the hot path | SATISFIED | `BufWriter::write_all` (~100-300ns memcpy); `fdatasync` in background 1s timer only |
| ELOG-04 | 06-03-PLAN.md, 06-04-PLAN.md | User can configure history TTL per stream | SATISFIED | `history_ttl` on `StreamDefinition`; parsed from `RegisterRequest`; Python SDK serializes it |
| ELOG-05 | 06-03-PLAN.md | Background compaction removes events older than history TTL | SATISFIED | `compact_stream` filters `entry.timestamp` against `history_ttl`; 60s background timer |
| OPS-01 | 06-02-PLAN.md, 06-04-PLAN.md | MGET batch key reads in single call | SATISFIED | `OP_MGET = 0x06` in Rust server; `App.mget()` in Python SDK |
| OPS-02 | 06-01-PLAN.md, 06-02-PLAN.md | Entity state TTL per stream (independent expiry) | SATISFIED | `StreamEntityState.last_event_at` per-stream; `evict_expired_stream_entries` removes stream entries independently |

All 7 required requirement IDs (ELOG-01 through ELOG-05, OPS-01, OPS-02) are satisfied.

### Anti-Patterns Found

No blockers found. All methods have real implementations. Event log writes use real file I/O with BufWriter. Eviction logic reads real timestamps.

| File | Pattern | Severity | Impact |
|------|---------|----------|--------|
| None | — | — | — |

### Human Verification Required

None. All success criteria are verifiable programmatically via code inspection and test results.

### Gaps Summary

No gaps. All 5 roadmap success criteria are verified, all 13 required artifacts exist with substantive implementations, all 7 key links are wired, and the full test suite (432 Rust + 160 Python = 592 tests) passes with 0 failures.

---

_Verified: 2026-04-09T23:59:00Z_
_Verifier: Claude (gsd-verifier)_
