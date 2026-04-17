---
phase: 46-correctness-audit-fixes
plan: "04"
subsystem: engine/watermark + python-sdk
tags: [correctness, watermark, event-time, python-sdk, serde, forward-compat]
dependency_graph:
  requires: [46-03]
  provides: [CORR-03, CORR-04]
  affects: [src/engine/event_time.rs, src/engine/pipeline.rs, src/engine/register.rs, python/beava/_stream.py, python/beava/_serialize.py]
tech_stack:
  added: []
  patterns: [parse_duration_str reuse, DashMap per-stream override, serde(default) forward-compat]
key_files:
  created:
    - tests/test_watermarks_per_stream_lateness.rs
    - tests/test_snapshot_lateness_migration.rs
  modified:
    - src/engine/pipeline.rs
    - src/engine/event_time.rs
    - src/engine/register.rs
    - python/beava/_stream.py
    - python/beava/_serialize.py
decisions:
  - "Reused existing parse_duration_str helper (src/duration.rs) for watermark_lateness parsing — no humantime_serde dep added (46-RESEARCH.md Gap 4 override of CONTEXT D-09)"
  - "watermark_lateness: None on all 45 struct literal initializers in pipeline.rs (replace_all via Python script — struct derives Default so all are safe)"
  - "set_lateness called before streams.insert in PipelineEngine::register to ensure immediate observe() calls use the correct lateness"
metrics:
  duration_minutes: ~15
  completed_date: "2026-04-17"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 7
---

# Phase 46 Plan 04: Per-stream watermark_lateness (CORR-03/CORR-04) Summary

**One-liner:** Per-stream `watermark_lateness: Option<Duration>` in `StreamDefinition` wired end-to-end from Python `@bv.stream(watermark_lateness="10m")` through `SourceDescriptor` → `parse_duration_str` → `WatermarkTracker::set_lateness`, with `#[serde(default)]` forward-compat for older snapshots (absent field → 5 s constant).

## What Was Built

### Task 1: Rust server-side (CORR-03/CORR-04)

**`src/engine/pipeline.rs`**
- Added `pub watermark_lateness: Option<Duration>` to `StreamDefinition` struct
- Added `watermark_lateness: None` to all 45 explicit struct literal initializers (automated via Python replace script)
- `PipelineEngine::register` now calls `self.watermarks.set_lateness(&stream.name, lateness)` when `stream.watermark_lateness` is `Some` — called before `streams.insert` so any downstream cascade uses the correct lateness immediately

**`src/engine/event_time.rs`**
- `WatermarkTracker` struct gains `watermark_lateness: DashMap<String, Duration>` field
- `WatermarkTracker::new()` initializes it to an empty `DashMap`
- Added `pub fn set_lateness(&self, stream: &str, lateness: Duration)` — lock-free DashMap insert
- Added `pub fn lateness_for(&self, stream: &str) -> Duration` — lookup with `WATERMARK_LATENESS` (5 s) fallback
- `watermark()` replaced constant `WATERMARK_LATENESS` with per-stream `self.lateness_for(stream)` call

**`src/engine/register.rs`**
- `SourceDescriptor` gains `#[serde(default)] pub watermark_lateness: Option<String>` — absent field in older JSON → `None` (CORR-04)
- `v0_source_to_stream_def` parses `watermark_lateness` via existing `parse_duration_str` helper, mirroring the `entity_ttl` pattern
- All 5 other `StreamDefinition` literal constructions in register.rs updated with `watermark_lateness: None`

**Tests (6 new, all green):**
- `tests/test_watermarks_per_stream_lateness.rs`: `per_stream_override_honored`, `absent_field_defaults_to_5s`, `register_propagates_watermark_lateness_to_tracker`, `register_no_watermark_lateness_keeps_5s_default`
- `tests/test_snapshot_lateness_migration.rs`: `old_snapshot_loads_with_default_lateness`, `new_payload_with_watermark_lateness_parses_correctly`

### Task 2: Python SDK (D-11)

**`python/beava/_stream.py`**
- `stream()` decorator accepts `watermark_lateness: str | None = None` kwarg
- Client-side `_validate_duration_str(watermark_lateness, field="watermark_lateness")` check (mirrors `history_ttl` pattern)
- `_stream_impl()` signature extended with `watermark_lateness=None` kwarg
- `_wrap()` passes `watermark_lateness=watermark_lateness` to `StreamSource(...)`
- `StreamSource.__init__` accepts and stores `self._watermark_lateness = watermark_lateness`

**`python/beava/_serialize.py`**
- `_compile_source()` emits `"watermark_lateness": descriptor._watermark_lateness` in the register JSON dict when `_watermark_lateness` is not None

## Test Output

```
running 2 tests
test new_payload_with_watermark_lateness_parses_correctly ... ok
test old_snapshot_loads_with_default_lateness ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

running 4 tests
test register_no_watermark_lateness_keeps_5s_default ... ok
test absent_field_defaults_to_5s ... ok
test per_stream_override_honored ... ok
test register_propagates_watermark_lateness_to_tracker ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

lib tests: 788 passed; 0 failed; 0 ignored
```

## Commits

| Hash | Message |
|------|---------|
| `44e4128` | `feat(46-04): CORR-03/CORR-04 — per-stream watermark_lateness with forward-compat serde default (D-09/D-10/D-12)` |
| `ff2cfd5` | `feat(46-04): python SDK @bv.stream watermark_lateness kwarg plumbing (D-11)` |

## Deviations from Plan

### Auto-fixed Issues

None.

### Research-driven Amendments (pre-planned in 46-RESEARCH.md)

**1. [Gap 4] Reused `parse_duration_str` instead of `humantime_serde`**
- CONTEXT D-09 originally specified `#[serde(default, with = "humantime_serde::option")]`
- 46-RESEARCH.md Gap 4 overrode this: reuse the existing `parse_duration_str` Beava helper, matching the established `entity_ttl` plumbing pattern
- No new dependencies added; `Cargo.toml` humantime count = 0
- `StreamDefinition` does not require `Deserialize` derive — forward-compat is handled by `SourceDescriptor` (which already derives `Deserialize` with `#[serde(default)]` on Option fields)

**2. All struct literal initializers updated via automated script**
- 45 occurrences of `max_keys: None,` in `pipeline.rs` + 5 in `register.rs` needed `watermark_lateness: None,` appended
- Used a Python `re.sub` script rather than 50 individual Edit calls — functionally identical result, faster and less error-prone

## Dependency / Parse Chain

```
Python @bv.stream(watermark_lateness="10m")
  → StreamSource._watermark_lateness = "10m"
  → _compile_source() emits {"watermark_lateness": "10m"} in JSON
  → Server SourceDescriptor.watermark_lateness: Option<String> = Some("10m")
  → v0_source_to_stream_def calls parse_duration_str("10m") → Duration::from_secs(600)
  → StreamDefinition.watermark_lateness = Some(600s)
  → PipelineEngine::register calls watermarks.set_lateness("Txns", 600s)
  → WatermarkTracker::watermark("Txns") subtracts 600s from observed_max
```

## Requirements Closed

| Req ID | Status |
|--------|--------|
| CORR-03 | CLOSED — per-stream watermark_lateness honored end-to-end |
| CORR-04 | CLOSED — old snapshots load cleanly with 5 s default; no version bump |

Running Phase 46 closed total: 6 (CORR-01, CORR-02, CORR-03, CORR-04, CORR-05, CORR-09)

## Known Stubs

None — plan goal fully achieved; no placeholder data or TODO markers in modified files.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes at trust boundaries introduced by this plan.

## Self-Check: PASSED

- `src/engine/pipeline.rs` — exists, `watermark_lateness: Option<Duration>` field present
- `src/engine/event_time.rs` — exists, `set_lateness` + `lateness_for` + `watermark_lateness: DashMap` present
- `src/engine/register.rs` — exists, `watermark_lateness: Option<String>` on `SourceDescriptor`, `parse_duration_str` used twice
- `tests/test_watermarks_per_stream_lateness.rs` — exists, 0 `#[ignore]` attrs, 4 tests green
- `tests/test_snapshot_lateness_migration.rs` — exists, 0 `#[ignore]` attrs, 2 tests green
- `python/beava/_stream.py` — exists, 9 occurrences of `watermark_lateness`
- `python/beava/_serialize.py` — exists, 3 occurrences of `watermark_lateness`
- Commit `44e4128` — verified via `git log`
- Commit `ff2cfd5` — verified via `git log`
- `cargo build --release --bin beava` → exit 0
- `cargo test --test test_watermarks_per_stream_lateness --release` → exit 0 (4 tests)
- `cargo test --test test_snapshot_lateness_migration --release` → exit 0 (2 tests)
- `cargo test --lib --release` → exit 0 (788 tests)
- `grep -ci humantime Cargo.toml` → 0 (no new dep)
