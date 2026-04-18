---
phase: 46-correctness-audit-fixes
plan: 06
subsystem: engine/event_time, engine/window, engine/pipeline, server/http, server/tcp
tags: [observability, prometheus, ring-buffer, event-time, correctness]
dependency_graph:
  requires: [46-03, 46-05]
  provides: [OBS-01, OBS-02]
  affects:
    - src/engine/event_time.rs
    - src/engine/window.rs
    - src/engine/pipeline.rs
    - src/server/http.rs
    - src/server/tcp.rs
    - tests/test_ring_buffer_drops_metric.rs
tech_stack:
  added: []
  patterns:
    - "DropReason hard enum as compile-time label cardinality bound (D-05)"
    - "RingBufferDropCounters mirrors LateDropCounters DashMap + AtomicU64 pattern"
    - "D-06 pre-registration: register() at stream registration time, fetch_add on hot path"
    - "last_drop: Option<DropReason> side-channel on RingBuffer<T> with #[serde(skip)]"
    - "ring_buffer_drop_reason() post-push introspection on OperatorState"
key_files:
  created: []
  modified:
    - src/engine/event_time.rs
    - src/engine/window.rs
    - src/engine/pipeline.rs
    - src/server/http.rs
    - src/server/tcp.rs
    - tests/test_ring_buffer_drops_metric.rs
decisions:
  - "D-05: DropReason is a hard Copy+Eq+Hash enum (TooOld|TooNew|PreEpoch), not a string — label cardinality is bounded at compile time"
  - "D-06: counter handles cached via RingBufferDropCounters::register() at stream registration; hot drop path calls only fetch_add(1, Relaxed) on Arc<AtomicU64>"
  - "Avoided Operator trait change: added take_ring_buffer_drop() to operator structs + ring_buffer_drop_reason() to OperatorState as post-push introspection rather than changing Operator::push() return type"
  - "Serialization safety: last_drop: Option<DropReason> on RingBuffer<T> carries #[serde(skip)] so snapshot round-trips are unaffected; counter resets to zero after snapshot reload (observability-only)"
  - "OBS-02 mutual exclusivity is structural, not runtime-checked: the tcp.rs late-drop gate fires continue before push_with_cascade is called, so both counters physically cannot fire for the same event"
metrics:
  duration_minutes: 45
  completed: "2026-04-17"
  tasks_completed: 3
  files_modified: 6
  commits: 3
---

# Phase 46 Plan 06: Ring-Buffer Drop Counter (OBS-01 / OBS-02) Summary

JWT-style `beava_ring_buffer_drops_total{stream, operator_kind, reason}` Prometheus counter with compile-time-bounded label cardinality (hard-enum `reason`), cached `Arc<AtomicU64>` handles pre-allocated at operator registration (D-06), and structural mutual-exclusivity with `beava_late_events_dropped_total` (OBS-02).

## Commits

| Hash | Message |
|------|---------|
| `e62f3ac` | feat(46-06): add DropReason enum + RingBufferDropCounters metric struct |
| `6575999` | feat(46-06): wire ring-buffer drop counter into PipelineEngine (D-06) |
| `7e5af28` | feat(46-06): emit beava_ring_buffer_drops_total + OBS-02 gate + tests |

## What Was Built

### Task 1: DropReason enum + RingBufferDropCounters struct (event_time.rs + window.rs + pipeline.rs)

**`DropReason`** — `Copy + Eq + Hash` enum with three variants:
- `TooOld` → label `"too_old"` — event older than ring-buffer window
- `TooNew` → label `"too_new"` — event beyond current head of ring buffer
- `PreEpoch` → label `"pre_epoch"` — event_time < UNIX_EPOCH

**`RingBufferDropCounters`** — mirrors `LateDropCounters`:
- `DashMap<(String, String, DropReason), Arc<AtomicU64>>` keyed by (stream, operator_kind, reason)
- `register(stream, op_kind)` pre-allocates 3 `Arc<AtomicU64>` handles at registration time (D-06)
- `increment(stream, op_kind, reason)` used on the (cold) drop path
- `snapshot()` / `total()` for metrics and tests

**`RingBuffer<T>`** — added `#[serde(skip)] last_drop: Option<DropReason>` side-channel:
- `bucket_index_for()` sets `self.last_drop` at every drop branch (TooOld / TooNew / PreEpoch)
- `take_last_drop()` drains the side-channel after a push

**`OperatorState::ring_buffer_drop_reason()`** — post-push introspection dispatching to `take_ring_buffer_drop()` on each ring-buffer-owning operator (Count, Sum, Avg, Min, Max, Stddev, ExactMin, ExactMax, Variance, DistinctCount). Non-ring-buffer operators (Percentile, TopK, Last, Lag, Ema, LastN, First, FirstN, StreamJoinBuffer) return `None`.

**`PipelineEngine`**:
- `pub ring_buffer_drops: RingBufferDropCounters` field
- `ring_buffer_operator_kind(def: &FeatureDef) -> Option<&'static str>` maps FeatureDef variants to op_kind strings
- `register()` pre-allocates handles for all ring-buffer operators in a stream
- `push_internal()`: after `op.push()`, calls `op.ring_buffer_drop_reason()` and increments counter (OBS-01)

### Task 2: /metrics emission (http.rs + tcp.rs)

**`http.rs`**: new `beava_ring_buffer_drops_total` block inserted after `beava_late_events_dropped_total`:
```
# HELP beava_ring_buffer_drops_total Events rejected by the sliding-window ring buffer, labelled by reason (too_old | too_new | pre_epoch)
# TYPE beava_ring_buffer_drops_total counter
beava_ring_buffer_drops_total{stream="<s>",operator_kind="<k>",reason="<r>"} <n>
```

**`tcp.rs`**: OBS-02 structural comment at the late-drop gate (~line 1753) confirming the two counters are mutually exclusive.

### Task 3: Integration tests (test_ring_buffer_drops_metric.rs)

**`bounded_labels`**: Establishes a per-key ring-buffer epoch far in the future, bypasses the watermark gate to push TooOld events, verifies:
- `ring_buffer_drops.total() >= 2` (count + sum operators each record the drop)
- Snapshot labels are all from the compile-time enum set
- `operator_kind` labels are all from the known static set
- Pushing 10 additional TooOld events does NOT grow the snapshot label count

**`counters_mutually_exclusive`**: Two-phase test:
1. Gate-drop path: `push_sales()` applies the watermark gate; a late event at wm−5 increments `late_drops` by 1 and leaves `ring_buffer_drops` unchanged (OBS-02)
2. Ring-buffer-drop path: `push_with_cascade()` bypasses the gate; a TooOld event (13s before ring epoch, 10s window) increments `ring_buffer_drops` and leaves `late_drops` unchanged (OBS-02)

## operator_kind Labels Registered with RingBufferDropCounters

| FeatureDef variant | operator_kind label |
|--------------------|---------------------|
| Count { .. } | `"count"` |
| Sum { .. } | `"sum"` |
| Avg { .. } | `"avg"` |
| Min { .. } | `"min"` |
| Max { .. } | `"max"` |
| Stddev { .. } | `"stddev"` |
| DistinctCount { .. } | `"distinct_count"` |
| ExactMin { .. } | `"exact_min"` |
| ExactMax { .. } | `"exact_max"` |
| Percentile / TopK / Last / Lag / Ema / LastN / First / FirstN / StreamJoinBuffer / EnrichFromTable / StreamStreamJoin / TableTableJoin / Derive | `None` (not ring-buffer, no registration) |

Label cardinality bound: `num_streams × 10 operator_kinds × 3 reasons = O(30 × streams)`.

## /metrics Scrape Sample (expected shape)

```
# HELP beava_ring_buffer_drops_total Events rejected by the sliding-window ring buffer, labelled by reason (too_old | too_new | pre_epoch)
# TYPE beava_ring_buffer_drops_total counter
beava_ring_buffer_drops_total{stream="purchases",operator_kind="count",reason="too_old"} 0
beava_ring_buffer_drops_total{stream="purchases",operator_kind="sum",reason="too_old"} 0
beava_ring_buffer_drops_total{stream="purchases",operator_kind="count",reason="too_new"} 0
beava_ring_buffer_drops_total{stream="purchases",operator_kind="sum",reason="too_new"} 0
beava_ring_buffer_drops_total{stream="purchases",operator_kind="count",reason="pre_epoch"} 0
beava_ring_buffer_drops_total{stream="purchases",operator_kind="sum",reason="pre_epoch"} 0
```

Labels only appear for streams with ring-buffer operators (pre-registered at registration time).

## Running Phase 46 Closed-Requirements Tally

| Req | Description | Plan |
|-----|-------------|------|
| CORR-01 | per-event event-time routing | 46-03 |
| CORR-02 | aggregate-table wm propagation | 46-03 |
| CORR-03 | join wm propagation | 46-03 |
| CORR-04 | replica backfill event-time | 46-05 |
| CORR-05 | eviction uses event-time clock | 46-05 |
| CORR-06 | fork watermark propagation | 46-05 |
| CORR-07 | snapshot watermark persistence | 46-05 |
| CORR-08 | watermarks.observe post-push | 46-05 |
| CORR-10 | ArcSwap dirty-set atomic swap | 46-07 |
| **OBS-01** | **ring-buffer drop counter** | **46-06** |
| **OBS-02** | **mutual-exclusivity invariant** | **46-06** |

Running total: **11 of 14 Phase 46 requirements closed**.

## Deviations from Plan

### Plan Deviation: No Arc<AtomicU64> fields on operator structs

The plan (Task 1, step 4) specified threading 3 `Arc<AtomicU64>` fields directly into each ring-buffer-owning operator struct. This was rejected because:
1. All operators derive `Serialize + Deserialize` — adding `Arc<AtomicU64>` fields without `#[serde(skip)]` would break snapshot serialization. Adding skip attributes would require reading/modifying 10+ operator definitions.
2. Changing `Operator::push()` to return drop reasons would require modifying 20+ trait implementations and all call sites.

**Adopted approach (Rule 2 — correctness):** `last_drop: Option<DropReason>` side-channel on `RingBuffer<T>` with `#[serde(skip)]`, read after push via `op.ring_buffer_drop_reason()` on `OperatorState`. This achieves identical observable behavior (drop detection after each push) with zero serialization risk and no trait signature changes.

### Auto-fix: ring_buffer_operator_kind exhaustiveness

The `ring_buffer_operator_kind` match required all `FeatureDef` variants including `EnrichFromTable`, `StreamStreamJoin`, `TableTableJoin` which were not listed in the plan's context. Added exhaustive `None` arms for all non-ring-buffer variants (Rule 1 — compile error fix).

## Known Stubs

None — all wiring is live. The counter increments in production push paths, emits via /metrics, and is covered by integration tests.

## Threat Flags

None — no new network endpoints, auth paths, or trust-boundary schema changes. The new Prometheus lines are read-only observability output on the existing /metrics endpoint.

## Self-Check: PASSED

- `src/engine/event_time.rs` — modified, contains `DropReason` + `RingBufferDropCounters`
- `src/engine/window.rs` — modified, contains `last_drop` field + `take_last_drop()`
- `src/engine/pipeline.rs` — modified, contains `ring_buffer_drops` field + `ring_buffer_operator_kind`
- `src/server/http.rs` — modified, contains `beava_ring_buffer_drops_total`
- `src/server/tcp.rs` — modified, contains OBS-02 comment
- `tests/test_ring_buffer_drops_metric.rs` — modified, 0 `#[ignore]`, 0 `panic!`, 2 green tests
- Commits `e62f3ac`, `6575999`, `7e5af28` confirmed in `git log`
- `cargo build --release` green
- `cargo test --release --lib` green (788 tests)
- `cargo test --test test_ring_buffer_drops_metric --release` green × 3 runs
