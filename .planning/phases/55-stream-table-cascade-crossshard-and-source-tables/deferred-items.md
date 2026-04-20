# Phase 55 — Deferred Items

Items discovered during Phase 55 execution that are out of scope for the
phase close and must be handled by a later pass or a follow-up ticket.
Carried forward as 55-NEXT.

## Pre-existing (inherited from Phase 54)

### Pre-existing: `tests/test_concurrent.rs` — 6 failing tests

**State:** FAILED on HEAD prior to Phase 55 (verified via `git stash` +
re-running `cargo test --release --test test_concurrent` against commit
`9a1a78b` before any Wave 4 test-file edits landed).

**Failure mode:** All 6 tests panic with
`assertion left == right failed: PUSH should succeed. left: 1 right: 0`.
The PUSH path returns status 1 (ERROR) instead of 0 (OK) when many threads
hammer the same state in parallel.

**Tests failing:**
- `concurrent_push_and_get`
- `fan_out_under_concurrency`
- `multi_stream_parallel_push`
- `same_stream_different_keys_concurrent`
- `set_mset_concurrent_with_push`
- `test_enriched_concurrent_clients`

**Root cause (hypothesis):** Unknown — predates Phase 55. Matches the
pattern noted in 55-01-SUMMARY's "Wiring Follow-Up" section
("`test_concurrent`: 6 failed — pre-existing (confirmed against pre-wiring
tip of branch; orthogonal to this change)") and 55-03-SUMMARY.

**Scope ruling:** Out of scope for Phase 55 per the Wave 4 deviation-rule
scope boundary ("Only auto-fix issues DIRECTLY caused by the current task's
changes"). The 6 failures were not introduced by any Phase 55 wave; they
run against a shared `AppState` concurrency path that predates the phase.

**Action:** Phase 55 VERIFICATION.md documents these as Known Pre-existing
Issues; full-suite green claim is scoped to lib + Phase 55 integration
tests + state-inmem. A dedicated triage ticket is filed as 55-NEXT item
below.

## 55-NEXT (follow-up tickets filed during Phase 55 execution)

### 55-NEXT #1 — Cross-shard fan-out in boot rematerialization

**Origin:** Plan 55-03 Deviation 3 (Task 2).
**Scope:** ~80 LOC in `src/engine/pipeline.rs` + a two-shard unit test.
**Detail:** Thread `CascadeBuffer` + `SyncCascadeTargets` through boot
replay so cross-shard cascade outputs at v8→v9 boot migration land on
`hash(output_key) % N`. Wave 3 left `sibling_shards=None` (same-shard fast
path only), which is fully correct at N=1 (the overwhelming majority of
pre-Phase-55 installations) and a gap at N>1.

### 55-NEXT #2 — Triage `tests/test_concurrent.rs` 6 failing tests

**Origin:** Pre-existing since at least Phase 54 Wave 4.
**Scope:** Unknown until triaged. Likely 1–2 days of investigation.
**Detail:** See "Pre-existing" section above. Could live in a dedicated
Phase 56 or v1.3 triage plan.

### 55-NEXT #3 — Counter hoist (Phase 61 territory)

**Origin:** Plan 55-01 Task 2 / 55-RESEARCH § Don't Hand-Roll Metrics.
**Scope:** Hoist `metrics_util::Registry::get_or_create_counter` from
per-event to stream-register time (~3.5% of CPU per pprof).

### 55-NEXT #4 — Per-event cascade-stream log records

**Origin:** Plan 55-00 Area A deferred; CONTEXT deferred section.
**Scope:** Per-event granularity for external sink tail.

### 55-NEXT #5 — Variable-length `source_lsn_bytes` (MySQL GTID)

**Origin:** Plan 55-02 D-B3.
**Scope:** Customer-driven.

### 55-NEXT #6 — `BEAVA_BATCH_MAX` env row-count cap

**Origin:** Plan 55-02 Deviation 4.
**Scope:** Currently the `ingest_layers` body limit enforces a byte-level
cap (`BEAVA_HTTP_MAX_BODY`); a row-count-based cap is not wired.

### 55-NEXT #7 — ROADMAP.md Phase 55 SC #7 EPS-number fix (cosmetic)

**Origin:** CONTEXT §D-D3 / RESEARCH "Perf Gate Number Discrepancy".
**Scope:** ROADMAP.md cites 935,000 for Phase 55 perf gate; CONTEXT +
VALIDATION both cite 1,138,529 (85% of 1,339,446 Phase 54 baseline).
Update ROADMAP to match.

### 55-NEXT #8 — Cosmetic: graceful bench.py client shutdown at EOS

**Origin:** Plan 55-04 Task 1 — perf-gate run exhibited 8/8 clients
exiting non-zero on their final flush due to EOS backpressure wave. Not
a correctness or throughput regression (steady-state window 55s at
1.3–1.4M EPS before the wave); cosmetic blemish in the summary output.
**Scope:** `bench.py` could re-drain the send buffer before the final
flush instead of surfacing as `ProtocolError`.

### 55-NEXT #9 — `iter_entities` streaming for clear-downstream-rows

**Origin:** Plan 55-03 Task 2.
**Scope:** Current clear-downstream helper materializes full entity list
before RMW; stream via fjall range iterator without materializing. Phase
57-ish territory.

### 55-NEXT #10 — Incremental replay for very large logs

**Origin:** Plan 55-03 Task 2.
**Scope:** For rematerialization on shards with billions of entries,
checkpointed replay would avoid redoing work on crash. Not needed for the
one-shot v8→v9 transition.

### 55-NEXT #11 — Graph-hash snapshot gating

**Origin:** Plan 55-03 Task 1 deferred.
**Scope:** Beyond `schema_version`, a `graph_hash` field on
`SnapshotHeader` could trigger rematerialization when operator
definitions drift.
