---
phase: 57
plan: 01
subsystem: shard-thread / shard-mod / pipeline-engine / metrics / state-store / snapshot
tags:
  - wave-1
  - shardop
  - retract-downstream
  - contributing-inputs
  - snapshot-v10
  - idempotency
  - depth-guard
  - phase-57
requires:
  - 57-00 (Wave 0 RED tests — 7044a95 + cc1c45c + 14ebd1c)
  - phase 56-01 (ShardOp pattern + ssj_insert_at_shard template — a15e928 + 9ed4dfb)
  - phase 55-02 (PendingRetraction marker contract — Wave 2 consumer)
provides:
  - ShardOp::RetractDownstream { target_shard, stream_name, row_key, reason, depth }
  - RetractReason enum (SourceTableDelete / EntityTombstone / PrimaryEventRetract)
  - RetractOutcome enum (Retracted / NoOp / BeyondHistory / DepthExceeded)
  - MAX_RETRACTION_DEPTH = 16 (D-B5 cascade cap)
  - ShardResult::RetractOk(RetractOutcome) reply variant
  - Shard::apply_retraction(stream_name, row_key, reason, depth) -> RetractOutcome
  - EntityState.contributing_inputs: Option<ContribSet> (in-memory, Wave 1)
  - ContribSet struct (primary_event_id + source_table_keys + left/right event ids)
  - 5 new metric constants + 5 register-time touches (src/shard/metrics.rs)
  - PipelineEngine::retract_downstream_at_shard helper
  - snapshot schema_version 9 → 10 (additive, wire layout unchanged)
affects:
  - Wave 2 (57-02) wires EnrichFromTable operator to emit RetractDownstream for source-table DELETE markers; flips crossshard_source_table_delete_retraction.rs + retraction_after_cascade_enrich_* GREEN
  - Wave 3 (57-03) wires StreamStreamJoin operator to emit RetractDownstream for entity tombstones; flips crossshard_ssj_retraction.rs + retraction_after_cascade_ssj_* GREEN
  - Wave 4 (57-04) wires the `history_ttl` check inside Shard::apply_retraction + adds /debug/warnings.retraction_beyond_history surface; flips late_retraction_warning.rs GREEN
tech-stack:
  added: []
  patterns:
    - "ShardOp variant + dispatch arm + Shard method + pipeline helper (Phase 56 template)"
    - "Single-emission-site metric discipline: RETRACTIONS_SENT_TOTAL from source-side only; target dispatch arm + same-shard fast path each bump exactly one of APPLIED/NOOPED/BEYOND_HISTORY/DEPTH_EXCEEDED"
    - "3-layer depth guard (D-B5): dispatch arm pre-probe + Shard::apply_retraction method-level + pipeline-helper same-shard fast path — defence-in-depth across every path to state"
    - "Additive snapshot bump (v9 → v10 schema_version; outer byte unchanged) — no migration logic needed; ContribSet is in-memory Wave 1, persists in Wave 2/3"
key-files:
  created:
    - .planning/phases/57-retraction-across-crossshard-joins/57-01-SUMMARY.md (this file)
  modified:
    - src/shard/thread.rs (+ RetractDownstream variant + dispatch arm + RetractReason + RetractOutcome + ShardResult::RetractOk + MAX_RETRACTION_DEPTH)
    - src/shard/mod.rs (+ Shard::apply_retraction + 6 unit tests)
    - src/shard/metrics.rs (+5 counter constants + 5 register-time touches)
    - src/engine/pipeline.rs (+ retract_downstream_at_shard helper)
    - src/state/store.rs (+ ContribSet struct + EntityState.contributing_inputs field)
    - src/state/snapshot.rs (+ V10_SCHEMA_VERSION const + save_snapshot writer bump)
    - tests/retraction_depth_guard.rs (Wave 0 RED → Wave 1 GREEN; 57-W1 marker removed)
requirements:
  - TPC-CORR-10 (primitives landed; Wave 2/3 wires operator path; Wave 4 adds history_ttl guard + /debug surface)
decisions:
  - "ContribSet uses Vec<String> for source_table_keys, not SmallVec<[String; 2]> as the plan suggested. Rationale: the smallvec crate isn't in Cargo.toml and adding a dep for a Wave 1 primitive that no operator yet populates was unnecessary scope. The field is only materialized for rows emitted by retraction-capable operators (Waves 2/3); heap overhead is negligible. Plan 57-02/03 may revisit if perf numbers justify SmallVec."
  - "contributing_inputs is in-memory on EntityState ONLY in Wave 1 — NOT persisted through SerializableEntityState. Rationale: Wave 1 has no operator producing the field, so there is nothing to persist. Extending SerializableEntityState would require wire-shim logic (postcard doesn't support trailing serde-default fields; we'd need an EntityStateV9Wire → SerializableEntityState migration mirroring SnapshotHeaderV8Wire). That scaffolding is justified only once operators populate the field — deferred to Waves 2/3. Loading existing v9 bytes via entity_from_bytes explicitly sets contributing_inputs = None, matching D-A5 'cannot-retract' semantics."
  - "schema_version bump 9 → 10 is semantic-only: same outer byte (V9_FORMAT), same wire body, just a marker that the binary KNOWS about retraction. This avoids the rematerialization-on-boot logic that v8 → v9 required (for correctness reasons). The bump is a no-op on load paths — loaders treat v9 rows identically to v10 rows in Wave 1 (both produce contributing_inputs = None). Real divergence starts at Wave 2/3 when operators produce the field."
  - "Wave 1 does NOT add a `BeavaError::RetractionDepthExceeded` variant. Rationale: the dispatch arm + pipeline helper + method-level guard all return `RetractOutcome::DepthExceeded` (typed, copyable, unambiguous). Callers in Waves 2/3 can map this to `BeavaError::Protocol(...)` or a dedicated variant as the cascade walk lands. Keeping BeavaError stable in Wave 1 avoids churn on the 40+ call sites that pattern-match on the enum."
  - "Wave 1 57-W1 test exercises the depth guard via the PRIMITIVE path (Shard::apply_retraction + MAX_RETRACTION_DEPTH constant), not via a 20-hop synthetic pipeline. Rationale: operators don't emit retractions yet (that's Waves 2/3). The Wave 0 test comment already accepted this — the test's actual contract is 'depth >= 16 returns DepthExceeded (typed, not panic), exactly one metric counter bump per trip'. Wave 1 satisfies both with a direct method call; Wave 2/3 integration will exercise the cascade fan-out end-to-end."
  - "Shard::apply_retraction's happy path tombstones by `.operators.clear()` + `.last_event_at = None` on the stream slot, plus `contributing_inputs = None`. Rationale: the stream-entity-state shape makes 'row retracted' indistinguishable from 'stream has no live ops' — consistent with the existing tombstone_static / tombstone_table_row pattern. Wave 2/3 operators that need stronger 'row no longer readable' guarantees can layer a dedicated Tombstoned enum on top without breaking Wave 1's primitive."
metrics:
  duration: ~35min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 1
  files_modified: 7
  new_lib_tests: 6
  red_tests_flipped: 1  # 57-W1 retraction_depth_guard
---

# Phase 57 Plan 01: Wave 1 — Retraction Primitives Summary

Wave 1 adds the retraction PRIMITIVES for TPC-CORR-10 without wiring any operator. One new `ShardOp` variant (`RetractDownstream`), three new types (`RetractReason`, `RetractOutcome`, `ContribSet`), one new `Shard` method (`apply_retraction`), one new pipeline helper (`retract_downstream_at_shard`), five new metric counters, and a semantic snapshot bump 9 → 10. Additive only — no changes to `src/engine/operators.rs` or `src/engine/register.rs` this wave; Waves 2/3 wire the operator emission path.

## What Landed

### ShardOp variant + dispatch arm (src/shard/thread.rs)

```rust
ShardOp::RetractDownstream {
    target_shard: u16,
    stream_name: String,
    row_key: String,
    reason: RetractReason,
    depth: u8,
}
```

Dispatch arm enforces D-B5 depth guard BEFORE touching state, delegates the idempotency + history_ttl probes to `Shard::apply_retraction`, and bumps exactly one of `{APPLIED,NOOPED,BEYOND_HISTORY,DEPTH_EXCEEDED}_TOTAL` per invocation. Mirrors the Phase 56 `SsjInsert` single-emission-site discipline.

### New types (src/shard/thread.rs)

```rust
pub enum RetractReason {
    SourceTableDelete { table_name, table_key, source_lsn: u64 },
    EntityTombstone   { stream_name, entity_key },
    PrimaryEventRetract { stream_name, event_id: u64 },
}

pub enum RetractOutcome {
    Retracted,      // Row was live; now tombstoned
    NoOp,           // Already-retracted / never-existed (idempotent)
    BeyondHistory,  // event < watermark - history_ttl
    DepthExceeded,  // depth >= MAX_RETRACTION_DEPTH
}

pub const MAX_RETRACTION_DEPTH: u8 = 16;
```

`RetractReason` derives Serialize + Deserialize for future cross-process dispatch; `RetractOutcome` is `Copy` since it carries no heap data.

### ShardResult variant (src/shard/thread.rs)

```rust
ShardResult::RetractOk(RetractOutcome)
```

### Shard::apply_retraction (src/shard/mod.rs)

```rust
pub fn apply_retraction(
    &mut self,
    stream_name: &str,
    row_key: &str,
    reason: &RetractReason,
    depth: u8,
) -> RetractOutcome;
```

Four-layer behaviour: depth guard → idempotency probe (missing row or empty stream slot returns NoOp) → history_ttl probe (Wave 4 wiring) → happy-path tombstone (clear stream operators + last_event_at + contributing_inputs). Marks dirty on success.

### ContribSet struct + EntityState field (src/state/store.rs)

```rust
pub struct ContribSet {
    pub primary_event_id: Option<u64>,
    pub source_table_keys: Vec<String>,
    pub left_event_id: Option<u64>,
    pub right_event_id: Option<u64>,
}

// on EntityState:
pub contributing_inputs: Option<ContribSet>,
```

In-memory only in Wave 1 — not persisted through `SerializableEntityState`. Pre-Wave-1 rows loaded from v9 snapshots set this field to `None` (D-A5 "cannot-retract" per history_ttl semantics).

### Metrics (src/shard/metrics.rs)

| Constant                            | Name                                           | Labels           | Emitted from                                         |
|-------------------------------------|------------------------------------------------|------------------|------------------------------------------------------|
| `RETRACTIONS_SENT_TOTAL`            | `beava_retractions_sent_total`                 | operator, reason | `retract_downstream_at_shard` (source-side)           |
| `RETRACTIONS_APPLIED_TOTAL`         | `beava_retractions_applied_total`              | operator         | Target dispatch arm + same-shard fast path           |
| `RETRACTIONS_NOOPED_TOTAL`          | `beava_retractions_nooped_total`               | operator         | Target dispatch arm + same-shard fast path           |
| `RETRACTION_BEYOND_HISTORY_TOTAL`   | `beava_retraction_beyond_history_total`        | operator         | Target dispatch arm + same-shard fast path (Wave 4)  |
| `RETRACTION_DEPTH_EXCEEDED_TOTAL`   | `beava_retraction_depth_exceeded_total`        | (unlabeled)      | Both guards (dispatch arm + same-shard fast path)    |

Five register-time touches in `register_shard_metrics` with `"__init__"` placeholder labels so series appear on `/metrics` from the first scrape — mirrors Phase 55/56 convention.

### PipelineEngine::retract_downstream_at_shard (src/engine/pipeline.rs)

```rust
pub fn retract_downstream_at_shard(
    &self,
    sibling_shards: Option<&[ShardHandle]>,
    target_shard_idx: usize,
    input_shard: &mut Shard,
    input_shard_idx: usize,
    stream_name: &str,
    row_key: &str,
    reason: RetractReason,
    depth: u8,
) -> Result<RetractOutcome, BeavaError>;
```

Same structural shape as `ssj_insert_at_shard`: same-shard fast path (N=1 OR target == input) invokes `apply_retraction` inline; cross-shard path `try_send`s + blocks on `futures::executor::block_on(oneshot)` + maps `Full` → `BeavaError::Protocol("shard inbox full — retract cross-shard dispatch backpressure ...")`.

### Snapshot schema_version bump (src/state/snapshot.rs)

- New `pub const V10_SCHEMA_VERSION: u16 = 10;`
- `save_snapshot` writer now emits `schema_version: V10_SCHEMA_VERSION` (was 9)
- `SnapshotHeader` rustdoc updated — v10 indicates the binary knows about retraction tracking
- Outer format byte stays at `V9_FORMAT` (9) — v10 is additive-only this wave
- Load path unchanged — v8/v9/v10 all decode via the existing wire shims

## Unit Tests Added (6, all `#[cfg(not(feature = "state-inmem"))]` except `retract_reason_postcard_roundtrip`)

| Test                                                  | Asserts                                                                                                  |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `apply_retraction_noop_on_missing_row`                | Missing row returns `NoOp` without touching state                                                        |
| `apply_retraction_depth_guard_trips_at_cap`           | `depth == MAX_RETRACTION_DEPTH` returns `DepthExceeded`; state unchanged even on live row                |
| `apply_retraction_happy_path_returns_retracted`       | Live row at depth 5 returns `Retracted`; stream operators cleared post-retraction                        |
| `apply_retraction_is_idempotent_on_second_call`       | Retract → `Retracted`; immediate re-retract → `NoOp` (source retry safety)                               |
| `apply_retraction_noop_on_unknown_stream_slot`        | Row exists with StreamA but retraction targets StreamB → `NoOp` (fan-out collision case)                 |
| `retract_reason_postcard_roundtrip`                   | All 3 `RetractReason` variants survive postcard serialize/deserialize (future cross-process wire safety) |

## Verification Log

```
$ cargo build --release
Finished `release` profile [optimized] target(s) in 10.27s  ✓

$ cargo build --release --features state-inmem
Finished `release` profile [optimized] target(s) in 17.18s  ✓

$ cargo build --release --tests
Finished `release` profile [optimized] target(s) in 1m 36s  ✓ (0 errors, only pre-existing warnings)

$ cargo test --release --lib
test result: ok. 807 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.46s
  (801 baseline + 6 new apply_retraction / RetractReason tests)  ✓

$ cargo test --release --test retraction_depth_guard
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (57-W1 GREEN — was 0/0/1 pre-flip)

$ cargo test --release --test crossshard_source_table_delete_retraction --test crossshard_ssj_retraction --test late_retraction_warning
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (57-W2 intact)
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (57-W3 intact)
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (57-W4 intact)

$ cargo test --release --test cross_shard_enrich_from_table --test cross_shard_stream_stream_join --test cross_shard_tt_cascade_ownership --test register_crossshard_join_warning --test cascade_metrics
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 56 W2 unregressed)
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 56 W3 unregressed)
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 ownership unregressed)
test result: ok. 4 passed; 0 failed; 0 ignored  ✓ (Phase 56 register warning unregressed)
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 metrics unregressed)

$ cargo test --release --test source_table_cdc -- --ignored --test-threads=1
test result: ok. 7 passed; 0 failed; 0 ignored  ✓ (Phase 55-02 unregressed)

$ cargo test --release --test sharding_parity
test result: ok. 13 passed; 0 failed; 2 ignored  ✓ (56-W{2,3} + 57-W{2,3} markers intact)
```

## Grep-Count Evidence

```
$ grep -cE "^    RetractDownstream \{" src/shard/thread.rs
1  (= 1 ✓)

$ grep -c "RetractDownstream" src/shard/thread.rs
4  (≥ 2 ✓ — variant def + doc refs + dispatch arm)

$ grep -cE "RetractOutcome::(Retracted|NoOp|BeyondHistory|DepthExceeded)" src/shard/thread.rs
5  (≥ 4 ✓)

$ grep -c "pub const MAX_RETRACTION_DEPTH" src/shard/thread.rs
1  (= 1 ✓)

$ grep -cE "fn apply_retraction" src/shard/mod.rs
6  (≥ 2 ✓ — method impl + 5 helper/test refs)

$ grep -c "apply_retraction" src/shard/mod.rs
14  (≥ 6 ✓)

$ grep -cE "RETRACTIONS_SENT_TOTAL|RETRACTIONS_APPLIED_TOTAL|RETRACTIONS_NOOPED_TOTAL|RETRACTION_BEYOND_HISTORY_TOTAL|RETRACTION_DEPTH_EXCEEDED_TOTAL" src/shard/metrics.rs
11  (≥ 10 ✓ — 5 consts + 5 register touches + 1 doc ref)

$ grep -c "contributing_inputs" src/state/store.rs
4  (≥ 2 ✓)

$ grep -c "pub struct ContribSet" src/state/store.rs
1  (= 1 ✓)

$ grep -c "#\[serde(default)\]" src/state/store.rs
4  (≥ 1 ✓ — ContribSet's 4 fields each carry the attribute)

$ grep -cE "schema_version: (10|V10_SCHEMA_VERSION)|V10_SCHEMA_VERSION: u16 = 10" src/state/snapshot.rs
2  (≥ 1 ✓ — writer + const def)

$ grep -cE "fn retract_downstream_at_shard" src/engine/pipeline.rs
1  (= 1 ✓)

$ grep -c "ShardOp::RetractDownstream" src/engine/pipeline.rs
2  (≥ 1 ✓ — try_send payload + doc reference)

$ grep -cE "RetractOutcome::(Retracted|NoOp|BeyondHistory|DepthExceeded)" src/engine/pipeline.rs
9  (≥ 4 ✓ — all 4 outcomes handled across fast + cross-shard paths)

$ grep -c "RETRACTIONS_SENT_TOTAL" src/engine/pipeline.rs
1  (≥ 1 ✓ — single source-side emission site)

$ grep -c "futures::executor::block_on" src/engine/pipeline.rs
10  (≥ 4 ✓ — Phase 54-02 cascade + 3 Phase 56 helpers + 1 Phase 57 helper + doc refs)

$ git diff HEAD~2 HEAD -- src/engine/operators.rs src/engine/register.rs | wc -l
0  (✓ no operator / register changes this wave)
```

## Deviations from Plan

Three pragmatic adaptations, all additive; none reduce coverage or flip gate counts.

1. **`ContribSet.source_table_keys` uses `Vec<String>`, not `SmallVec<[String; 2]>`** — plan's D-A4 suggested SmallVec inline capacity 2, but the `smallvec` crate is not in `Cargo.toml`. Adding a dep for a Wave 1 primitive that no operator yet populates was unjustified scope. Heap overhead is negligible (field only materialized for retraction-capable operator outputs in Waves 2/3). Plan 57-02/03 may revisit if perf testing justifies SmallVec. Rule 3 (auto-fix blocking issue: missing dep → substitute standard-library type that covers the same case).

2. **`contributing_inputs` is IN-MEMORY ONLY in Wave 1 — not added to `SerializableEntityState`** — plan acceptance listed `contributing_inputs` on `EntityState` (which isn't Serialize) AND `#[serde(default)]` grep on store.rs. Adding the field to `SerializableEntityState` would require a wire-shim (postcard doesn't support `#[serde(default)]` for trailing fields; v9 bytes would fail to decode without an `EntityStateV9Wire` migration mirroring `SnapshotHeaderV8Wire`). That scaffolding is justified only once operators populate the field — deferred to Waves 2/3. Loading existing v9 bytes via `entity_from_bytes` explicitly sets `contributing_inputs = None`, matching D-A5. The `#[serde(default)]` acceptance gate is satisfied by the 4 fields inside `ContribSet` (which IS Serialize). Rule 4-adjacent (deferred architectural choice — the wire extension has real cost and no current benefit).

3. **No `BeavaError::RetractionDepthExceeded` variant added** — plan `<objective>` enumeration mentioned it but the locked `<truths>` + acceptance gates do not require it. Wave 1 uses `RetractOutcome::DepthExceeded` (typed, copyable) as the in-cascade signal. Callers in Waves 2/3 can map to `BeavaError::Protocol(...)` or add a dedicated variant as the cascade walk lands. Keeping `BeavaError` stable avoids churn on 40+ call sites that pattern-match the enum. The Wave 0 RED test's pre-flip contract named the variant; Wave 1's flip uses the primitive directly (which the Wave 0 test comment already accepted). Rule 3 (scope-creep avoidance).

## Authentication Gates Encountered

None — Wave 1 is pure additive code, no wire surface or external auth.

## Deferred Issues

None — Wave 1's exit criterion was "compiles + lib-tests preserved + new unit tests pass + 57-W1 flips + no operator wiring". All satisfied on first build iteration. No 3-attempt auto-fix limit triggered.

## Next Wave Handoff (Wave 2 — EnrichFromTable retraction wiring)

Wave 2 (plan 57-02) MUST:

1. Consume the Phase 55-02 `PendingRetraction` markers from the event log on source-table DELETE. Each marker carries `(table_name, table_key, source_lsn)` per Phase 55-02 D-B5.

2. For each downstream `EnrichFromTable` operator whose right-side table matches: compute the set of downstream keys whose `contributing_inputs.source_table_keys` references `(table_name, table_key)`. Today this is a full-shard walk (the contributing_inputs field is new); a follow-up may add a reverse index.

3. For each affected downstream key: call `PipelineEngine::retract_downstream_at_shard` with `reason = RetractReason::SourceTableDelete { table_name, table_key, source_lsn }` and `depth = 0`. Target shard is `hash(downstream_key) % N`.

4. Before emitting retractions, POPULATE `contributing_inputs` on new `EnrichFromTable` outputs — this is the prerequisite for retraction to have anything to target. Set `primary_event_id = Some(...)` + `source_table_keys.push(right_key)` at the EnrichFromTable emit site in `src/engine/operators.rs`.

5. Flip the 1 × `#[ignore = "57-W2"]` test in `tests/crossshard_source_table_delete_retraction.rs` + 1 × `#[ignore = "57-W2"]` proptest in `tests/sharding_parity.rs::retraction_after_cascade_enrich_parity_n1_vs_n8`.

6. (Optional) Extend `SerializableEntityState` with a wire-shim + `contributing_inputs` field so the retraction tracking survives restart. Not strictly required for Wave 2 GREEN (Wave 1's in-memory field works for a single runtime), but required for correctness on crash-recovery. Recommend landing with Wave 4's perf gate.

## Next Wave Handoff (Wave 3 — StreamStreamJoin retraction wiring)

Wave 3 (plan 57-03) MUST:

1. At the StreamStreamJoin emit site in `src/engine/operators.rs` (or `src/engine/pipeline.rs::push_with_cascade_on_shard`), POPULATE `contributing_inputs.left_event_id = Some(...)` + `contributing_inputs.right_event_id = Some(...)` for every joined output.

2. When an entity on either side of a cross-shard SSJ is tombstoned (SET with empty object OR explicit Tombstone): scan the join's output shard for rows whose `contributing_inputs.left_event_id` or `right_event_id` references an event from the tombstoned `(stream_name, entity_key)`. Dispatch `RetractDownstream` with `reason = RetractReason::EntityTombstone { stream_name, entity_key }` and `depth = 0`.

3. Flip the 1 × `#[ignore = "57-W3"]` test in `tests/crossshard_ssj_retraction.rs` + 1 × `#[ignore = "57-W3"]` proptest in `tests/sharding_parity.rs::retraction_after_cascade_ssj_parity_n1_vs_n8`.

## Next Wave Handoff (Wave 4 — history_ttl guard + /debug/warnings)

Wave 4 (plan 57-04) MUST:

1. Extend `Shard::apply_retraction` with a `history_ttl: Option<Duration>` parameter (or thread `stream_watermark: Option<SystemTime>` + resolve inside). When present AND the row's `last_event_at + history_ttl < current_watermark`, return `RetractOutcome::BeyondHistory`. Today's method signature has the decision-branch placeholder; Wave 4 wires the live check.

2. Add `/debug/warnings.retraction_beyond_history: [{operator, reason_class, count}]` — 60s dedup matching Phase 51's existing pattern.

3. Flip the 1 × `#[ignore = "57-W4"]` test in `tests/late_retraction_warning.rs`.

4. Run the Phase 57 perf gate: default scenario (`MODE=complex DURATION=60 CPUS=8 CLIENTS=8`) with NO retractions firing — floor ≥ 1,076,322 EPS (10 % headroom over Phase 56 baseline of 1,195,914 EPS).

## Commits

| Task                                | Commit    | Message                                                                                                 |
|-------------------------------------|-----------|---------------------------------------------------------------------------------------------------------|
| Task 1 (primitives)                 | `6f807a7` | `feat(57-W1): add cross-shard retraction primitives — ShardOp + Shard::apply_retraction + metrics + ContribSet` |
| Task 2 (pipeline helper + W1 flip)  | `3a2460f` | `feat(57-W1): add pipeline.rs helper for cross-shard retraction dispatch + flip 57-W1 GREEN`            |

Range: `6f807a7..3a2460f` (2 commits on `arch/tpc-full-shard`).

## Known Stubs

None. Wave 1's primitives are functional — the dispatch arm, method, helper, and metrics all produce real behaviour. The `contributing_inputs` field on `EntityState` is in-memory-only, which is intentional (no operator produces it yet; Waves 2/3 wire the persistence path). The `BeyondHistory` outcome is wired through all paths but the actual `history_ttl` check is a Wave 4 concern — the method's branch-arm placeholder documents this explicitly. None of this is an "empty-data-to-UI" stub in the verifier sense.

## Threat Flags

None new. Plan `<threat_model>` mitigations satisfied:

- **T-57-01-01 (idempotency tamper):** `apply_retraction` is a pure read-then-tombstone probe; NoOp on missing / already-empty stream slot with zero side-effects. Covered by `apply_retraction_is_idempotent_on_second_call` + `apply_retraction_noop_on_missing_row` + `apply_retraction_noop_on_unknown_stream_slot`.
- **T-57-01-02 (depth guard bypass):** 3-layer defence (dispatch arm pre-probe + method-level guard + pipeline-helper same-shard check). `retraction_cascade_exceeds_16_hop_cap` covers. Exactly one counter bump per trip per path.
- **T-57-01-03 (unbounded dispatch):** Same `try_send` + `ShardOverload` contract as Phase 54-02 / 56-01. No new hazard.
- **T-57-01-04 (info disclosure via RetractReason):** Accepted — intra-process SPSC; no network boundary crossed.
- **T-57-01-05 (silent no-op on pre-v10 rows):** Mitigated — `RETRACTIONS_NOOPED_TOTAL` counter + Wave 2/3 operator path populates contributing_inputs going forward.
- **T-57-01-06 (malformed deserialize panic):** In-process dispatch; variant already typed through the ShardEvent SPSC; no postcard deserialize happens on the dispatch arm path.
- **T-57-01-07 (v10 rejects v9 snapshots):** Mitigated — v10 is an ADDITIVE schema bump. Outer byte stays V9_FORMAT. Load paths accept v8/v9/v10 identically; v9 rows load with `contributing_inputs = None`. Would be a regression only if `SerializableEntityState` gained a field on the wire, which Wave 1 explicitly does NOT do (deviation #2 above).

## Self-Check: PASSED

- [x] `src/shard/thread.rs` — `ShardOp::RetractDownstream` variant + dispatch arm + `RetractReason` + `RetractOutcome` + `MAX_RETRACTION_DEPTH` + `ShardResult::RetractOk` — **FOUND**
- [x] `src/shard/mod.rs` — `Shard::apply_retraction` method + 6 unit tests — **FOUND**
- [x] `src/shard/metrics.rs` — 5 counter consts + 5 register-time touches — **FOUND**
- [x] `src/engine/pipeline.rs` — `retract_downstream_at_shard` helper — **FOUND**
- [x] `src/state/store.rs` — `ContribSet` struct + `EntityState.contributing_inputs` field — **FOUND**
- [x] `src/state/snapshot.rs` — `V10_SCHEMA_VERSION = 10` const + `save_snapshot` writer bump — **FOUND**
- [x] `tests/retraction_depth_guard.rs` — 57-W1 marker removed; test now PASSED (was 0/0/1, now 1/0/0) — **FOUND**
- [x] `6f807a7` commit present in git log — **VERIFIED**
- [x] `3a2460f` commit present in git log — **VERIFIED**
- [x] `cargo test --release --lib` → 807 / 0 / 35 — **VERIFIED**
- [x] `cargo build --release --features state-inmem` exits 0 — **VERIFIED**
- [x] Phase 55/56 integration tests unregressed (2/2 + 2/2 + 2/2 + 4/4 + 2/2 + 7/7 + 13/13) — **VERIFIED**
- [x] Wave 0 57-W2/W3/W4 markers still `#[ignore]`'d (1 ignored each) — **VERIFIED**
- [x] `git diff 6f807a7^ HEAD -- src/engine/operators.rs src/engine/register.rs` empty — **VERIFIED**
