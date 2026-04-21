---
phase: 56
plan: 01
subsystem: shard-thread / pipeline-engine / metrics
tags:
  - wave-1
  - shardop
  - cross-shard-primitives
  - enrich-from-table
  - stream-stream-join
  - additive
requires:
  - 56-00 (Wave 0 RED tests landed — 97caab0 + 1304bb5)
  - phase 54-02 (cascade_table_upsert_on_shard scatter-gather pattern)
  - phase 55-01 (CascadeBuffer + SHARD_INBOX_HIGH_WATERMARK_TOTAL)
provides:
  - ShardOp::ReadEntityAt / ReadEntityBatch / SsjInsert variants + dispatch arms
  - ShardResult::ReadEntityOk / ReadEntityBatchOk / SsjInsertOk
  - MAX_ENRICH_BATCH_KEYS = 4096 (T-56-01-01 DoS guard)
  - Shard::read_entity_at(table, key) -> Option<EntityState>
  - Shard::apply_ssj_insert(join_id, side, join_key, event, within_ms) -> Vec<Map>
  - 5 new counter constants + register-time touches in src/shard/metrics.rs
  - PipelineEngine::read_entity_at_shard / read_entity_batch_at_shard / ssj_insert_at_shard
affects:
  - Wave 2 (56-02) consumes read_entity_at_shard / read_entity_batch_at_shard in EnrichFromTable operator path
  - Wave 3 (56-03) consumes ssj_insert_at_shard in StreamStreamJoin operator path + relaxes register() + extends /debug/warnings
  - Wave 4 (56-04) measures p99 latency + EPS floor with the primitives exercised
tech-stack:
  added: []
  patterns:
    - "crossbeam::channel::bounded SPSC + futures::executor::block_on oneshot (Phase 54-02 template)"
    - "try_send + BeavaError::Protocol('shard inbox full ...') on Full (ShardOverload mapping)"
    - "touch-with-zero label placeholder in register_shard_metrics (matches Phase 55 cascade metrics pattern)"
    - "synthetic '__ssj__' stream slot on EntityState for relocated join buffer ownership"
key-files:
  created: []
  modified:
    - src/shard/thread.rs (+3 ShardOp variants + 3 dispatch arms + 3 ShardResult variants + MAX_ENRICH_BATCH_KEYS const)
    - src/shard/mod.rs (+2 Shard methods + 5 unit tests)
    - src/shard/metrics.rs (+5 counter constants + 5 register-time touches)
    - src/engine/pipeline.rs (+3 PipelineEngine helpers + module-level deadlock-analysis comment block)
    - .planning/phases/56-enrich-from-table-and-stream-stream-join-crossshard/56-01-SUMMARY.md (this file)
requirements:
  - TPC-CORR-08 (primitives in place; Wave 2 wires operator)
  - TPC-CORR-09 (primitives in place; Wave 3 wires operator + relaxes register)
decisions:
  - "SsjInsert buffer location — store under a reserved synthetic stream slot '__ssj__' on the EntityState at join_key. Rationale: for the cross-shard relocation the stream-scope doesn't matter (only (join_id, join_key) identifies the buffer); '__ssj__' cannot collide with any real stream name. This differs from the Phase 23 in-place path where the buffer lives under the downstream stream_in_order slot; Wave 3 must reconcile when wiring the operator eval."
  - "Followed the existing touch-with-zero pattern for metric registration in register_shard_metrics (no describe_counter! anywhere in the repo); the plan's describe_counter! directive was aspirational and the existing convention is what makes series appear on /metrics from the first scrape."
  - "Cross-shard counter bump lives ONLY on the target dispatch arm (single emission site). Source-side helpers bump INTRA_SHARD on same-shard fast path only, preventing double-count under any path."
  - "Same-shard fast path invoked when n_shards ≤ 1 OR target_shard_idx == input_shard_idx — preserves N=1 behaviour and covers test harnesses that pass None for sibling_shards."
  - "apply_ssj_insert lives in src/shard/mod.rs (co-located with the other Shard methods) rather than a new file; the buffer logic is ~50 lines and the existing mod.rs already hosts Shard::upsert_source_table_row, tombstone_table_row, etc."
  - "event_time_ms extraction inside apply_ssj_insert falls back to 0 when _event_time is missing/unparseable. Rationale: the evict floor is max_seen - within_ms so a 0-timestamp entry is simply evicted on the next insert with a real timestamp; no unbounded buffer growth."
  - "pipeline.rs helpers use futures::executor::block_on (not tokio block_on) for oneshot recv — matches Phase 54-02 cascade_table_upsert_on_shard's comment about tokio Handle::block_on panicking on re-entry inside the per-shard current_thread runtime."
metrics:
  duration: ~35min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 1
  files_modified: 4
  new_lib_tests: 5
---

# Phase 56 Plan 01: Wave 1 — Cross-Shard Primitives Summary

Three new `ShardOp` variants (`ReadEntityAt`, `ReadEntityBatch`, `SsjInsert`) with dispatch arms, two new `Shard` methods (`read_entity_at`, `apply_ssj_insert`), five metric counters, and three `PipelineEngine` helpers that encapsulate the same-shard fast path + cross-shard `try_send` + blocking-oneshot + `ShardOverload` dispatch contract locked by Phase 54-02. Additive only — Wave 0 RED tests remain `#[ignore]`'d; no operator wiring this wave.

## What Landed

### ShardOp variants (src/shard/thread.rs)

```rust
ReadEntityAt    { table_name: String, key: String }
ReadEntityBatch { table_name: String, keys: Vec<String> }
SsjInsert       { join_id, side: JoinSide, join_key, event: Value, within_ms: u64 }
```

Dispatch arms:
- **ReadEntityAt** — pure read via `shard.read_entity_at`. Bumps `beava_enrich_cross_shard_total{table}`; on `None` also bumps `beava_enrich_missing_total{table}`.
- **ReadEntityBatch** — DoS-guarded (`keys.len() > MAX_ENRICH_BATCH_KEYS=4096 → Err`); iterates via `shard.read_entity_at`; single counter increment of `keys.len()` for cross-shard + per-batch missing count.
- **SsjInsert** — calls `shard.apply_ssj_insert`. Bumps `beava_ssj_cross_shard_total{join_id}`; replies with matched counterparty maps.

### ShardResult variants

```rust
ReadEntityOk(Option<EntityState>)
ReadEntityBatchOk(Vec<Option<EntityState>>)
SsjInsertOk(Vec<serde_json::Map<String, Value>>)
```

### Shard methods (src/shard/mod.rs)

```rust
pub fn read_entity_at(&self, _table_name: &str, key: &str) -> Option<EntityState>;
pub fn apply_ssj_insert(
    &mut self,
    join_id: &str,
    side: JoinSide,
    join_key: &str,
    event: serde_json::Value,
    within_ms: u64,
) -> Vec<serde_json::Map<String, Value>>;
```

`read_entity_at` wraps the existing `read_entity_from_shard` free function and clones the `EntityState` out — caller picks `table_rows[table_name]` from the returned entity. `_table_name` is threaded through for API stability + future per-table indexing (not yet used).

`apply_ssj_insert` stores the buffer under a reserved synthetic stream slot `"__ssj__"` on the EntityState at `join_key`. Probes opposite side → inserts → evicts. T-56-01-02 mitigation: non-object events (malformed source) silently return empty matches without inserting.

### Metrics (src/shard/metrics.rs)

| Constant | Name | Labels | Emitted from |
|---|---|---|---|
| `ENRICH_CROSS_SHARD_TOTAL` | `beava_enrich_cross_shard_total` | `table` | Target dispatch arm (this wave) |
| `ENRICH_INTRA_SHARD_TOTAL` | `beava_enrich_intra_shard_total` | `table` | `read_entity_at_shard` same-shard fast path (this wave) |
| `ENRICH_MISSING_TOTAL`     | `beava_enrich_missing_total`     | `table` | Both target arm + fast path (this wave) |
| `SSJ_CROSS_SHARD_TOTAL`    | `beava_ssj_cross_shard_total`    | `join_id` | Target dispatch arm (this wave) |
| `CROSSSHARD_JOINS_REGISTERED_TOTAL` | `beava_crossshard_joins_registered_total` | `join_id` | `register()` (Wave 3) |

All five counters registered at startup via `register_shard_metrics` with a `"__init__"` placeholder label so series appear on /metrics from the first scrape — real labels land at runtime.

### PipelineEngine helpers (src/engine/pipeline.rs)

```rust
pub fn read_entity_at_shard(
    &self, sibling_shards, target_shard_idx, input_shard: &Shard, input_shard_idx,
    table_name, key,
) -> Result<Option<EntityState>, BeavaError>;

pub fn read_entity_batch_at_shard(
    &self, sibling_shards, target_shard_idx, input_shard: &Shard, input_shard_idx,
    table_name, keys: &[String],
) -> Result<Vec<Option<EntityState>>, BeavaError>;

pub fn ssj_insert_at_shard(
    &self, sibling_shards, target_shard_idx, input_shard: &mut Shard, input_shard_idx,
    join_id, side, join_key, event, within_ms,
) -> Result<Vec<serde_json::Map<String, Value>>, BeavaError>;
```

Each helper has a module-level 3-point deadlock analysis block (source never try_sends to own inbox; try_send non-blocking with ShardOverload on Full; target drains inbox on its own pinned thread and replies via oneshot). `futures::executor::block_on` on the oneshot recv per Phase 54-02's "tokio block_on panics on re-entry" note.

All cross-shard sends record inbox high-watermark via `shard::metrics::record_inbox_depth` before `try_send`, keeping Phase 55 `SHARD_INBOX_HIGH_WATERMARK_TOTAL` counter accurate under new traffic.

## Unit Tests Added (5, all `#[cfg(not(feature = "state-inmem"))]`)

| Test | Asserts |
|---|---|
| `read_entity_at_returns_none_on_missing` | Absent key returns `None` |
| `read_entity_at_returns_some_after_upsert` | After `upsert_source_table_row`, entity readable and `table_rows["Countries"].fields["gdp_usd"] == Int(800_000)` |
| `apply_ssj_insert_first_side_returns_empty_matches` | First Left insert on empty buffer returns `[]` |
| `apply_ssj_insert_second_side_returns_prior_counterparty` | Right insert within window returns the prior Left event |
| `apply_ssj_insert_rejects_non_object_event` | T-56-01-02 — bare string event returns `[]` and is NOT buffered (confirmed via follow-up probe) |

## Verification Log

```
$ cargo build --release
Finished `release` profile [optimized] target(s) in 14.08s  ✓

$ cargo build --release --features state-inmem
Finished `release` profile [optimized] target(s) in 13.20s  ✓

$ cargo test --release --lib
test result: ok. 801 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
  (796 baseline + 5 new apply_ssj_insert / read_entity_at tests)

$ cargo test --release --lib --features state-inmem
test result: ok. 800 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
  (Phase 55 state-inmem baseline preserved; new tests are cfg-gated to default/fjall)

$ cargo test --release --test cross_shard_tt_cascade_ownership
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 unregressed)

$ cargo test --release --test cascade_metrics
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 metrics intact)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 11 passed; 0 failed; 2 ignored  ✓ (56-W2/W3 markers preserved)

$ cargo test --release --test cross_shard_enrich_from_table
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W2 markers intact)

$ cargo test --release --test cross_shard_stream_stream_join
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W3 markers intact)

$ cargo test --release --test register_crossshard_join_warning
test result: ok. 0 passed; 0 failed; 3 ignored  ✓ (56-W3 markers intact)

$ cargo test --release --test crossshard_enrich_perf_smoke
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W4 markers intact)
```

## Grep-Count Evidence

```
$ grep -nE "^    (ReadEntityAt|ReadEntityBatch|SsjInsert) \{" src/shard/thread.rs
    3 hits (enum variant definitions at lines 272, 282, 294)

$ grep -cE "ShardOp::(ReadEntityAt|ReadEntityBatch|SsjInsert)" src/shard/thread.rs
    3 hits (dispatch arms at lines 1043, 1064, 1104)

$ grep -cE "ShardResult::(ReadEntityOk|ReadEntityBatchOk|SsjInsertOk)" src/shard/thread.rs
    4 hits (1 doc comment ref + 3 dispatch arm send() calls; enum variant
    definitions are unqualified `ReadEntityOk(...)` etc., same pattern as
    existing `ShardResult::SetOk` / `ShardResult::EvictedCount`)

$ grep -c "MAX_ENRICH_BATCH_KEYS" src/shard/thread.rs
    4 hits (const def + doc refs + DoS guard usage)

$ grep -c "ENRICH_CROSS_SHARD_TOTAL\|ENRICH_INTRA_SHARD_TOTAL\|ENRICH_MISSING_TOTAL\|SSJ_CROSS_SHARD_TOTAL\|CROSSSHARD_JOINS_REGISTERED_TOTAL" src/shard/metrics.rs
    10 hits (5 const defs + 5 register-time touches)

$ grep -cE "fn (read_entity_at|apply_ssj_insert)" src/shard/mod.rs
    7 hits (2 method impls + 5 unit test fn defs)

$ grep -cE "fn (read_entity_at_shard|read_entity_batch_at_shard|ssj_insert_at_shard)" src/engine/pipeline.rs
    3 hits (exactly one per helper as required)

$ grep -cE "Deadlock analysis|wait-chain" src/engine/pipeline.rs
    5 hits (1 module-level block + 4 cross-ref mentions)

$ git diff --name-only a15e928^ HEAD -- src/engine/operators.rs src/engine/register.rs
    (empty ✓ no operator / register changes this wave)
```

## Deviations from Plan

Four minor corrections/adaptations, all additive:

1. **`EntityState` path correction** — Plan text said `crate::state::snapshot::EntityState`. Actual path is `crate::state::store::EntityState` (snapshot has `SerializableEntityState`, which is the postcard wire-form). Used the correct path throughout.

2. **`FeatureValue::Int` not `FeatureValue::U64`** — Plan used `FeatureValue::U64(800_000)` in the unit test template. The actual enum has only `Float(f64)`, `Int(i64)`, `String(String)`, `Missing` (no unsigned variant). Test updated to `FeatureValue::Int(800_000)`.

3. **No `describe_counter!` in repo** — Plan directed me to add `describe_counter!(...)` calls next to the const defs. Grepping the entire repo turned up zero occurrences of the `metrics::describe_*!` macros. The established pattern in `register_shard_metrics` is to touch each counter with `.increment(0)` using a placeholder label (see `CASCADE_CROSS_SHARD_TOTAL` in the existing code). Followed that pattern with `"__init__"` placeholders for the Phase 56 counters — real labels (`table` / `join_id`) land at runtime from the dispatch arms + helpers (Wave 2/3). Rule 3: deviation forced by reality of the codebase.

4. **`apply_ssj_insert` buffer slot location** — Plan text said the buffer is stored where the existing Phase 23 code puts it (under `stream_in_order` on the downstream stream). For the cross-shard relocated path, the downstream stream scope is set by the source shard, not the target; using `stream_in_order` as the slot key would collision-couple the relocated buffer to the source shard's naming. I chose a reserved synthetic slot `"__ssj__"` so `(join_id, join_key)` uniquely identifies the buffer regardless of which stream drove the insert. Wave 3 MUST ensure the operator eval reads from this slot when the relocated path is active, and also handle the Phase 23 in-place path (possibly by making both paths use `"__ssj__"`). Documented as a Wave 3 handoff item below.

None of the 4 deviations change the wave assignments, the acceptance grep gates (all pass), or the baseline test counts.

## Authentication Gates Encountered

None — Wave 1 is a pure additive code change, no wire surface or external auth.

## Deferred Issues

None — Wave 1's exit criterion was "compiles + lib-tests preserved + new unit tests pass + no operator wiring". All satisfied on first build iteration. No 3-attempt auto-fix limit triggered.

## Next Wave Handoff (Wave 2 — EnrichFromTable wiring)

Wave 2 (plan 56-02) MUST:

1. In `src/engine/operators.rs::EnrichFromTable::eval`, replace the current Missing-on-cross-shard branch with a call path that:
   - Computes `target_shard_idx = hash(right_key) % n_shards`.
   - Calls `PipelineEngine::read_entity_at_shard(sibling_shards, target_shard_idx, &input_shard, input_shard_idx, &right_table, &right_key)`.
   - On `Ok(Some(entity))`: extract `entity.table_rows[&right_table].fields[&right_field]` and populate the enrichment.
   - On `Ok(None)`: keep Missing semantics (D-A4) — downstream handles null.
   - On `Err(...)`: propagate up to push_with_cascade_on_shard's caller (which maps to HTTP 503 / TCP SHARD_OVERLOAD).

2. For the per-batch coalesced variant (D-A2), accumulate per-`(target_shard, table)` enrichment keys into a stack-local `AHashMap<(usize, String), Vec<String>>` across all events in a batch, and end-of-batch call `read_entity_batch_at_shard` once per bucket. Cap each bucket at `MAX_ENRICH_BATCH_KEYS` (split larger buckets into multiple calls).

3. Flip the 2 × `#[ignore = "56-W2"]` tests in `tests/cross_shard_enrich_from_table.rs` to GREEN by removing the markers, replacing `todo!()` bodies with the actual assertions already specified in the Wave 0 assertion-hook map.

4. Extend the `mismatched_shard_enrich_parity_n1_vs_n8` proptest body in `tests/sharding_parity.rs` to replay through both N=1 and N=8 engines and byte-compare enrichment output.

5. Grep-gate: `beava_enrich_intra_shard_total{table}` emits from `read_entity_at_shard` (already wired in Wave 1 — Wave 2 just exercises it).

## Next Wave Handoff (Wave 3 — StreamStreamJoin + register relaxation)

Wave 3 (plan 56-03) MUST:

1. In `src/engine/operators.rs` (or `src/engine/pipeline.rs::push_with_cascade_on_shard`), replace the in-place StreamStreamJoin probe+insert block (pipeline.rs:1805-1840) with a call to `PipelineEngine::ssj_insert_at_shard(..., target_shard_idx = hash(state_key) % n_shards, ...)`. The co-located fast path (D-B5) is already inlined in `ssj_insert_at_shard` — no manual branch needed.

2. **Buffer slot reconciliation (Wave 1 carryover):** the relocated `apply_ssj_insert` writes the buffer under the synthetic `"__ssj__"` stream slot on the join-key-owning shard's EntityState. The pre-existing Phase 23 in-place StreamStreamJoin code writes it under the downstream `stream_in_order`. Wave 3 must pick ONE convention. Recommendation: unify on `"__ssj__"` for both paths (the downstream stream name was an implementation detail; `(join_id, join_key)` is the true identity). This requires updating the in-place block in `pipeline.rs:1811-1838` too.

3. In `src/engine/register.rs`, relax the `JoinShardKeyMismatch` error path:
   - Detect the mismatch case (`left.shard_key != join.on || right.shard_key != join.on`).
   - Replace `return Err(BeavaError::JoinShardKeyMismatch { .. })` with `tracing::warn!(...)` (see D-B4 message text in 56-CONTEXT.md) AND `metrics::counter!(CROSSSHARD_JOINS_REGISTERED_TOTAL, "join_id" => ...).increment(1)`.
   - Keep the `BeavaError::JoinShardKeyMismatch` variant — it's still raised by other paths, just not at register().

4. Extend the `/debug/warnings` endpoint (Phase 51 `src/server/debug_warnings.rs`) to surface a `cross_shard_joins: [{join_id, left_shard_key, right_shard_key, on_field, perf_note}]` array. Perf note: `"+1 inbox hop per event; co-locate by setting shard_key='<join.on>' on both streams"`.

5. Flip the 7 × `#[ignore = "56-W3"]` tests:
   - `tests/cross_shard_stream_stream_join.rs` (2 tests)
   - `tests/register_crossshard_join_warning.rs` (3 tests)
   - `tests/sharding_parity.rs::mismatched_shard_join_parity_n1_vs_n8` (1 proptest)

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 (ShardOp + Shard + metrics) | `a15e928` | `feat(56-W1): add cross-shard primitives — ShardOp variants + Shard methods + metrics` |
| Task 2 (pipeline helpers)         | `9ed4dfb` | `feat(56-W1): add pipeline.rs helpers for cross-shard enrich + SSJ dispatch` |

Range: `a15e928..9ed4dfb` (2 commits on `arch/tpc-full-shard`).

## Known Stubs

None of the Phase 56 primitives are stubs in the "UI rendering empty data" sense. The five metric counters are registered with `"__init__"` placeholder labels at boot; every runtime call site overrides with the real `table` / `join_id`. Wave 2/3 will produce real label emission as they wire the operator paths.

## Threat Flags

None new. The two mitigations applied from the plan's threat model:
- **T-56-01-01 (DoS on ReadEntityBatch):** `MAX_ENRICH_BATCH_KEYS = 4096` constant defined in `src/shard/thread.rs`; target dispatch arm rejects oversized batches with a typed `ShardResult::Err(ProcessingError("enrich batch > 4096 keys..."))`. Unit test coverage deferred to Wave 2 harness (requires a spawned shard thread; Wave 1 tests exercise the `Shard` method directly which has no guard — the guard lives in the ShardOp arm, which is a SPSC integration concern).
- **T-56-01-02 (SSJ buffer poisoning):** `apply_ssj_insert` validates `event` is a JSON object; bare strings/numbers are silently dropped. Unit test `apply_ssj_insert_rejects_non_object_event` covers this.

T-56-01-03 (deadlock via dropped Sender), T-56-01-04 (silent-drop without metric), T-56-01-05 (EoP — accepted, in-process SPSC only) — all satisfied by the pipeline-helper contract (`let _ = tx.send(...)` in all dispatch arms; metric bumps before reply on every arm; no new auth boundary).

## Self-Check: PASSED

- [x] `src/shard/thread.rs` — 3 ShardOp variants + 3 dispatch arms + 3 ShardResult variants + MAX_ENRICH_BATCH_KEYS const — **FOUND**
- [x] `src/shard/mod.rs` — 2 Shard methods + 5 unit tests — **FOUND**
- [x] `src/shard/metrics.rs` — 5 counter consts + 5 register-time touches — **FOUND**
- [x] `src/engine/pipeline.rs` — 3 PipelineEngine helpers + deadlock analysis block — **FOUND**
- [x] `a15e928` commit present in git log — **VERIFIED**
- [x] `9ed4dfb` commit present in git log — **VERIFIED**
- [x] `cargo test --release --lib` → 801 / 0 / 35 — **VERIFIED**
- [x] `cargo test --release --lib --features state-inmem` → 800 / 0 / 35 — **VERIFIED**
- [x] Phase 55 integration tests unregressed (2/2 + 2/2 + 11/2) — **VERIFIED**
- [x] Wave 0 RED tests still `#[ignore]`'d (2+2+3+2 = 9 ignored, 0 passed) — **VERIFIED**
- [x] `git diff a15e928^ HEAD -- src/engine/operators.rs src/engine/register.rs` empty — **VERIFIED**
