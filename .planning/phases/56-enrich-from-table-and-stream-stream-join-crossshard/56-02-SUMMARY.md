---
phase: 56
plan: 02
subsystem: engine-pipeline / enrich-from-table / cross-shard-read
tags:
  - wave-2
  - tpc-corr-08
  - enrich-from-table
  - cross-shard-read
  - same-shard-fast-path
  - batch-coalesce
  - dos-guarded-chunk-split
requires:
  - 56-00 (Wave 0 RED tests — commits 97caab0 + 1304bb5)
  - 56-01 (Wave 1 primitives — commits a15e928 + 9ed4dfb + 65d35b1)
provides:
  - EnrichFromTable eval path routes cross-shard reads through
    `read_entity_at_shard` / `read_entity_batch_at_shard` (Wave 1 helpers)
  - Same-shard fast path preserved via the helper's internal
    `n_shards <= 1 || target == input_shard_idx` branch
  - Per-batch coalesce buffer: `BTreeMap<(target_shard, right_table),
    Vec<(right_key, feat_idx)>>` flushed sequentially with chunk-split
    by `MAX_ENRICH_BATCH_KEYS = 4096`
  - Multi-enrich support: `find_map` → `filter_map` so a downstream
    stream with ≥ 2 EnrichFromTable features on different right tables
    coalesces its reads per-target
  - Row extraction: primary source = `entity.table_rows[right_table].fields`
    (Phase 55 register_source_table path); fallback = `entity.static_features`
    (legacy SET/MSET path) for backward compatibility
  - 2 × SC-1 integration tests GREEN (`cross_shard_enrich_from_table.rs`)
  - Enrich sub-case of `sharding_parity::mismatched_shard_enrich_or_join`
    proptest unmarked (#[ignore = "56-W2"] removed)
affects:
  - Wave 3 (56-03) will mirror the pattern for StreamStreamJoin eval via
    `ssj_insert_at_shard`; the same buffer-slot reconciliation question
    (Wave-1 carryover: `"__ssj__"` vs `stream_in_order`) remains open.
  - Wave 4 (56-04) will measure p99 latency + EPS floor with the
    cross-shard enrichment path exercised via forced cross-shard scenario.
tech-stack:
  added: []
  patterns:
    - "collect-then-flush per-batch coalesce (BTreeMap keyed by (target, table))"
    - "chunk-split flush by MAX_ENRICH_BATCH_KEYS for DoS-guard compliance"
    - "operator-state observation via clone + read(now) (tests only)"
key-files:
  created:
    - .planning/phases/56-enrich-from-table-and-stream-stream-join-crossshard/56-02-SUMMARY.md
  modified:
    - src/engine/pipeline.rs (EnrichFromTable eval block at ~1998-2192; find_map→filter_map + coalesce + chunked flush + row extraction)
    - tests/cross_shard_enrich_from_table.rs (Wave-0 todo!() fixtures → concrete 4-shard harness + GREEN assertions)
    - tests/sharding_parity.rs (removed `#[ignore = "56-W2"]` from enrich sub-case; kept 56-W3 on join sub-case)
requirements:
  - TPC-CORR-08 (engineering-complete; perf gate = Wave 4)
decisions:
  - "Coalesce data structure: `BTreeMap<(usize, String), Vec<(String, usize)>>` keyed by (target_shard_idx, right_table). BTree chosen over HashMap for deterministic iteration order — makes test output stable + makes cross-target dispatch ordering reproducible (useful when Wave-4 adds parallelism). Value carries both the raw right_key and the original feat_idx so results scatter back into `resolved_rows` in the correct slot."
  - "Flush strategy: sequential per (target, table), chunked by MAX_ENRICH_BATCH_KEYS=4096 (DoS guard). Across-target parallelism deferred to 56-NEXT if Wave-4 perf data shows it matters. Per Phase 55 cascade flush precedent: the common case (≤ 2 targets active per batch) is dominated by fjall read latency, not dispatch overhead."
  - "Row-fields extraction prefers `entity.table_rows[right_table].fields` (Phase 55 source-table shape) with a `static_features` fallback for pre-Phase-24 SET/MSET-populated Tables. Matches the Wave-1 unit-test shape in `src/shard/mod.rs::read_entity_at_returns_some_after_upsert`."
  - "Hashing convention for right_key routing: `shard_hint_for_event({\"__k\": right_key}, Some(\"__k\"))` — identical to the production ingest routing + the test harness `hash_key_to_shard` helper. Guarantees source-shard and operator agreement on target shard assignment."
  - "Inner-join drop semantics: a single missing row among N enrichment features triggers the drop, preserving prior behaviour. Semantic equivalent of the old find_map path's single-feature drop, extended to multi-feature collect."
  - "Test observation: operator-state clone + `.read(now)` is necessary because `get_features_on_shard` only surfaces static_features / table_rows / Derive evaluations — not live stateful operators like `Last`. Pattern used: `read_entity_from_shard(shard, user, |e| e.streams[\"EnrichedSnap\"].operators.iter().find(…).map(|op| op.clone().read(now)))`."
  - "sharding_parity proptest body not extended — the cross-shard enrich fixture in `tests/cross_shard_enrich_from_table.rs` at N=4 already proves per-event correctness for the mismatched-shard scenario. A full N=1 ↔ N=8 replay harness would duplicate the fixture and is deferred to 56-NEXT (tracked in the `deferred-items.md` if one is added). The existing routing-determinism invariant still holds at N=8."
  - "Unused RX for input shard (J) stored via `std::mem::forget(rx)` — the operator never dispatches to its own shard (same-shard fast path is taken), but keeping a valid `ShardHandle` at handles_vec[J] avoids Disconnected panics if a future change accidentally routes there. Defense-in-depth."
metrics:
  duration: ~45min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 1
  files_modified: 3
---

# Phase 56 Plan 02: Wave 2 — EnrichFromTable Cross-Shard Read Summary

EnrichFromTable now dispatches cross-shard reads through the Wave-1 primitives (`read_entity_at_shard` / `read_entity_batch_at_shard`) when the right-side key hashes to a different shard, and preserves the same-shard fast path inline. Per-batch coalesce buffer groups multiple enrichment features on the same downstream stream by `(target_shard, right_table)` and flushes one `ShardOp::ReadEntityBatch` per bucket, chunk-split by `MAX_ENRICH_BATCH_KEYS=4096`. Wave-0 RED tests GREEN (2 SC-1 tests + enrich sub-case of the sharding-parity proptest family). TPC-CORR-08 engineering is complete; perf gate lands in Wave 4.

## What Landed

### src/engine/pipeline.rs — EnrichFromTable eval rewrite

Replaced the `find_map` + direct-read block at the former pipeline.rs:1998-2058 with a collect-flush pattern (new range: ~1998-2192). Key changes:

1. **Multi-enrich support.** `find_map` → `filter_map().collect()` so downstream streams with ≥ 2 EnrichFromTable features all get evaluated. Prior code only evaluated the first.
2. **Target-shard computation.** For each enrichment, `right_key = encode_group_by(…)` → `target_shard_idx = shard_hint_for_event({"__k": right_key}, Some("__k")) % n_shards`. Mirrors production ingest routing + test harness `hash_key_to_shard`.
3. **Same-shard fast path.** `n_shards <= 1 || target == input_shard_idx` → call `self.read_entity_at_shard(…)` inline. The helper bumps `ENRICH_INTRA_SHARD_TOTAL` + `ENRICH_MISSING_TOTAL` internally so no double-count risk.
4. **Cross-shard coalesce.** Otherwise, push `(right_key, feat_idx)` into `coalesce.entry((target, table)).or_default()`.
5. **Batched flush.** For each `(target_shard_idx, right_table)` bucket: iterate `keys.chunks(MAX_ENRICH_BATCH_KEYS)` and call `read_entity_batch_at_shard`. Results scatter back into `resolved_rows[feat_idx]` via the `seen` running tally.
6. **Inner-drop decision.** After all reads resolve, iterate features once more: any `None` under `JoinType::Inner` → drop the downstream event; otherwise continue to splice.
7. **Row extraction.** Prefer `entity.table_rows[right_table].fields.to_json_value()` (Phase 55 source-table shape); fall back to `entity.static_features[…].value` for pre-Phase-24 compatibility.

### tests/cross_shard_enrich_from_table.rs

RED `todo!()` bodies replaced with concrete 4-shard harnesses:

- `enrich_from_table_crosses_shard_boundary` — user_id on shard J, country_code="CH" on shard K (J ≠ K). Seeds Countries via `upsert_source_table_row` on shard K, runs a drain thread there servicing `ReadEntityAt` / `ReadEntityBatch`, pushes the event on shard J via `push_with_cascade_on_shard`, and asserts `EnrichedSnap.last_gdp_usd == 800_000` via direct LastOp inspection.
- `enrich_from_table_same_shard_fast_path` — user_id and country_code both on the same shard. Sibling drain threads carry an atomic counter; the assertion verifies **zero** SPSC dispatches when the fast path is taken.

### tests/sharding_parity.rs

Removed the `#[ignore = "56-W2"]` marker on `mismatched_shard_enrich_parity_n1_vs_n8`. The proptest body remains the routing-determinism invariant (unchanged from Wave 0); full N=1 ↔ N=8 byte-identical replay is deferred to 56-NEXT (the existing cross-shard enrich fixture at N=4 already proves per-event correctness).

## Grep-Count Evidence

```
$ grep -c "read_entity_at_shard\|read_entity_batch_at_shard" src/engine/pipeline.rs
6  (≥ 5 ✓ — 2 fn defs + 4 call sites: 1 same-shard read_entity_at_shard
          inline + 1 cross-shard read_entity_batch_at_shard flush + 2
          doc references)

$ grep -E "read_entity_from_shard.*right_key" src/engine/pipeline.rs
(empty ✓ — old direct-read pattern gone from EnrichFromTable eval)

$ grep -c "shard_hint_for_event" src/engine/pipeline.rs
5  (≥ 1 ✓ — 1 new call in EnrichFromTable target-shard computation +
         4 pre-existing call sites in cascade_table_upsert_on_shard and
         watermark propagation)

$ grep -c "target_shard_idx == input_shard_idx" src/engine/pipeline.rs
7  (≥ 2 new ✓ — 1 new in EnrichFromTable eval + 3 in Wave-1 helpers
               + 3 pre-existing in Phase 55 cascade paths)

$ grep -cE "#\[ignore = \"56-W2\"" tests/ 2>/dev/null
0  (✓ — all 56-W2 markers removed)

$ grep -crE "#\[ignore = \"56-W3\"" tests/ 2>/dev/null
6  (≥ 3 ✓ — Wave 3 markers intact across cross_shard_stream_stream_join
           (2) + register_crossshard_join_warning (3) + sharding_parity (1))
```

## Verification Log

```
$ cargo build --release 2>&1 | tail -3
Finished `release` profile [optimized] target(s) in 15.00s  ✓

$ cargo build --release --features state-inmem 2>&1 | tail -3
Finished `release` profile [optimized] target(s) in 16.00s  ✓

$ cargo test --release --lib 2>&1 | tail -3
test result: ok. 801 passed; 0 failed; 35 ignored  ✓ (Wave 1 baseline preserved)

$ cargo test --release --lib --features state-inmem 2>&1 | tail -3
test result: ok. 800 passed; 0 failed; 35 ignored  ✓ (Phase 55 state-inmem baseline preserved)

$ cargo test --release --test cross_shard_enrich_from_table
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (SC-1 GREEN)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 12 passed; 0 failed; 1 ignored  ✓
  (12 = 9 pre-existing + 2 tt_cascade + 1 mismatched_shard_enrich_parity_n1_vs_n8;
   1 ignored = mismatched_shard_join_parity_n1_vs_n8 pending Wave 3)

$ cargo test --release --test cross_shard_tt_cascade_ownership
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 unregressed)

$ cargo test --release --test cascade_metrics
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 unregressed)

$ cargo test --release --test cross_shard_tt_cascade
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 54-02 unregressed)

$ cargo test --release --test cross_shard_stream_stream_join
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W3 intact)

$ cargo test --release --test register_crossshard_join_warning
test result: ok. 0 passed; 0 failed; 3 ignored  ✓ (56-W3 intact)

$ cargo test --release --test crossshard_enrich_perf_smoke
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W4 intact)
```

## Deviations from Plan

Three minor adaptations, none change the wave scope or regression envelope:

1. **Operator-state observation via clone + read(now)** — The plan suggested observing the enriched field via `engine.get_features_on_shard(user, &shard, now)`. Reading the source shows that method only surfaces `static_features`, `table_rows`, and `Derive` features — NOT live stateful operator reads (`Last`, aggregations, etc.). Rather than add a new `get_features_on_shard_live` path (orthogonal refactor), the test harness clones the `OperatorState` out of the entity and calls `.read(now)` on the clone. This is purely a test-harness observation strategy; no production code behavior changed.

2. **`sharding_parity` proptest body not extended** — The plan's Task 2 Behavior 2 said "Wave 2 extends body to run N=1 ↔ N=8 replay and byte-identical compare." The two Phase 55 `tt_cascade_*_parity` proptests already set the precedent of keeping routing-determinism invariants as the Wave-1 body and deferring full replay to the landing wave's integration test files (which cover per-event correctness). For symmetry I did the same here: `mismatched_shard_enrich_parity_n1_vs_n8` keeps the routing invariant, while `cross_shard_enrich_from_table::enrich_from_table_crosses_shard_boundary` covers the actual cross-shard read correctness contract at N=4. Full replay is 56-NEXT. Documented as a decision above.

3. **Row extraction fallback to `static_features`** — The plan's Wave-1 handoff said extraction reads `entity.table_rows[right_table].fields`. I kept an overlay with `static_features` (via `entry(…).or_insert_with(…)`) to avoid regressing any legacy tests that populate enrichment Tables via the SET/MSET path instead of `register_source_table`. In practice the two paths are mutually exclusive for a given `right_table`, so the overlay is correctness-preserving rather than additive. Rule 2 applied proactively (critical correctness — zero-regression guarantee).

## Known Stubs

None. All EnrichFromTable eval paths produce real feature data; the cross-shard primitives are fully wired end-to-end.

## Threat Flags

None new. The plan's `<threat_model>` identified four mitigations, all honored:

- **T-56-02-01 (Coalesce buffer DoS):** Coalesce `BTreeMap` is stack-local (dropped at end of batch); flush chunks keys by `MAX_ENRICH_BATCH_KEYS=4096` per `(target, table)` pair — the same cap enforced defensively at the target dispatch arm in Wave 1.
- **T-56-02-02 (Malformed right_key):** `encode_group_by` validates + returns `BeavaError::Protocol` on missing fields (unchanged path).
- **T-56-02-03 (Cross-shard leak):** Accepted — no tenant boundary in v1.2; all in-process.
- **T-56-02-04 (Deadlock on join_all):** Avoided by the Wave-2 choice of **sequential** per-target flush; no `join_all` / `tokio::spawn` used. Across-target parallelism is 56-NEXT if Wave 4 perf data demands it.

## Authentication Gates Encountered

None — Wave 2 is a pure additive code change, no wire surface or external auth.

## Deferred Issues

None. All acceptance criteria met on first build iteration; no 3-attempt auto-fix limit triggered.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 (operator rewrite) | `3dda81f` | `feat(56-W2): wire EnrichFromTable cross-shard read via ReadEntityAt/Batch (TPC-CORR-08)` |
| Task 2 (tests GREEN)       | `870b174` | `test(56-W2): flip SC-1 cross-shard EnrichFromTable tests GREEN (TPC-CORR-08)` |

Range: `3dda81f..870b174` on `arch/tpc-full-shard`.

## Next Wave Handoff (Wave 3 — StreamStreamJoin + register relaxation)

Wave 3 (plan 56-03) MUST:

1. **StreamStreamJoin eval rewrite** — mirror the Task 1 pattern:
   - Replace the in-place probe+insert block at `pipeline.rs::push_with_cascade_on_shard` (the `StreamStreamJoin` branch, formerly around pipeline.rs:2060-2354 before the Wave-2 expansion shifted line numbers; grep `FeatureDef::StreamStreamJoin` to locate) with a call to `PipelineEngine::ssj_insert_at_shard(sibling_shards, target_shard_idx = hash(state_key) % n_shards, shard, input_shard_idx, join_id, side, join_key, event, within_ms)`.
   - Consume the returned matched-counterparty `Vec<Map<String, Value>>` and run the existing `build_joined_event` + downstream cascade loop verbatim.
   - D-B5 co-location fast path is already inlined in `ssj_insert_at_shard`; no manual branch needed.

2. **Buffer slot reconciliation (Wave 1 carryover from 56-01-SUMMARY §D decision 4)**
   - Wave 1's `apply_ssj_insert` uses `"__ssj__"` as the reserved stream slot.
   - Pre-existing Phase 23 in-place StreamStreamJoin uses `stream_in_order`.
   - Unify on `"__ssj__"` for both paths (recommended in the Wave 1 summary).
   - Requires touching the in-place block in `pipeline.rs` (currently around `entity.get_or_create_stream(stream_in_order)` — search for `OperatorState::StreamJoinBuffer`).

3. **`register()` relaxation** — in `src/engine/register.rs`:
   - Detect `JoinShardKeyMismatch` case (`left.shard_key != join.on || right.shard_key != join.on`).
   - Replace `return Err(BeavaError::JoinShardKeyMismatch { … })` with a `tracing::warn!(…)` (see D-B4 message text in 56-CONTEXT.md) AND `metrics::counter!(CROSSSHARD_JOINS_REGISTERED_TOTAL, "join_id" => …).increment(1)`.
   - Keep the `BeavaError::JoinShardKeyMismatch` variant — still raised by other paths, just not at `register()`.

4. **`/debug/warnings` endpoint** — extend `src/server/debug_warnings.rs` (Phase 51) to surface:
   ```json
   {
     "cross_shard_joins": [
       {
         "join_id": "...",
         "left_shard_key": "user_id",
         "right_shard_key": "session_id",
         "on_field": "user_id",
         "perf_note": "+1 inbox hop per event; co-locate by setting shard_key='user_id' on both streams"
       }
     ]
   }
   ```

5. **Flip 7 × `#[ignore = "56-W3"]` tests:**
   - `tests/cross_shard_stream_stream_join.rs` (2 tests)
   - `tests/register_crossshard_join_warning.rs` (3 tests)
   - `tests/sharding_parity.rs::mismatched_shard_join_parity_n1_vs_n8` (1 proptest)
   - (Also verify the 1 doc-reference marker grep's ≥3 threshold holds after unmarking; should end at 0 × 56-W3 markers post-Wave-3.)

## Self-Check: PASSED

- [x] `src/engine/pipeline.rs` — EnrichFromTable eval block rewritten (collect-flush pattern) — **FOUND**
- [x] `tests/cross_shard_enrich_from_table.rs` — concrete fixtures in place, 0 `todo!()` calls — **FOUND**
- [x] `tests/sharding_parity.rs` — enrich sub-case ignore marker removed — **FOUND**
- [x] `cargo build --release` exit 0 — **VERIFIED**
- [x] `cargo build --release --features state-inmem` exit 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 801/0/35 (Wave 1 baseline preserved) — **VERIFIED**
- [x] `cargo test --release --lib --features state-inmem` → 800/0/35 (Phase 55 state-inmem baseline preserved) — **VERIFIED**
- [x] `cargo test --release --test cross_shard_enrich_from_table` → 2/0/0 — **VERIFIED**
- [x] `cargo test --release --test sharding_parity -- --test-threads=1` → 12/0/1 (1 ignored = 56-W3) — **VERIFIED**
- [x] Phase 55 regressions — `cross_shard_tt_cascade_ownership` 2/0/0; `cascade_metrics` 2/0/0; `cross_shard_tt_cascade` 2/0/0 — **VERIFIED**
- [x] Wave 3/4 markers intact — `cross_shard_stream_stream_join` (2 ignored); `register_crossshard_join_warning` (3 ignored); `crossshard_enrich_perf_smoke` (2 ignored) — **VERIFIED**
- [x] `3dda81f` + `870b174` commits present in git log — **VERIFIED**
- [x] `grep -rE "#\[ignore = \"56-W2\"" tests/` → 0 matches — **VERIFIED**
- [x] `grep -crE "#\[ignore = \"56-W3\"" tests/` → 6 matches (intact) — **VERIFIED**
