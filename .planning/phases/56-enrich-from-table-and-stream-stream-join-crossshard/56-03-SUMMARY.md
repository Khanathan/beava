---
phase: 56
plan: 03
subsystem: engine-pipeline / stream-stream-join / register-validation / debug-warnings
tags:
  - wave-3
  - tpc-corr-04-relaxation
  - tpc-corr-09
  - stream-stream-join
  - cross-shard-ssj-insert
  - cross-shard-join-warning
  - debug-warnings
  - phase-56
requires:
  - 56-00 (Wave 0 RED tests — 97caab0 + 1304bb5)
  - 56-01 (Wave 1 primitives — a15e928 + 9ed4dfb + 65d35b1)
  - 56-02 (Wave 2 EnrichFromTable wiring — 3dda81f + 870b174 + cba6023)
provides:
  - CrossShardJoinWarning struct in src/engine/join_validator.rs
  - validate_shard_keys -> Vec<CrossShardJoinWarning> (never returns Err)
  - register() loop: log (eprintln!) + counter bump (CROSSSHARD_JOINS_REGISTERED_TOTAL) + signal registry push
  - emit_cross_shard_join_warning helper in src/server/signals.rs
  - SignalRegistry.cross_shard_joins Vec + dedupe push + snapshot accessor
  - /debug/warnings top-level cross_shard_joins array (sibling to warnings)
  - StreamStreamJoin eval at ~pipeline.rs:2296-2345 routes via ssj_insert_at_shard
  - Buffer slot unified on "__ssj__" (W1 deviation 4 reconciled)
  - 2 × SC-2 tests GREEN (cross_shard_stream_stream_join.rs)
  - 2 × SC-3 tests GREEN + 1 dedupe test + 1 co-located quiet test (register_crossshard_join_warning.rs)
  - sharding_parity SSJ sub-case un-ignored (13 passed, was 12/1 ignored)
affects:
  - Wave 4 (56-04) perf gate: measures p99 latency + EPS floor with both EnrichFromTable
    cross-shard (Wave 2) and StreamStreamJoin cross-shard (Wave 3) paths exercised.
tech-stack:
  added: []
  patterns:
    - "validate_shard_keys returns Vec<CrossShardJoinWarning> (empty = OK, populated = relaxed-mismatch)"
    - "register() loop: per-warning eprintln! + metrics::counter!.increment(1) + emit_cross_shard_join_warning"
    - "SignalRegistry dedupe by join_id (T-56-03-01)"
    - "HTTP response shape additive: `cross_shard_joins` at response root (NOT nested under `warnings`)"
    - "SSJ eval: target_shard_idx = shard_hint_for_event({\"__k\": state_key}, Some(\"__k\")) % N"
    - "Same-shard fast path via ssj_insert_at_shard's internal `n_shards <= 1 || target == input_shard_idx` branch"
key-files:
  created:
    - .planning/phases/56-enrich-from-table-and-stream-stream-join-crossshard/56-03-SUMMARY.md
  modified:
    - src/engine/join_validator.rs (+CrossShardJoinWarning struct, validate_shard_keys rewrite, JoinShardKeyMismatch #[deprecated])
    - src/engine/pipeline.rs (register() relaxation loop + StreamStreamJoin eval rewrite via ssj_insert_at_shard)
    - src/server/signals.rs (SignalRegistry.cross_shard_joins field + push_cross_shard_join + cross_shard_joins_snapshot + emit_cross_shard_join_warning)
    - src/server/http.rs (/debug/warnings handler emits cross_shard_joins sibling field)
    - tests/register_crossshard_join_warning.rs (RED → GREEN, 4 tests total)
    - tests/cross_shard_stream_stream_join.rs (RED → GREEN, 2 tests)
    - tests/sharding_parity.rs (56-W3 marker removed from SSJ sub-case)
requirements:
  - TPC-CORR-04 (relaxed — register() no longer rejects; runtime correctness via Wave 1's ssj_insert_at_shard)
  - TPC-CORR-09 (engineering-complete; perf gate = Wave 4)
decisions:
  - "Tracing vs eprintln! — the repo does NOT pull in the `tracing` crate (Cargo.toml has
    `metrics`, `metrics-exporter-prometheus`, but no tracing/log/env_logger). The plan text
    prescribed `tracing::warn!`. Deviation: use `eprintln!` with structured 'key=value' body
    and the '[WARN] beava::register' prefix — matches the existing convention
    (src/shard/fjall_backend.rs:137 uses `eprintln!(\"[WARN] ...\")`). The test harness
    asserts the warning via the structured data surfaces (signal registry, validate_shard_keys
    return value, /debug/warnings JSON) rather than parsing stderr. This preserves the D-B4
    audit-trail intent without adding a dep."
  - "HTTP response shape — the plan prescribed `warnings: { join_shard_key_mismatch, shard_key_missing, cross_shard_joins }` as an object. This would break all Phase 51 warning tests (test_debug_warnings_endpoint.rs + test_warnings_feed.rs both assert `body[\"warnings\"].as_array()`). Deviation: surface `cross_shard_joins` as a sibling field at the response root, keeping `warnings` as the flat Phase 51 array. Each cross-shard-join warning also lands in the unified `warnings` feed as a Category::Safety / Severity::Warning signal via emit_cross_shard_join_warning. Net effect: plan intent met (structured cross_shard_joins array visible at /debug/warnings); Phase 51 back-compat preserved."
  - "Buffer slot unification — Wave 1 left two buffer placements: (a) new `apply_ssj_insert` writes to synthetic `\"__ssj__\"` slot with `join_id=feat_name`; (b) pre-Phase-56 in-place SSJ eval wrote to `stream_in_order` slot with `feat_name` operator. Wave 3 unifies on `\"__ssj__\"` by routing ALL SSJ inserts through `ssj_insert_at_shard`. The helper's fast-path (target==input) also writes to `\"__ssj__\"` via `apply_ssj_insert`, so both paths now use the same layout. No shim needed — no consumer reads the SSJ buffer by stream name."
  - "event_time_ms carry-through — plan raised a possible Wave-1 signature gap: 'if Wave 1's apply_ssj_insert takes within_ms only and derives event_time internally, adjust.' Checked Wave 1 code: apply_ssj_insert DOES derive event_time_ms internally from the event map via `parse_event_time`. No signature change needed. The pipeline.rs eval retains its own `event_time_ms` computation only because the same variable feeds the `stream_state.last_event_at = Some(now)` touch in the old code — which is now inside apply_ssj_insert's closure. We left the derivation in place with a `_`-prefixed binding to avoid churn; could prune in 56-NEXT."
  - "register() signals dispatch scoped to `#[cfg(feature = \"server\")]` — mirrors the Phase 51 pattern (same file, line 950). Non-server builds still get the warning via eprintln! + counter, just no signal registry (there is no signal registry on non-server builds anyway)."
  - "Back-compat JoinShardKeyMismatch retained — `#[deprecated(since=\"56.0\")]` annotation on the struct + Display + Error impls; `#[allow(deprecated)]` on the remaining internal consumers (emit_join_shard_key_mismatch signals helper + build_mismatch + test module). External callers matching on `BeavaError::Protocol(msg)` where msg contained the D-12 locked 'requires matching shard_key' substring still work — `CrossShardJoinWarning.message` preserves that substring verbatim."
metrics:
  duration: ~50min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 1
  files_modified: 7
---

# Phase 56 Plan 03: Wave 3 — StreamStreamJoin cross-shard + TPC-CORR-04 relaxation

Two linked deliveries close the correctness leg of Phase 56:

1. **TPC-CORR-04 relaxed** — `register()` no longer errors on mismatched
   shard_key joins. `validate_shard_keys` returns
   `Vec<CrossShardJoinWarning>`; the registration path logs + increments
   `beava_crossshard_joins_registered_total{join_id}` + records into the
   signal registry. The `/debug/warnings` endpoint gains a top-level
   `cross_shard_joins` array.

2. **TPC-CORR-09 engineered** — `StreamStreamJoin` eval in
   `push_with_cascade_on_shard` rewires through Wave 1's
   `ssj_insert_at_shard` helper. Both L and R events converge on
   `hash(join.on) % N` via `shard_hint_for_event({"__k": state_key},
   Some("__k")) % N`. Co-located case (D-B5) short-circuits to
   `apply_ssj_insert` inline — zero SPSC hops.

## What Landed

### src/engine/join_validator.rs

- New `pub struct CrossShardJoinWarning { join_id, stream_a, stream_b,
  left_shard_key, right_shard_key, on_field, perf_note, message }` with
  stable `join_id` synthesis `"{stream_a}_x_{stream_b}_on_{on_field}"`.
- `validate_shard_keys(streams, new_stream) -> Vec<CrossShardJoinWarning>`
  (was `Result<(), JoinShardKeyMismatch>`). Empty Vec = no mismatches;
  populated Vec = one entry per mismatched peer pair (deduped internally
  by `join_id`).
- `JoinShardKeyMismatch` + `build_mismatch` marked `#[deprecated(since =
  "56.0")]` but retained for back-compat (D-C2 additive-not-destructive).
  Tests in the `#[cfg(test)]` module updated to the new signature.

### src/engine/pipeline.rs

- `register()` (~pipeline.rs:948-985): replaced the `if let Err(mismatch)
  ... return Err(BeavaError::Protocol(mismatch.message.clone()))` block
  with a loop over the returned warnings. Per warning:
  - `eprintln!("[WARN] beava::register CrossShardJoinWarning: ...")`
    (`tracing` crate is not in this codebase — deviation, see decisions).
  - `metrics::counter!(CROSSSHARD_JOINS_REGISTERED_TOTAL, "join_id" =>
    ...).increment(1)`.
  - `#[cfg(feature = "server")] emit_cross_shard_join_warning(registry, w)`.
  Registration proceeds regardless — no early return.
- `StreamStreamJoin` eval block (~pipeline.rs:2296-2345): replaced the
  `StoreView::Sharded(shard).with_entity_mut(&state_key, |entity| {
  get_or_create + probe + insert + evict })` RMW block with:

  ```rust
  let target_shard_idx = if n_shards <= 1 {
      input_shard_idx
  } else {
      (shard_hint_for_event({"__k": state_key}, Some("__k")) as usize) % n_shards
  };
  let matches = self.ssj_insert_at_shard(
      sibling_shards, target_shard_idx, shard, input_shard_idx,
      &feat_name, side, &state_key, Value::Object(arriving_map.clone()),
      within_ms,
  )?;
  ```
  The rest of the eval block (matches → joined_events → cascade emission)
  is unchanged.

### src/server/signals.rs

- `SignalRegistry.cross_shard_joins: Vec<CrossShardJoinWarning>` —
  dedupe-by-join_id push bucket.
- `push_cross_shard_join(warning)` — inserts if `join_id` absent.
- `cross_shard_joins_snapshot()` — read-only clone for the HTTP handler.
- `emit_cross_shard_join_warning(registry, warning)` — dual-wire surface:
  (a) records a `Category::Safety` / `Severity::Warning` signal with id
  `crossshard_join.{join_id}` (flows into unified `warnings` feed);
  (b) pushes onto the dedicated `cross_shard_joins` Vec.
- `emit_join_shard_key_mismatch` marked `#[allow(deprecated)]` so its
  signature continues to compile with the deprecated
  `JoinShardKeyMismatch` parameter.

### src/server/http.rs

- `debug_warnings` handler (~1507-1535) extended: reads
  `state.signals.read().cross_shard_joins_snapshot()` and attaches it as
  a sibling field `cross_shard_joins` on the response JSON. Phase 51's
  flat `warnings` array contract is preserved.

### tests/cross_shard_stream_stream_join.rs

Wave-0 `todo!()` bodies replaced with concrete N=4 harnesses:

- `stream_stream_join_routes_to_join_key_shard` (SC-2 primary):
  two-pass fixture. Pass A pushes L on shard J (inline fast path —
  target==input); asserts SSJ buffer exists on J, zero sibling hops.
  Pass B pushes R on shard S (hash(session_id) % 4 != J); asserts
  exactly 1 SsjInsert dispatch to J, zero to other shards, and the SSJ
  buffer is absent on S.
- `stream_stream_join_colocated_fast_path` (SC-2 corollary):
  both L and R registered with shard_key=user_id (D-B5). Asserts
  zero SsjInsert hops across all shards and buffer present on J.

### tests/register_crossshard_join_warning.rs

Four tests GREEN (plan asked for 3; added dedupe smoke):

- `register_emits_crossshard_warning_not_error` (SC-3 primary) —
  drives register() end-to-end, asserts Ok + warning contents via
  `validate_shard_keys` direct call (matches plan's assertion hooks:
  "user_id" + "session_id" + "CrossShardJoinWarning" + "+1 inbox hop").
- `register_colocated_join_emits_no_warning` — D-B5 quiet-path.
- `debug_warnings_endpoint_lists_cross_shard_joins` (SC-3 HTTP
  surface) — spawns full axum router, asserts body.cross_shard_joins
  has 1 entry with (left_shard_key=user_id, right_shard_key=session_id,
  on_field=user_id, perf_note contains "+1 inbox hop"). Also asserts
  the matching signal appears in the unified `warnings` feed.
- `signal_registry_dedupes_cross_shard_joins_by_join_id` — T-56-03-01
  mitigation test.

### tests/sharding_parity.rs

- `#[ignore = "56-W3"]` removed from
  `mismatched_shard_join_parity_n1_vs_n8`. Body unchanged (routing
  invariant at N=8). Full N=1↔N=8 replay remains 56-NEXT (same as
  Wave 2 did for the enrich sub-case).

## Verification Log

```
$ cargo build --release
Finished `release` profile [optimized] target(s) in 15.32s  ✓

$ cargo build --release --features state-inmem
Finished `release` profile [optimized] target(s) in 13.65s  ✓

$ cargo test --release --lib
test result: ok. 801 passed; 0 failed; 35 ignored  ✓ (Wave 1/2 baseline preserved)

$ cargo test --release --test cross_shard_stream_stream_join
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (SC-2 GREEN)

$ cargo test --release --test register_crossshard_join_warning
test result: ok. 4 passed; 0 failed; 0 ignored  ✓ (SC-3 + dedupe GREEN)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 13 passed; 0 failed; 0 ignored  ✓ (was 12/1, SSJ sub-case un-ignored)

$ cargo test --release --test cross_shard_enrich_from_table
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Wave 2 unregressed)

$ cargo test --release --test cross_shard_tt_cascade_ownership
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 unregressed)

$ cargo test --release --test cascade_metrics
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 metrics intact)

$ cargo test --release --test cross_shard_tt_cascade
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 54-02 unregressed)

$ cargo test --release --test test_debug_warnings_endpoint
test result: ok. 10 passed; 0 failed; 0 ignored  ✓ (Phase 51 unregressed)

$ cargo test --release --test test_warnings_feed
test result: ok. 10 passed; 0 failed; 0 ignored  ✓ (Phase 51 unregressed)

$ cargo test --release --test test_warnings_dedupe
test result: ok. 6 passed; 0 failed; 0 ignored  ✓ (Phase 51 unregressed)

$ cargo test --release --test test_warnings_integration
test result: ok. 4 passed; 0 failed; 0 ignored  ✓ (Phase 51 unregressed)

$ cargo test --release --test crossshard_enrich_perf_smoke
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W4 markers intact)
```

## Grep-Count Evidence

```
$ grep -rE "#\[ignore = \"56-W3\"" tests/ | wc -l
0   ✓ (all 56-W3 markers removed)

$ grep -rE "#\[ignore = \"56-W4\"" tests/ | wc -l
2   ✓ (perf gate tests remain, Wave 4 work)

$ grep -c "ssj_insert_at_shard" src/engine/pipeline.rs
4   ✓ (1 def + 1 eval call + 2 doc refs; ≥2 required)

$ grep -c "return Err(BeavaError::Protocol(mismatch.message" src/engine/pipeline.rs
0   ✓ (relaxation complete)

$ grep -c "pub struct CrossShardJoinWarning" src/engine/join_validator.rs
1   ✓ (exactly one struct)

$ grep -c "fn emit_cross_shard_join_warning" src/server/signals.rs
1   ✓ (exactly one emitter)

$ grep -c "cross_shard_joins" src/server/http.rs
3   ✓ (≥1 required; 3 = comment + json field + snapshot call)

$ grep -c "#\[deprecated" src/engine/join_validator.rs
3   ✓ (JoinShardKeyMismatch + 2 others touched — all back-compat retained)

$ grep -n "with_entity_mut.*state_key" src/engine/pipeline.rs
(empty)   ✓ (old in-place RMW pattern gone from SSJ eval)
```

## Deviations from Plan

Two material adaptations, documented inline in the `decisions` frontmatter
and honored by the test harness:

1. **`tracing::warn!` → `eprintln!`** — the repo does not pull in the
   `tracing` crate. The plan text prescribed `tracing::warn!(target: ...,
   join_id = %w.join_id, ...)`. Deviation: `eprintln!("[WARN]
   beava::register CrossShardJoinWarning: join_id={} ...")` matches the
   existing convention (`src/shard/fjall_backend.rs:137`). Test
   assertions verify the structured data via the signal registry +
   HTTP endpoint rather than stderr parsing — D-B4's audit-trail intent
   (who/what/when got relaxed) is preserved.

2. **`/debug/warnings` shape: object-of-categories → flat array +
   sibling field** — the plan prescribed nesting `cross_shard_joins`
   under an object-typed `warnings` field (`warnings.cross_shard_joins`).
   All Phase 51 tests (`test_debug_warnings_endpoint.rs:88/89/102`,
   `test_warnings_feed.rs:87/312`) assert `body["warnings"].as_array()`.
   Switching to an object breaks Phase 51. Deviation: add
   `cross_shard_joins` as a sibling top-level field at the response root
   (`body["cross_shard_joins"]`). Each warning also lands in the
   unified flat `warnings` feed as a `Category::Safety` signal via
   `emit_cross_shard_join_warning`. Plan intent met, Phase 51 contract
   preserved. The SC-3 test was adjusted accordingly
   (`body["cross_shard_joins"]` direct access).

Neither deviation changes wave assignments, counter names, or the
register() behaviour (still no Err on mismatch; still emits warning via
three surfaces).

## Known Stubs

None. Every code path added this wave produces real data:

- `CrossShardJoinWarning` fields all populated at construction.
- `validate_shard_keys` returns a real Vec driven by the `FeatureDef`
  enum arms.
- `/debug/warnings.cross_shard_joins` populated from a real Vec on the
  SignalRegistry.
- StreamStreamJoin eval produces real match lists from `ssj_insert_at_shard`.

## Threat Flags

None new. The threat model's five mitigations are all in place:

- **T-56-03-01 (DoS — signals registry unbounded growth):** `SignalRegistry.cross_shard_joins`
  dedupe by `join_id`. Covered by
  `signal_registry_dedupes_cross_shard_joins_by_join_id` test.
- **T-56-03-02 (Tampering — operator treats warning as correctness):** `perf_note` text
  makes the correctness-vs-perf distinction explicit ("+1 inbox hop per event").
  Runtime correctness delivered by Wave 1's `ssj_insert_at_shard`.
- **T-56-03-03 (Repudiation — lost audit trail):** `eprintln!` per relaxation event
  with full context; `CROSSSHARD_JOINS_REGISTERED_TOTAL{join_id}` is a persistent
  monotonic counter; signal registry entry.
- **T-56-03-04 (DoS — SSJ buffer linger):** Accepted. `within_ms` eviction unchanged
  from Phase 51. Buffer-level TTL eviction remains Phase 57 work.
- **T-56-03-05 (EoP):** Accepted. No auth boundary added.

## Authentication Gates Encountered

None — Wave 3 is a pure additive code change, no wire surface or external auth.

## Deferred Issues

None. All acceptance criteria met on first build iteration; no 3-attempt
auto-fix limit triggered.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 (relaxation)  | `39b9536` | `feat(56-W3a): relax TPC-CORR-04 → CrossShardJoinWarning at register time` |
| Task 2 (SSJ eval)    | `ea251b0` | `feat(56-W3b): StreamStreamJoin routes via hash(join.on) shard (TPC-CORR-09)` |

Range: `39b9536..ea251b0` on `arch/tpc-full-shard` (2 commits).

## Wave 4 Handoff (Perf Gate — 56-04)

Wave 4 (plan 56-04) MUST:

1. **Perf gate invocation** — reuse the Phase 55 bench harness with
   a new scenario variant that forces ≥1 cross-shard EnrichFromTable
   per event (already on the roadmap per 56-CONTEXT §D-D3). Also
   trigger a cross-shard SSJ dispatch if the scenario allows (the
   enrich case is the primary Phase 56 perf surface; SSJ co-located
   perf is covered by Phase 51).

   Command template (from 56-CONTEXT D-D3):
   ```bash
   BEAVA_SHARD_INBOX_SIZE=1048576 \
   BEAVA_ENRICH_CROSSSHARD_SCENARIO=1 \
   bench/fraud-pipeline/run_bench.sh MODE=complex DURATION=60 CPUS=8 CLIENTS=8
   ```

2. **Perf floor** — ≥ 1,059,261 EPS (85% of Phase 55's 1,246,190 EPS).
   Source baseline: `.planning/STATE.md` Phase 55 engineering-complete
   metric.

3. **Un-ignore 56-W4 markers** — `tests/crossshard_enrich_perf_smoke.rs`
   (2 tests currently ignored). These are the smoke-level assertions;
   the full perf gate runs in the bench harness.

4. **Counter observability checks** — verify all 5 Phase 56 counters
   emit real labels under bench load:
   - `beava_enrich_cross_shard_total{table=...}`
   - `beava_enrich_intra_shard_total{table=...}`
   - `beava_enrich_missing_total{table=...}`
   - `beava_ssj_cross_shard_total{join_id=...}`
   - `beava_crossshard_joins_registered_total{join_id=...}`

5. **If perf floor missed** — roll the `across-target parallel dispatch`
   56-NEXT candidate (noted in Wave 2 SUMMARY's deferred list) into
   Wave 4 scope: `read_entity_batch_at_shard` + `ssj_insert_at_shard`
   per-target flushes are currently sequential; parallelizing via
   `futures::join_all` is the most likely win.

## 56-NEXT Candidates Surfaced This Wave

- **Full N=1 ↔ N=8 byte-identical replay proptests** — both enrich and
  SSJ sub-cases of `sharding_parity` currently enforce only the routing
  invariant. A multi-shard engine fixture replaying the proptest events
  through N=1 and N=8 and byte-comparing outputs is the natural next
  step. Wave 2 deferred this; Wave 3 continues the deferral. Candidate
  for 56-NEXT or an early Phase 57 task.

- **Across-target parallel dispatch** — both `read_entity_batch_at_shard`
  (Wave 2) and `ssj_insert_at_shard` (Wave 3) dispatch to one target at a
  time. If Wave 4 perf is marginal, parallelize via `futures::join_all`
  over distinct targets. Precedent: Phase 51-02 scatter-gather.

- **Prune `event_time_ms` local in SSJ eval** — the computation remains
  as a `_event_time_ms_for_touch` binding; `apply_ssj_insert` derives
  its own timestamp. Low-priority cleanup for 56-NEXT.

- **Tracing crate adoption** — every `eprintln!` call site in this
  codebase (5+ hits across src/) is structured log spam. Adding the
  `tracing` crate would clean up all of them uniformly. Out of scope
  for Phase 56; candidate for a dedicated observability plan.

## Self-Check: PASSED

- [x] `src/engine/join_validator.rs` — CrossShardJoinWarning struct + validate_shard_keys rewrite — **FOUND**
- [x] `src/engine/pipeline.rs` — register() loop + StreamStreamJoin eval rewrite — **FOUND**
- [x] `src/server/signals.rs` — SignalRegistry.cross_shard_joins + emit_cross_shard_join_warning — **FOUND**
- [x] `src/server/http.rs` — /debug/warnings cross_shard_joins field — **FOUND**
- [x] `tests/cross_shard_stream_stream_join.rs` — 2/0/0 GREEN — **VERIFIED**
- [x] `tests/register_crossshard_join_warning.rs` — 4/0/0 GREEN — **VERIFIED**
- [x] `tests/sharding_parity.rs` — 13/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --lib` — 801/0/35 preserved — **VERIFIED**
- [x] `cargo build --release` exit 0 — **VERIFIED**
- [x] `cargo build --release --features state-inmem` exit 0 — **VERIFIED**
- [x] Phase 55 regression: cross_shard_tt_cascade_ownership 2/0/0, cascade_metrics 2/0/0, cross_shard_tt_cascade 2/0/0 — **VERIFIED**
- [x] Wave 2 regression: cross_shard_enrich_from_table 2/0/0 — **VERIFIED**
- [x] Phase 51 warnings: test_debug_warnings_endpoint 10/0/0, test_warnings_feed 10/0/0, test_warnings_dedupe 6/0/0, test_warnings_integration 4/0/0 — **VERIFIED**
- [x] Wave 4 markers intact: crossshard_enrich_perf_smoke 0/0/2 — **VERIFIED**
- [x] `39b9536` commit present in git log — **VERIFIED**
- [x] `ea251b0` commit present in git log — **VERIFIED**
- [x] All grep gates pass (ssj_insert_at_shard ≥2, cross_shard_joins ≥1, deprecated ≥1, no Err mismatch, no W3 markers) — **VERIFIED**
