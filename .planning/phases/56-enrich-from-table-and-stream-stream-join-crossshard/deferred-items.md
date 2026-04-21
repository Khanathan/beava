# Phase 56 — 56-NEXT Deferred Items

These items surfaced during Phase 56 execution but were scoped out. File
them here; ROADMAP consumers should pull items 1 + 6 forward into Phase
57 planning (retraction) or a dedicated 56.5 patch if ops pressure on the
cross-shard perf gate mounts.

---

## 56-NEXT #1 — Full N=1 ↔ N=8 byte-identical replay proptest for enrich + SSJ cross-shard

**Status:** deferred from Wave 2 and Wave 3.

Both `sharding_parity::mismatched_shard_enrich_parity_n1_vs_n8` (Wave 2)
and `sharding_parity::mismatched_shard_join_parity_n1_vs_n8` (Wave 3)
currently enforce only the ROUTING invariant (the event hits the expected
shard) but do NOT drive a full multi-shard engine replay through the same
proptest events and byte-compare the resulting output stream against an
N=1 replay. A true byte-identical replay proptest is the natural next
step and would catch any routing-correct-but-semantically-divergent
regressions.

**Estimate:** ~120 LOC — add a two-engine fixture (N=1 and N=8), drive a
proptest-generated event stream through both, collect the resulting
output events in deterministic order per key, assert equality. Pattern:
see `tests/sharding_parity.rs::tt_cascade_parity_n1_vs_n8` (Phase 55).

**Priority:** Medium. The routing-only proptest is sufficient to prove
the dispatch layer is correct. The full replay is a nice-to-have
regression surface.

---

## 56-NEXT #2 — Across-target parallel dispatch in `read_entity_batch_at_shard` + `ssj_insert_at_shard`

**Status:** deferred from both Wave 2 and Wave 3.

The per-batch coalesce produces a `HashMap<(target_shard, table), Vec<key>>`
map, but the dispatch loop iterates it sequentially: send oneshot to target
1, await reply, send oneshot to target 2, await reply, etc. When a single
batch enriches against multiple source tables or a single SSJ arrives
both L and R in the same batch with different target shards, the
source shard blocks sequentially across those distinct-target hops.

**Measured gap:** Wave 4 gate run at Phase 56 HEAD on the default
fraud-pipeline workload (which hits only the coalesce fast-path, not the
cross-target path) landed 1,195,914 EPS (−4.0 % vs Phase 55 baseline).
The cross-target path is not exercised there. Once 56-NEXT #6 lands and
the cross-shard enrichment scenario runs, the Phase 55 perf-gate
comparison is what quantifies the gap.

**Estimate:** ~15 LOC using `std::thread::scope` to spawn a closure per
distinct target and join all replies at the end of the batch. See Phase
51-02 scatter-gather for the pattern.

**Priority:** Medium — promote to High if 56-NEXT #6 lands and the
cross-shard scenario EPS comes in below the 1,059,261 floor.

---

## 56-NEXT #3 — SSJ buffer TTL eviction

**Status:** deferred (Phase 57 territory, but filed here for visibility).

The Wave 3 `StreamStreamJoin` implementation still relies on the
pre-Phase-56 `within_ms` check per evaluated match to drop stale buffer
entries. Buffer rows themselves linger in the `ssj-<join_id>/` partition
until the partition's `history_ttl` evicts them. Intertwined with Phase
57's retraction propagation work — a late retraction inside the
`within_ms` window must revoke previously-emitted joined outputs.

**Estimate:** out-of-scope for 56-NEXT; belongs to Phase 57.

**Priority:** High for Phase 57 scope. Not actionable at this wave.

---

## 56-NEXT #4 — Per-join partition vs shared partition decision revisit

**Status:** deferred pending Wave-4 perf signal.

Wave 1 shipped `ssj-<join_id>/` as one fjall partition per join. This
scales linearly in #-of-joins but may inflate compaction pressure per
partition when #-joins is large. The alternative (one shared partition
per shard, with join_id as a prefix on the key) consolidates compaction
but risks read-amplification for narrow joins.

**Estimate:** Phase 63 perf-tuning scope.

**Priority:** Low. Not actionable until a multi-join customer workload
exposes the gap.

---

## 56-NEXT #5 — `/debug/warnings` cross-shard joins pruning / TTL

**Status:** deferred (surfaced Wave 3).

`SignalRegistry.cross_shard_joins: Vec<CrossShardJoinWarning>` dedupes by
`join_id` (T-56-03-01 mitigation) but entries never expire. For
long-running servers with rapid register/unregister cycles, this bucket
grows monotonically. Not a memory hazard at realistic scale (each entry
~500 bytes; 10k joins ≈ 5 MB) but a potential rust-edge in multi-tenant
setups.

**Estimate:** ~20 LOC to add a `pruned_at` timestamp + LRU cap at 10k
entries + a grep-style metric `beava_cross_shard_joins_pruned_total`.

**Priority:** Low. File opens only for shops that anticipate >10k
unique join_ids.

---

## 56-NEXT #6 ★ — Wire-path REGISTER dispatch for `@bv.source_table`

**Status:** **BLOCKING the cross-shard enrichment perf gate** (Phase 56
SC-5 `human_needed` hangs off this).

Phase 55 shipped `@bv.source_table` decorator + `upsert_table_row` /
`delete_table_row` wire methods, but never added a wire-REGISTER dispatch
arm to call `register_source_table()` on the server. Today:

- Python SDK `app.register(Countries)` emits `kind="table"` for both
  `TableSource` and `SourceTable` descriptors.
- Server's REGISTER handler dispatches on `kind` and falls through to
  the generic Source variant for `kind="table"`.
- `has_registered_source_table(name)` checks the `raw_register_jsons`
  bucket for `kind == "source_table"`.
- Upsert / delete paths require `has_registered_source_table()` → fail
  with "table not registered as @bv.source_table" if the SDK registered
  via `@bv.source_table` over the wire.

Consequence: the Phase 56 Wave 4 perf gate's cross-shard enrichment
scenario cannot seed `Countries` rows. Result: cross-shard enrichment
EPS gate remains `human_needed`. Correctness for Phase 56 is proven via
the 14 in-process Wave 2/3 integration tests that call
`register_source_table()` directly.

**Remediation scope:**

1. Extend `src/engine/register.rs` — add `V0RegisterPayload::SourceTable(SourceTableDescriptor)`
   variant serde-untagged above `Source(SourceDescriptor)`; its `apply()`
   arm calls `register_source_table(engine, name, key_fields, entity_ttl)`.
   Matches on a top-level `"kind": "source_table"` in the REGISTER JSON.
   (~30 LOC.)
2. Update `python/beava/_serialize.py::_compile_source` — when descriptor
   `_beava_kind == "source_table"`, override `d["kind"] = "source_table"`
   and include `key_fields` as an array (not `key_field` scalar). (~6 LOC.)
3. Add an integration test `tests/register_source_table_wire.rs` that
   spawns a server, drives Python-SDK-equivalent register via
   `BeavaClient` raw, asserts `has_registered_source_table("Countries")`
   returns true, and upserts a row + reads it via enrichment. (~40 LOC.)

Total: ~80 LOC + 1 new integration test. Estimate: ~1-2 hour ticket.

**Priority:** **High** — gates Phase 56 SC-5 close and any downstream
customer workload that uses @bv.source_table from Python.

---

## 56-NEXT #7 — Prune `event_time_ms` local in SSJ eval

**Status:** Wave 3 cleanup.

The `_event_time_ms_for_touch` binding in `pipeline.rs` StreamStreamJoin
eval remains because `apply_ssj_insert` derives its own timestamp
internally. The variable is dead-but-tolerated with a `_`-prefix.

**Estimate:** ~3 LOC removal + a `cargo test --release --lib` confirmation.

**Priority:** Very Low. Cosmetic.

---

## 56-NEXT #8 — `tracing` crate adoption

**Status:** Wave 3 surfaced.

Every `eprintln!` call site in `src/` (5+ hits, Wave 3 added one for
`CrossShardJoinWarning`) is structured-log spam. Adding the `tracing`
crate + `tracing-subscriber` would unify these into a single observable
stream with `tracing::warn!(target: "beava::register", …)` semantics.

**Estimate:** ~1 new dep + ~8 call-site rewrites + ~10 LOC tracing
configuration in `src/server/main.rs`.

**Priority:** Low. Candidate for a dedicated observability plan (Phase
61+).

---

## Carry-forwards from Phase 55

These 55-NEXT items remain open and applicable to Phase 56 too:

- **55-NEXT #2** — pre-existing `tests/test_concurrent.rs` 6/6 failures
  (pre-dates Phase 54). Carried through Phase 55 + 56. Not scoped here.
- **55-NEXT #8** — graceful `bench.py` / `scenario_crossshard_enrich.py`
  client shutdown at EOS (the trailing-edge `ProtocolError: shard inbox
  full — backpressure` warning is a cosmetic; final EPS aggregation still
  works).
