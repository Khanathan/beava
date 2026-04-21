# Phase 57 — Deferred Items (57-NEXT)

Post-engineering-close follow-ups. Priority-ordered.

Phase 57 engineering is COMPLETE — TPC-CORR-10 closed, perf gate PASSED at
1,297,293 EPS (+20.5 % over 1,076,322 floor). These items are carried
forward as optimization or SDK-surface work, not as correctness blockers.

## #1 — HIGH — Wire-REGISTER for `@bv.source_table` (inherited from Phase 56 56-NEXT #6)

Unblocks **both** Phase 56 SC-5 (cross-shard enrichment scenario perf gate)
**and** Phase 57 D-D4 (advisory retraction-firing micro-bench). Today the
Python SDK decorator emits `kind="table"` (generic `TableSource` path);
`src/engine/register.rs::SOURCE_TABLE_KIND` rejects everything that isn't
`"source_table"`, and the in-process Rust helper `register_source_table()`
is never called from the wire dispatch. Once landed, the scenario script
(`benchmark/fraud-pipeline/scenario_crossshard_enrich.py`), the harness
branch (`BEAVA_ENRICH_CROSSSHARD_SCENARIO=1`), and a future
retraction-firing-burst harness all work end-to-end.

- **Scope:** ~40 LOC Rust (register.rs dispatch) + ~6 LOC Python
  (`_serialize.py::_compile_source`) + 2 integration tests
- **Inherits from:** 56-NEXT #6 (currently HIGH)
- **Affects:** Phase 56 SC-5 close; Phase 57 D-D4 advisory capture

## #2 — MED — Full SsjSideMap + event_id threading through `apply_ssj_insert`

The Wave 3 `fan_out_retraction_for_join_side` helper uses a conservative
scope (row_key == entity_key) that covers SC-2's contract because
`delete_entity` wholesale-removes the L entity on the
`L.shard_key == join.on` path (wiping `__ssj_LR` under user_1). Full
cross-shard SSJ rebind — where left side and right side live on different
shards AND the downstream of the join emits a keyed row elsewhere —
requires tracking individual joined-output identity:

- (a) Extend `ShardOp::SsjInsert` payload with `primary_event_id` for
  the arriving event.
- (b) Add `SsjSideMap { left_event_id_to_joined_rows,
  right_event_id_to_joined_rows }` to the `__ssj__` EntityState.
- (c) Extend `apply_ssj_insert` to return the matched counterparty's
  primary_event_id alongside the event map.
- (d) Update V10 wire to round-trip `SsjSideMap`.
- (e) Update the 3 apply_ssj_insert unit tests.

**Not gating** for TPC-CORR-10 closure — SC-2 GREEN via the conservative
scope. Filed for completeness as the observationally-richer surface.

## #3 — MED — Cross-batch DELETE retraction coverage (secondary reverse index)

`fan_out_retraction_for_source_table` scans `dirty_set_for_stream_snapshot`
per downstream, which bounds the walk to rows touched in the current batch.
The primary correctness case (push → enrich → DELETE in the same batch) is
covered by the mark-dirty discipline tested by SC-1. Rows emitted in prior
batches + still live on the shard are NOT in the scan scope — cross-batch
retraction requires a secondary reverse index on
`contributing_inputs.source_table_keys`.

- **Scope:** Per-shard `BTreeMap<(table_name, table_key),
  SmallVec<[row_key; 4]>>` populated alongside `contributing_inputs` write
- **Perf impact:** small allocation per source-table-driven enrichment
  emit; offset by O(|affected|) retraction scan instead of O(|all dirty|)
- **Trigger:** revisit if a Phase 58+ workload reveals cross-batch
  retraction correctness gap or perf surface

## #4 — MED — Batched retraction coalesce (C1 from contingency ladder)

The contingency ladder C1 remains available as a future latency improvement
even though the Phase 57 gate PASSED without it (headroom 20.5 %). Would
land a new `ShardOp::RetractDownstreamBatch { target_shard, entries:
Vec<RetractEntry> }` variant + per-target coalesce buffer; apply per-entry
on target; single oneshot reply carrying `Vec<RetractOutcome>`. ~5-10 %
latency win on high-fan-in tombstones (e.g. GDPR user-delete cascading
through N downstreams of the same shard).

- **Scope:** ~100 LOC Rust (new variant + coalesce buffer + dispatch arm)
  + 2 unit tests
- **Trigger:** Phase 58+ perf regression OR a reported operational
  high-fan-in tombstone hotspot

## #5 — LOW — Async / background retraction for non-critical-path

Today retractions are synchronous on the write path of the retracting
event. For very high-fan-in tombstones (GDPR user-delete cascading through
millions of events), an async queue would absorb the burst and defer
downstream propagation off the event-arrival thread.

- **Scope:** new `ShardOp::RetractDownstreamAsync` variant that just
  enqueues; background shard-local work-stealing processor; opt-in via
  `RetractReason::*_async`
- **Trade-off:** latency-vs-eventual-consistency decision — may not land
  until v2
- **Trigger:** reported operational need

## #6 — LOW — Rewrite history beyond `history_ttl`

Today beyond-history retractions warn + skip (SC-3 contract). A future
operator may want to rewrite history (replay from the event log) for
forensic / correction use cases. Explicitly out of scope for v1.2 per
Phase 57 CONTEXT `<domain>`. Tracked as v2 retraction.

## #7 — LOW — UI / CLI for inspecting retraction graphs

A `/debug/retractions` endpoint surfacing the per-entity
`contributing_inputs` graph would help operator debugging of "why did this
entity get retracted?" questions. Not gating for v1.2.

## #8 — LOW — `tracing` crate adoption (cross-phase cleanup)

Phase 57-03 added an `eprintln!` warning in
`emit_retraction_beyond_history_warning` alongside the Phase 56-03
`eprintln!` spam noted in `56-NEXT #8`. Adopting `tracing` uniformly
would replace ~5-7 eprintln sites with proper leveled logging.

- **Scope:** cross-phase cleanup (Phase 55/56/57 and earlier)
- **Inherits from:** 56-NEXT #8

## #9 — LOW — Full N=1 ↔ N=8 byte-identical replay proptest with retraction

`sharding_parity::retraction_after_cascade` today enforces the routing
invariant for retraction (output keys match between N=1 and N=8). A full
byte-compare of the event log's post-retraction state at N=1 vs N=8
would prove retraction determinism end-to-end rather than via
observation of the derived key set.

- **Inherits from:** 56-NEXT #1 (was MED)
- **Trigger:** reported determinism concern (none yet)

## Carry-forwards (still open from earlier phases)

### From Phase 54

- **54-NEXT #1-5** — inbox auto-sizing, graceful client shutdown, cross-shard
  counters, state-inmem cfg cleanup (139 gates), shard-harness rewrite
  (~169 ignored tests)

### From Phase 55

- **55-NEXT #1** — SC-6 N>1 boot rematerialization fan-out (human_needed
  TPC-CORR-07 follow-up)
- **55-NEXT #8** — bench.py graceful-final on ProtocolError (observed again
  at Phase 57 perf gate — all 8 clients exit non-zero at EOS)

### From Phase 56

- **56-NEXT #1** — full byte-identical N=1 ↔ N=8 replay proptest (merged
  into Phase 57 #9 above)
- **56-NEXT #2** — across-target parallel dispatch in
  `read_entity_batch_at_shard` + `ssj_insert_at_shard`
- **56-NEXT #3** — SSJ buffer TTL eviction (intertwined with retraction
  cascade — partially addressed by Phase 57 `fan_out_retraction_for_join_side`
  but full TTL eviction still pending)
- **56-NEXT #6** — wire-REGISTER for `@bv.source_table` (promoted to
  Phase 57 #1 above — HIGH priority as it now unblocks both phases)

### From Phase 54 (still outstanding — operator-run)

- **TPC-PERSIST-04 soak** — 8h Hetzner CCX43 run at Phase 54+ HEAD;
  runbook at `.planning/phases/54-legacy-engine-removal/soak-runbook.md`;
  re-runnable at any Phase 55/56/57 HEAD since no state-format regressions.
