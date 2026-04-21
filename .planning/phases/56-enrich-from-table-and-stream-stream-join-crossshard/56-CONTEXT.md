# Phase 56: 56-enrich-from-table-and-stream-stream-join-crossshard - Context

**Gathered:** 2026-04-20
**Status:** Ready for planning
**Mode:** Auto (generated inside `/gsd-autonomous --auto` chain; decisions seeded from ROADMAP-locked items and Phase 55 carryover)

<domain>
## Phase Boundary

EnrichFromTable and StreamStreamJoin operate correctly when left/right keys hash to different
shards. EnrichFromTable performs a synchronous cross-shard read via `ShardOp::ReadEntityAt`
when the right-side key's shard differs from the current shard (today it silently returns
`Missing`). StreamStreamJoin's buffer lives on the shard owning `hash(join.on) % N`; both
sides route there. The register-time `JoinShardKeyMismatch` error (TPC-CORR-04) is relaxed
at runtime to a logged warning with perf impact noted.

**Out of scope (explicit):**
- Retraction propagation through cross-shard joins — Phase 57.
- Source-table DELETE retraction consumers — Phase 57.
- Async / non-blocking cross-shard reads — synchronous-with-batching is the Phase 56 call;
  revisit only if SC-4 p99 budget is exceeded.
- Connection-handling / Tokio rewrite — Phase 58.
- Any wire-format optimization on the enrichment read path — Phase 59.

</domain>

<decisions>
## Implementation Decisions

### Area A — EnrichFromTable Cross-Shard Read

- **D-A1 (read primitive):** New `ShardOp::ReadEntityAt { target_shard, table_name, key, reply: oneshot::Sender<Option<EntityState>> }` — symmetric to Phase 54 Wave 2 `UpsertTableRow` scatter-gather. Source shard `try_send`s to target shard's inbox and awaits oneshot reply. Reuses `crossbeam bounded(1)` oneshot pattern locked by 54-02.
- **D-A2 (synchronous with batching):** Source shard blocks on oneshot during enrichment read (ROADMAP-locked). When a downstream has multiple enrichments, the source shard coalesces per-target-shard into a single `ShardOp::ReadEntityBatch { target_shard, table_name, keys: Vec<String>, reply: oneshot::Sender<Vec<Option<EntityState>>> }` and pipelines reads across distinct target shards in parallel (spawn N oneshots, await all).
- **D-A3 (same-shard fast path):** When `hash(key) % N == current_shard`, read directly from local `PartitionHandle` — no inbox roundtrip. Same pattern as cascade's same-shard fast path in 54-02.
- **D-A4 (missing-row behaviour):** Preserve current `Missing` semantics (null-safe enrichment fields, downstream decides). Logged per-operator at debug level only (no per-event warning). `beava_enrich_missing_total{table}` counter increments.
- **D-A5 (target inbox full):** Propagate a `BeavaError::ShardOverload` upward — the source event's PUSH ack becomes 503 / `SHARD_OVERLOAD`. Unlike cascade (where we coalesce + retry), enrichment reads are on the critical path of a single event; back-pressuring at ingress is correct. Client retries whole batch.

### Area B — StreamStreamJoin Buffer Ownership

- **D-B1 (buffer shard):** Buffer lives on `hash(join.on) % N` (ROADMAP-locked). Both left and right events route there via the same dispatch primitive used by Stream→Table cascades: source shard accumulates per-batch, coalesces, `try_send`s a `ShardOp::SsjInsert { side: Left|Right, join_key, event }` to the target shard.
- **D-B2 (join match evaluation):** Evaluated inline on the target (join-owning) shard when an `SsjInsert` is applied. If a match is found, the resulting joined output is emitted to its own downstream via the existing cascade path (which may itself be cross-shard — pre-existing Phase 55 behaviour handles this).
- **D-B3 (buffer state backend):** Store in fjall under a dedicated partition `ssj-<join_id>/` alongside the join-owning shard's entity partitions. Same single-writer-per-shard invariant as Phase 53. Time-indexed by watermark for eventual eviction at `history_ttl` (retraction work is Phase 57).
- **D-B4 (register-time validation relaxed):** Remove the current `JoinShardKeyMismatch` hard-reject in `register()` (Phase 51 TPC-CORR-04 enforcement). Replace with: `tracing::warn!("CrossShardJoinWarning: join '{}' on field '{}' has mismatched left shard_key='{}' right shard_key='{}' — both sides will be shuffled to hash({}) % N. Expected perf impact: +1 inbox hop per event, +partition for join buffer. Co-locate by setting shard_key='{}' on both streams if this is hot-path.", join.name, join.on, left.shard_key, right.shard_key, join.on, join.on)` plus increment `beava_crossshard_joins_registered_total{join_id}`. Also surface via `/debug/warnings`.
- **D-B5 (co-location preserved):** When both sides already use `shard_key=join.on`, no relaxation applies — runtime path is unchanged, zero extra hops. The warning only fires for the mismatched case.

### Area C — Canonical Refs & Registration Flow

- **D-C1 (warning surface):** `/debug/warnings` gains a `cross_shard_joins: [{join_id, left_shard_key, right_shard_key, on_field, perf_note}]` field (extend the existing warnings endpoint introduced in Phase 51).
- **D-C2 (error migration):** `BeavaError::JoinShardKeyMismatch` is kept as a variant but no longer raised from `register()`. A new `BeavaError::CrossShardJoinWarning` is introduced as an INFO-level log event (not an error). Downstream (SDK, CLI) consumers of register() no longer see this as a failure.
- **D-C3 (TPC-CORR-04 updated):** REQUIREMENTS.md TPC-CORR-04 is relaxed from "register MUST reject mismatched shard_keys" to "register MUST accept mismatched shard_keys with a logged warning and co-location perf note" — in scope of this phase.

### Area D — Test Scope + Perf Gate

- **D-D1 (RED-first TDD):** Full RED suite lands Wave 0: SC-1 EnrichFromTable cross-shard, SC-2 SSJ cross-shard, SC-3 register accepts with warning, SC-4 latency sample, SC-5 perf gate harness. Follows Phase 54/55 Wave-0 pattern — `#[ignore]` markers with "passes at Wave N" comments.
- **D-D2 (integration tests):**
  - `tests/cross_shard_enrich_from_table.rs` — SC-1 (Txn on shard-J, Country on shard-K, verify enrichment populated).
  - `tests/cross_shard_stream_stream_join.rs` — SC-2 (L and R with different `shard_key`, assert join fires on `hash(join.on) % N`).
  - `tests/register_crossshard_join_warning.rs` — SC-3 (register succeeds; warning captured via `tracing::subscriber::with_default`).
  - Extend `tests/sharding_parity.rs` proptest with one new generator producing mismatched-shard enrich + join scenarios.
- **D-D3 (perf gate):** Reuse `bench/fraud-pipeline/run_bench.sh MODE=complex DURATION=60 CPUS=8 CLIENTS=8` with a new scenario variant that forces ≥1 cross-shard EnrichFromTable per event (via a Country enrichment table whose key does not match the event's shard_key). Floor: 85% of Phase 55 perf-gate candidate (1,246,190 EPS) → **≥ 1,059,261 EPS**. Apply the 54-NEXT inbox sizing knob (`BEAVA_SHARD_INBOX_SIZE=1048576`).
- **D-D4 (metrics):**
  - `beava_enrich_cross_shard_total{table}` — count of cross-shard enrichment reads.
  - `beava_enrich_intra_shard_total{table}` — same-shard fast-path count.
  - `beava_enrich_missing_total{table}` — cross-shard read returned None.
  - `beava_ssj_cross_shard_total{join_id}` — SsjInsert sends over inbox.
  - `beava_crossshard_joins_registered_total{join_id}` — count of warnings at register time.

### Claude's Discretion

- Exact SPSC message envelope for `ReadEntityAt` vs `ReadEntityBatch` (one opcode with `Vec<String>` always, or two variants) — pick whichever reads cleaner; batching path is the common case.
- Whether `ssj-<join_id>/` is one fjall partition per join or shared across joins on the same shard — pick per cache/compaction fit; can be tuned in Phase 63.
- Exact text of the `CrossShardJoinWarning` log line — match Phase 51's `JoinShardKeyMismatch` message style.
- Memory layout of the per-batch enrichment coalesce — any reasonable `HashMap<(target_shard, table), Vec<key>>` works.

### Folded Todos

None — no backlog items flagged for Phase 56 in the current todo queue.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 56 Source of Truth
- `.planning/ROADMAP.md` § Phase 56 — goal, locked decisions (EnrichFromTable synchronous; SSJ buffer on `hash(join.on)%N`), success criteria, requirements (TPC-CORR-08, TPC-CORR-09).
- `.planning/STATE.md` — current milestone state; Phase 55 engineering-complete baseline (1,246,190 EPS).
- `.planning/REQUIREMENTS.md` — TPC-CORR-04 relaxation (co-location warning), TPC-CORR-08, TPC-CORR-09.

### Architecture
- `.planning/arch/TPC-SHARD-DESIGN.md` — TPC architecture baseline.
- `.planning/arch/TPC-RESEARCH.md` — research backing the v1.2 design.

### Phase 55 Handoff (direct parent)
- `.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-CONTEXT.md` — cascade batching mechanics, coalesce strategy, target-inbox-full backpressure (reused here for enrichment path and SSJ routing).
- `.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-01-PLAN.md` — CascadeTarget trait + CascadeBuffer primitive that SSJ routing extends.
- `.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-04-PLAN.md` — wave-final scatter-gather pattern for downstream cascade delivery.

### Phase 54 Primitives Reused
- `.planning/phases/54-legacy-engine-removal/54-02-storeview-widening-and-scatter-gather-cascade-PLAN.md` — `try_send` + `crossbeam bounded(1)` oneshot pattern; `BeavaError::ShardOverload`.
- `.planning/phases/54-legacy-engine-removal/54-CONTEXT.md` — scatter-gather contract, StoreView::Sharded surface.
- `.planning/phases/54-legacy-engine-removal/deferred-items.md` — 54-NEXT inbox sizing, cross-shard counters.

### Phase 51 Prior Enforcement (now relaxed)
- `.planning/phases/51-cross-shard-queries-joins/51-04-PLAN.md` — original `JoinShardKeyMismatch` register-time reject; D-B4 here replaces the hard-reject with a warning.
- `.planning/phases/51-cross-shard-queries-joins/51-03-PLAN.md` — `/debug/warnings` endpoint extended in D-C1.

### Phase 52 / 53 Primitives Reused
- `.planning/phases/53-fjall-state-backend/53-CONTEXT.md` — fjall partition layout; single-writer invariant that constrains `ssj-<join_id>/` ownership.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/engine/pipeline.rs::cascade_table_upsert_on_shard` — scatter-gather cascade primitive (54-02) that EnrichFromTable cross-shard read re-uses the shape of (`try_send` + oneshot + blocking recv + ShardOverload on full).
- `src/shard/thread.rs` — shard event loop with `ShardOp` dispatch arms; add `ReadEntityAt` / `ReadEntityBatch` / `SsjInsert` variants here.
- `src/engine/operators.rs::EnrichFromTable` — today returns `Missing` on cross-shard miss; D-A1 wires the cross-shard read.
- `src/engine/operators.rs::StreamStreamJoin` — today uses a per-shard buffer; D-B1 relocates buffer ownership to `hash(join.on)%N`.
- `src/engine/register.rs` — today emits `JoinShardKeyMismatch`; D-B4/D-C2 replace with warning.
- `src/server/debug_warnings.rs` (Phase 51) — extend with `cross_shard_joins` field per D-C1.

### Established Patterns
- ShardOp variant + thread-loop arm + StoreView-widening is the pattern for every cross-shard operation — set in 54-02 (`UpsertTableRow`, `TombstoneTableRow`) and extended in 55-01 (`CascadeTarget` / `CascadeBuffer`). Phase 56 adds three more variants in the same mould.
- `crossbeam::bounded(1)` oneshot + blocking `recv` is the established SPSC reply pattern; per 54-02 deadlock analysis it is safe when source and target shards are distinct threads (always true here by construction).
- `tracing::warn!` + `/debug/warnings` JSON endpoint is the runtime-warning contract (Phase 51).

### Integration Points
- `register()` in `src/engine/register.rs` — warning replaces error.
- `src/engine/pipeline.rs::push_with_cascade_on_shard` — enrichment read injected into the operator evaluation path.
- `src/shard/thread.rs::shard_event_loop` — new `ShardOp` arms.
- `bench/fraud-pipeline/run_bench.sh` — new scenario variant for forced cross-shard enrichment.

</code_context>

<specifics>
## Specific Ideas

- **Symmetry with Phase 55 cascade:** The enrichment-read + SSJ-insert cross-shard primitives should look structurally identical to 55-01's `CascadeTarget` dispatch. A reviewer should be able to read the three operations (cascade write, enrichment read, SSJ insert) and see the same five lines: check same-shard fast path → accumulate per-target → end-of-batch coalesce → `try_send` → await oneshot / handle ShardOverload.
- **Relaxation is additive, not destructive:** The original `JoinShardKeyMismatch` error variant stays in `BeavaError` for back-compat of any caller matching on it; it is just no longer raised from `register()`. This mirrors how Phase 54 kept `StoreView` variants rather than deleting them.

</specifics>

<deferred>
## Deferred Ideas

- Retraction propagation through cross-shard joins and cascades — **Phase 57** (already on roadmap; this is the explicit next phase).
- Async / non-blocking cross-shard enrichment reads — revisit only if SC-4 p99 budget (≤ 2× Phase 55 baseline) is exceeded. If triggered, becomes 57-NEXT or a Phase 57.5.
- Join-buffer `history_ttl` eviction — deferred to Phase 57 (retraction work) since eviction and retraction are intertwined.
- SSJ buffer per-shard placement optimization for keys with very high cardinality — Phase 63 perf tuning.

</deferred>

---

*Phase: 56-enrich-from-table-and-stream-stream-join-crossshard*
*Context gathered: 2026-04-20*
