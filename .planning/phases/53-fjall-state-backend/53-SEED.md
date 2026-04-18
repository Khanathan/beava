# Phase 53: fjall-state-backend — Seed Context

**Status:** Phase added to ROADMAP 2026-04-18; **awaiting `/gsd-discuss-phase 53`** to lock design decisions before planning.

## Phase Boundary (seed)

Replace the per-shard in-memory `AHashMap<EntityId, Row>` state (introduced in Phase 49) with a per-shard `fjall::Partition` under `data/shard-N/fjall/`. State becomes durable-by-default (fjall WAL + SSTables), unbounded in size (LSM tree paged from disk via block cache), and crash-safe without event-log replay on the critical path. The `tally migrate-to-fjall` tool converts existing v8 in-memory snapshots to fjall partitions in place.

Positioned LAST in v1.2 (after Phase 52 ship-gate) so fjall lands atop a verified TPC architecture rather than underneath it.

## User decisions locked so far

| # | Decision | Locked |
|---|----------|--------|
| Position | Phase 53, appended to v1.2 milestone (not deferred to v1.3; not inserted mid-milestone) | 2026-04-18 |
| Scope | **Full replacement** of in-memory state — AHashMap deleted; all entity state in fjall. Not additive, not snapshot-only. | 2026-04-18 |

## Requirements

`TPC-PERSIST-01` through `TPC-PERSIST-06` (see `.planning/REQUIREMENTS.md` § TPC-PERSIST).

## Open design questions (to be closed by `/gsd-discuss-phase 53`)

1. **fjall API surface.** Which crate version? (0.3.x current as of research date — verify against Context7 before planning.) Single-partition-per-shard vs multi-partition-per-stream-per-shard? Impact on compaction behavior and write amplification.
2. **Snapshot/checkpoint format.** fjall supports snapshot creation via `Partition::snapshot()`. Do we keep a separate v8-style metadata file for non-state info (StreamDefinition registry, per-shard LSN map from Phase 52 D-11, WatermarkState), or persist everything inside fjall as special keys?
3. **WAL fsync cadence.** Per-write fsync (durable on ack) vs periodic (batch) — tradeoff between throughput and acknowledged-data loss window on crash. `BEAVA_FJALL_FSYNC_MS` env knob?
4. **Block cache sizing.** Default based on `available_memory / N_SHARDS × fraction`? `BEAVA_FJALL_CACHE_MB` env override?
5. **Compaction strategy.** Leveled vs tiered? Threaded vs inline? Coexist with shard hot-path safely.
6. **Migration tool scope.** `tally migrate-to-fjall` — idempotent? Reversible? What happens if interrupted mid-migration?
7. **Reshard tool update.** The Phase 52 `tally reshard` tool needs to understand fjall partitions (not just v8 snapshots). Does reshard become a fjall-aware operation, or does migrate-to-fjall always run first?
8. **Performance regression budget.** Phase 53 ship-gate says −15% is acceptable vs Phase 52 in-memory baseline — is this the right number? Or should we budget tighter (say −10%) to preserve Phase 52's ≥3× architecture gate?
9. **N=1↔N=8 proptest parity harness update.** Does it run against fjall in both N=1 and N=8, or do we run HashMap-backed N=1 vs fjall-backed N=8 as a dual-path validation?

## Integration points (seed)

- `src/state/shard/` (from Phase 49): `Shard.state` field changes from `AHashMap` to `fjall::Partition`.
- `src/state/store.rs`: `ShardedStateStoreV1` (from Phase 49) either gains a `ShardedStateStoreFjall` sibling and is retired, or is refactored in place.
- `src/reshard/`: Phase 52's reshard module grows fjall awareness.
- `Cargo.toml`: add `fjall` dep (version TBD at discuss time).
- `data/shard-N/`: new `fjall/` subdirectory per shard.
- `docs/architecture-tpc.md`: new "State durability (fjall)" section.
- `docs/operations.md`: new `BEAVA_FJALL_*` tuning knobs.

## Open canonical refs to gather at discuss time

- fjall crate docs (current version, Context7)
- fjall design rationale blog posts (author's Medium, if extant)
- LSM tree fundamentals if the planner needs primer (RocksDB design paper, etc.)
- Any prior Beava experiments with embedded KV stores (check `src/state/` git log for abandoned sled/rocksdb/redb attempts)

---

*This file is a SEED, not a CONTEXT.md. Run `/gsd-discuss-phase 53` to promote it into a canonical CONTEXT.md with all D-level decisions locked.*
