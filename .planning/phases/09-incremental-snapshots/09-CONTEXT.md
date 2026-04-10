# Phase 9: Incremental Snapshots - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

Snapshot persistence only serializes changed entities, reducing snapshot write time and disk I/O proportional to change rate rather than total state size. Adds dirty-key tracking, delta snapshot files, and base + delta recovery.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase. Use ROADMAP phase goal, success criteria, and codebase conventions to guide decisions.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/state/snapshot.rs` — Current full snapshot with postcard serialization, format version v5, SnapshotState with entities/pipelines/backfill_complete
- `src/state/store.rs` — StateStore with AHashMap<EntityKey, EntityState>, clone_for_snapshot_with_gc() for snapshot extraction
- `src/main.rs` — Periodic snapshot timer using tokio interval, cooperative yielding pattern already in place

### Established Patterns
- Postcard (not bincode) for serialization — per locked decision
- Version byte prefix for format compatibility (currently v5)
- clone_for_snapshot_with_gc extracts serializable entities with lazy GC for removed features
- Snapshot path from TALLY_SNAPSHOT_PATH env var, defaults to "tally.snapshot"
- Both main.rs periodic and http.rs manual trigger use same snapshot path

### Integration Points
- `save_snapshot()` / `load_snapshot()` in snapshot.rs — entry points for serialization
- `clone_for_snapshot_with_gc()` in store.rs — extracts entity data
- Periodic timer in main.rs — triggers snapshot writes every 30s
- HTTP POST /snapshot — manual snapshot trigger
- `restore_from_snapshot()` in store.rs — loads entities on startup

</code_context>

<specifics>
## Specific Ideas

No specific requirements — infrastructure phase. Refer to ROADMAP phase description and success criteria.

</specifics>

<deferred>
## Deferred Ideas

None — discuss phase skipped.

</deferred>
