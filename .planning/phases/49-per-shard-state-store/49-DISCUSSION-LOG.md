# Phase 49: per-shard-state-store - Discussion Log

> Audit trail only. Decisions in CONTEXT.md.

**Date:** 2026-04-18
**Phase:** 49-per-shard-state-store
**Areas discussed:** StateStore facade · Event log path + snapshot rename timing · WatermarkTracker migration · Python SDK shard_key server-side storage

---

## StateStore facade shape

| Option | Selected |
|--------|----------|
| `shards: Vec<Arc<Shard>>` + router | |
| Trait `ShardedStateStore` + impls | ✓ |
| Thin facade over `[Shard; N]` fixed array | |

**User's choice:** Trait `ShardedStateStore` + impls.
**Notes:** D-01 in CONTEXT.md. Flexibility hedge for Wave 2+ experimentation. Recommendation was Vec+router; user chose trait-based for future-proofing.

## Event log path + snapshot rename timing

| Option | Selected |
|--------|----------|
| Rename in Wave 4 — Wave 1 keeps current paths | ✓ |
| Rename now (Wave 1 lands layout + snapshot v8) | |
| Dual-path — Wave 1 writes new, reads old-or-new | |

**User's choice:** Rename in Wave 4.
**Notes:** D-03. Keeps Wave 1 diff tractable; preserves no-flag-day guarantee.

## WatermarkTracker migration strategy

| Option | Selected |
|--------|----------|
| Full relocation in Wave 1 | ✓ |
| Facade — WatermarkTracker delegates | |
| Parallel — keep WatermarkTracker + add per-shard | |

**User's choice:** Full relocation (after clarification).
**Notes:** User initially asked "How does this block global publish?" Resolved: full relocation makes Wave 3 lazy-publish purely additive; alternatives defer work and carry coupling cost. D-04/D-05/D-06.

## Python SDK shard_key server-side storage

| Option | Selected |
|--------|----------|
| New field on `StreamDefinition` | ✓ |
| Metadata sidecar (HashMap<&str, Value>) | |
| External registry file | |

**User's choice:** New field on StreamDefinition.
**Notes:** D-07/D-08/D-09. `Option<ShardKeySpec>` + `#[serde(default)]` so pre-Wave-1 snapshots deserialize cleanly. No snapshot-format bump needed in Wave 1.

## Claude's Discretion

- Exact trait method shape of `ShardedStateStore`.
- Where `BEAVA_SHARDS` env parsing lives.
- Field naming inside `Shard` struct.

## Deferred to later waves

- Event log path rename + snapshot v8 (Wave 4).
- DashMap/ArcSwap deletion (Wave 4).
- BEAVA_ENTITIES_SHARDS rename (Wave 2, TPC-INFRA-07).
- ShardKeyMissingWarning (Wave 2, TPC-DX-02).
- N>1 enforcement (Wave 2). Wave 1 warn-once if BEAVA_SHARDS>1 and proceeds at N=1.
