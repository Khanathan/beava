---
phase: 15-event-time-pit
type: context
created: 2026-04-24
status: locked
blocks: ["Phase 12 Plan 04 (event↔table join)"]
depends_on: ["Phase 11.5 (MVCC temporal store + retraction)", "Phase 14 (streaming correctness — watermark)"]
---

# Phase 15 — Event-time PIT temporal store — CONTEXT

## Why this phase exists

Phase 11.5 shipped an **LSN-keyed** MVCC temporal store. That was right for the retraction primitive (LSNs are the stable, globally-ordered event-IDs on a single-writer WAL). But it is **wrong for point-in-time joins** — the Phase 12 `event↔table` join contract is "give me the row that was visible at the event's `event_time_ms`", not "give me the row at some LSN". LSNs are arrival-order; event_time is logical time. Out-of-order upserts arriving late correct history only if the chain indexes by event_time.

Phase 15 swaps the chain key from `Lsn → (event_time_ms, lsn)` and aligns every downstream axis (lookup API, retention, HTTP surface, snapshot format) with event-time semantics. After this phase:

- `lookup_at_event_time(key, as_of_ms)` → correct PIT for Phase 12 Plan 04
- Late upserts self-heal (no replay, no recompute)
- Retention auto-derives from registered streams' `tolerate_delay_ms`
- No public historical-extraction API ships in v0 (`GET /table?as_of=...` moves behind `BEAVA_DEV_ENDPOINTS=1`)

## Locked decisions

### D-01 — Chain key becomes `(event_time_ms, lsn)`

`VersionChain::chain` type changes from `BTreeMap<u64, MvccVersion>` (LSN-keyed) to `BTreeMap<(i64, u64), MvccVersion>` (composite `(event_time_ms, lsn)` keyed). `i64` so pre-1970 event_times stay addressable. LSN is **tiebreaker** for equal event_times (two upserts at the same event_time_ms resolve by arrival order — last arrival wins). This is the foundational shape change — every other decision in this phase is downstream of it.

### D-02 — Lookup API swaps axis

`TemporalStore::lookup_at_lsn(key, as_of_lsn)` is renamed to `lookup_at_event_time(key, as_of_event_time_ms)`. Semantics: the version at `max (event_time, lsn) ≤ (as_of, u64::MAX)` wins. Retraction-skip pass unchanged — still walks newest-first and honors `Retracted{undo_of: lsn}` markers (which still reference LSN, not event_time — see D-09). Old function removed; callers migrate.

### D-03 — Out-of-order upserts self-heal

A late upsert for `key` with event_time `T_old` arriving after a newer event_time `T_new` upsert simply slots into its event_time position. No replay, no recompute, no downstream invalidation. Lookups at any `as_of ≥ T_old` now see the late upsert as the visible version between `T_old` and the next later event_time. This is naturally commutative under the upsert set: any arrival order of the same upserts produces the same chain.

### D-04 — Retention derived from watermark

`TableDescriptor.derived_retention_ms(table)` is computed at register time by walking the registered DAG: the registry knows which streams join into this table, and each stream carries `tolerate_delay_ms`. `derived_retention_ms(T) = max{S.tolerate_delay_ms : S joins T}`. If no streams join T, retention falls back to "minimal latest-only" (a tiny floor — 1 second — enough for one recent version plus retraction headroom). The user-supplied `@bv.table(temporal=True, retention_ms=N)` override is honored if `N > derived_retention_ms` (for longer retract horizons); otherwise `derived_retention_ms` wins.

### D-05 — Retention axis shifts from wall-time to event-time

`sweep_retention(watermark_ms, retention_ms)` drops versions where `watermark_ms - version.event_time_ms > retention_ms`. Watermark here is the **table-wide watermark** — for a temporal table T joined by streams S₁..Sₖ, the table's watermark is `min{S_i.watermark}` (the slowest contributing stream dictates; otherwise retention could cut off valid late-arriving upserts). The old wall_ms-driven sweep is removed. `wall_ms` field on MvccVersion is replaced by `event_time_ms`.

### D-06 — `GET /table?as_of=...` moves behind dev gate

The entire `temporal_router` is relocated inside the `if dev_endpoints_enabled` block in `http.rs`. `POST /push-table` and `POST /retract` stay **production-mounted** (they're write primitives the SDK uses). Only `GET /table` is dev-gated. The `as_of` query param becomes event-time (epoch-millis), not LSN — documented in SUMMARY. Rationale: v0 OSS should not expose a public historical-extraction surface (privacy, misuse, compliance footguns). Dev gate suffices for tests and local debugging.

Wait — correction: `POST /push-table` and `POST /retract` are write primitives used by the Python SDK's `app.push_table()` / `app.retract()`. They must remain production-mounted. Only `GET /table/{name}` (the PIT read) moves behind the dev gate. Implementation: split `temporal_router()` into `temporal_write_router()` (always mounted when `app_state` present) and `temporal_read_router()` (mounted only under `dev_endpoints_enabled`).

### D-07 — Snapshot format version bump

Snapshots currently store MVCC chains as `BTreeMap<u64, MvccVersion>` with positional bincode layout. The new chain is `BTreeMap<(i64, u64), MvccVersion>` with `event_time_ms` on each version instead of `wall_ms`. **Pre-v0, breaking OK.** Add an explicit `snapshot_schema_version: u32` field to the snapshot header. Bump from v1 → v2. At load time, if header reports v1 (or missing), return `SnapshotLoadError::IncompatibleSchema { found: 1, required: 2 }` with a clear message directing the operator to discard the snapshot and restart from WAL. Migration test verifies both paths.

### D-08 — Register-time schema constraint

`@bv.table(temporal=True)` requires that **every stream writing upserts to this table** declares an `event_time_field` on its push event. The upsert handler uses the event's declared `event_time_field` to extract `event_time_ms` from the payload. Register-time check: for each temporal table T, walk the stream DAG; if any feeding stream lacks `event_time_field`, reject the register with HTTP 400 `temporal_table_requires_event_time_field` naming the offending stream. This prevents silent NULL-event-time upserts that would all collide at key `(0, lsn)`.

### D-09 — Retract tombstones unchanged; still LSN-keyed payload

`MvccVersion::Retracted { undo_of: u64 }` stays. `undo_of` is an LSN (the stable event_id the external `POST /retract {event_id}` surface speaks), not an event_time. At retract-apply time, the MVCC walker scans the chain for the `(_, lsn)` entry whose `lsn == undo_of` and marks it retracted. Retract's own position in the chain is keyed by `(retract_event_time, retract_lsn)` — where `retract_event_time = event_time of the upsert being retracted` (so the retraction is visible from `as_of ≥ retracted_version.event_time_ms`). This preserves the "retraction takes effect from the retracted version's event-time forward" semantic.

### D-10 — Register-time retention-growth diagnostic

When a `POST /register` causes `derived_retention_ms(T)` to grow (e.g., because a newly-registered stream has `tolerate_delay_ms=30_000` and the previous max was 5_000), the register response body includes an info-level diagnostic `BV-I-RETENTION-GROWTH` naming the table, the old retention, the new retention, and the contributing stream. Non-fatal — just surfaces memory-budget implications the user should know about. Shrinkage (stream removed) is **not** diagnosed (additive-only schema discipline means streams don't usually disappear).

### D-11 — `tolerate_delay_ms` threading via DAG walk

At register time, the registry already knows which streams feed which tables (via the stream → table write path declared in the SDK). `derived_retention_ms(T)` walks this DAG: for each stream S with a write-path to T, contribute `S.tolerate_delay_ms.unwrap_or(0)`. Result is cached on `TableDescriptor` as `derived_retention_ms: Option<u64>` and recomputed on every register. The sweep reads this cached value.

### D-12 — `wall_ms` removed from `MvccVersion`

`Live`, `Tombstone`, `Retracted` variants swap `wall_ms: u64` for `event_time_ms: i64`. Tombstones get the event_time of the delete event. Retractions inherit the event_time of the version they retract (D-09). Wall-clock is no longer part of MVCC state.

## Out of scope

- Public `GET /table/{name}?as_of=...` in production (stays dev-gated).
- Historical-extraction bulk export API.
- Stream retraction (still `501` per Phase 11.5 D-12).
- Table aggregations consuming temporal tables (Phase 17).
- Multi-writer or replica coordination for temporal state.
- Periodic background sweep (still snapshot-time and per-write soft cap, now keyed on event_time).
- Migration utility for v1 → v2 snapshots (v1 snapshots are rejected; operators restart from WAL).

## Grey areas (to resolve during execution)

- **Multiple streams with different tolerate_delays writing to the same table**: `derived_retention_ms` uses `max`. Should we revisit if this proves too generous in practice? Defer to post-v0 usage data.
- **Table-wide watermark computation when no streams are registered yet**: fall back to "no sweep" (retention is effectively unbounded until first stream registers). Harmless; a newly-registered table has nothing to sweep.
- **`event_time_ms` in the past-of-watermark but retention hasn't swept yet**: lookup still returns the old version. Correct — retention is an eviction policy, not a visibility policy.
