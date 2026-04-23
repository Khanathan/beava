# Phase 7 — Snapshot + recovery + schema evolution — CONTEXT

**Phase:** 7
**Slug:** snapshot-recovery-schema-evolution
**Mode:** `--auto` (recommended defaults selected by Claude; logged inline)
**Depends on:** Phase 6 (WAL + idempotency) — complete
**Blocks:** Phases 8, 9, 10, 11 (all operator families depend on snapshot/recovery)

## Domain boundary

Periodic snapshot serializes in-memory state (registry + per-aggregation per-entity `AggOp` state + counters) and a WAL-covered `snapshot_lsn`. On restart, recovery loads the most recent valid snapshot, then replays WAL records with `lsn > snapshot_lsn` to catch up. Schema evolution across restart is preserved because the registry is serialized INTO the snapshot (not rebuilt from WAL alone). After successful snapshot, `WalSink::truncate_up_to(snapshot_lsn)` releases closed segments whose highest LSN is covered.

## Carrying forward from earlier phases

- **Registry serde baked in** (Phase 2 / 5) — `RegistryInner`, `EventDescriptor`, `TableDescriptor`, `DerivationDescriptor`, `OpNode`, `Expr`, `DerivedSchema` all already derive `Serialize`/`Deserialize`. Runtime caches (`compiled_chains`, `compiled_aggregations`, `feature_index`) are rebuildable from descriptors + `apply_registration`. (06-SUMMARY follow-up: "Schema evolution on replay (SRV-RECOV-05)".)
- **Apply-AFTER-fsync ordering (Phase 6 D-12)** — recovery replay must match runtime semantics. Replay calls `apply_event_to_aggregations` in LSN order, identical to hot-path apply.
- **Single-writer apply loop** (D-06 determinism) — recovery happens before the server accepts traffic; readiness flag flips AFTER recovery completes.
- **WAL framing + CRC32C** — `WalReader::read_all` already torn-tail safe; snapshot format reuses the same CRC32C primitive for checksum.
- **Determinism (BTreeMap, stable ordering)** — snapshot writer iterates in BTreeMap order; recovery reloads into BTreeMap; no rehashing drift.
- **macOS fsync hw-class WARNING** — snapshot write fsync inherits the same hw-class ceiling; document any numbers accordingly.
- **REQ-IDs closed this phase:** SRV-REG-04 (registry WAL'd on register — already live in Plan 06-03 via RegistryBump record type reservation — formalize here), SRV-RECOV-01..05.

## Gray areas — all auto-selected

### 1. Snapshot serialization format

**Decision (auto → recommended):** **bincode 1.x with explicit versioned header**.

| Option | Pros | Cons | Choice |
|---|---|---|---|
| bincode 1.x | zero-copy fast; already transitive dep; schema versioning via header | non-self-describing (breaks on field reorder if not versioned) | ✅ |
| postcard | embedded-friendly small size | extra dep; similar tradeoffs | — |
| serde_json | human-readable | 10-100× slower; 10× bigger | — |
| hand-rolled | max control | max effort; not worth for v0 | — |

**Rationale:** bincode + serde derive leverages existing `Serialize` on registry types. Add `#[derive(Serialize, Deserialize)]` to the AggOp state variants (CountState, SumState, AvgState, MinState, MaxState, VarianceState, RatioState, WindowedOp) — straightforward because they're plain struct-of-POD. The versioned header `SnapshotHeader { magic: u32, format_version: u16, created_at_ms: i64, snapshot_lsn: u64, registry_version: u64, payload_len: u64, payload_crc32c: u32 }` is hand-framed and self-identifying.

**Layout on disk:**
```
[SnapshotHeader (fixed-size, ~40 bytes, CRC-covered)]
[bincode-serialized RegistryInner (descriptors only — no runtime caches)]
[bincode-serialized Vec<(agg_node_name, Vec<(EntityKey, Vec<AggOp>)>)>]
[u64 next_event_id]
[u64 max_event_time_ms]
[trailing u32 CRC32C of everything above]
```

### 2. Atomic write strategy

**Decision (auto → recommended):** **write-tmp-then-rename + fsync-of-directory**.

Writer:
1. `snapshots/snapshot-{snapshot_lsn}.tmp`
2. stream-write header + body
3. `fsync` the tmp file
4. `rename(tmp, snapshot-{snapshot_lsn}.bvs)`
5. `fsync` the directory (`snapshots/`)
6. THEN call `WalSink::truncate_up_to(snapshot_lsn)`

Crash during steps 1-5 leaves the prior snapshot intact. Crash during step 6 may leave extra WAL (safe — replay tolerates re-applying past snapshot LSN by skipping). Never truncate WAL before the rename completes and its parent directory is fsynced.

### 3. Snapshot cadence / trigger

**Decision (auto → recommended):** **time-based (default 30s)** via a background tokio task. Config knob in `Config` (`snapshot_interval_ms`, default 30_000).

Not implementing in v0: event-count-based triggers, byte-delta-based triggers. Phase 7 keeps one knob; Phase 13 may add heuristics if the 30s default proves wrong.

### 4. Snapshot retention

**Decision (auto → recommended):** **keep latest N=2 snapshots** (current + immediately-prior, to satisfy SC4 "operator can fall back"). Older snapshots deleted after the successor's rename completes. Config: `snapshot_retain_count` default 2.

### 5. Recovery on startup

**Decision (auto → recommended):** **bind-block path**. `Server::bind` (or a new `Server::bind_with_recovery`) performs:

1. List `snapshots/*.bvs` — pick highest `snapshot_lsn`.
2. Load snapshot; verify CRC; on failure, try next-most-recent; if all fail, return `SnapshotCorrupt` error (operator recovers by removing bad files or starting fresh).
3. Install registry via `Registry::apply_registration`-equivalent low-level loader that ALSO rebuilds `compiled_chains`, `compiled_aggregations`, `feature_index` from descriptors (recompile path — already exists in register module, factor out).
4. Seed `AppState.dev_agg.state_tables` from the snapshot's state payload.
5. `WalReader::read_all(snapshot_lsn + 1..)` → for each record, dispatch: `Event` → apply via `apply_event_to_aggregations`; `RegistryBump` → call `apply_registration` on the embedded payload.
6. Once replay loop exits clean, flip **readiness flag** to `true`. `/ready` returns 200.

Before step 6, `/ready` returns 503; `/health` can return 200 (liveness != readiness). No traffic is accepted until readiness.

### 6. Schema evolution across restart

**Decision (auto → recommended):** **serialize registry descriptors into the snapshot verbatim; WAL RegistryBump records replay after snapshot_lsn** (SRV-REG-04 + SRV-RECOV-05 fold together).

Each successful `POST /register` writes a `RegistryBump { version, payload_nodes }` record to the WAL (opcode reserved in Phase 6, format finalized here). Snapshot captures the registry at `snapshot_lsn`. On recovery, snapshot restores registry-up-to-snapshot, then WAL replay re-applies `RegistryBump` records for versions after that, each triggering `apply_registration` in-process (same code path as live registration). Net effect: byte-for-byte restoration of the registry in the order it was built, and every subsequent event-record replay sees the right schema version.

**Compatibility posture:** additive-only is the product contract (Phase 5 REQ); the registry diff engine already enforces it. Snapshot format itself stays at `format_version=1` for the whole v0 cycle; any future breaking change bumps the constant and recovery refuses to load older/newer versions with a clear error.

### 7. Corruption handling (SC4)

**Decision (auto → recommended):** **soft failover to prior snapshot; log + exit-with-code if all corrupt**.

- Snapshot header CRC mismatch → skip, try next-most-recent.
- Body CRC mismatch → skip.
- If all candidates exhausted → return `RecoveryError::AllSnapshotsCorrupt` from `Server::bind`. The binary main should print a clear message: "all snapshots corrupt at /path/to/snapshots; remove the dir to start fresh or restore from backup".
- WAL record CRC mismatch mid-replay → already handled by `WalReader` (torn-tail semantics); treat as end-of-log; readiness can still flip (operator sees log warning).

### 8. Performance benchmarks (Phase 7 deliverable per CLAUDE.md §Performance Discipline)

**Decision (auto → recommended):** add `crates/beava-persistence/benches/snapshot.rs` or `crates/beava-server/benches/recovery.rs` with at minimum:
- `snapshot/write_10k_entities_30agg` — end-to-end snapshot write (serialize + fsync) with 10k entities × 30 agg-ops.
- `recovery/load_snapshot_10k_entities_30agg` — snapshot read + state rebuild.
- `recovery/wal_replay_10k_events` — WAL replay hot loop (reuses Phase 5 apply hot path).

Baselines go into `.planning/perf-baselines.md` under Phase 7 rows. Compare against Phase 6 `wal/append_nofsync` (279.71 ns) — recovery apply per-event SHOULD be dominated by `apply_event_to_aggregations` (Phase 5 `apply/3agg_100ent_1Kevt` = 1.01 ms / 1000 = ~1µs per event); if replay exceeds 2µs/event flag WARNING.

### 9. Registry WAL record format (SRV-REG-04)

**Decision (auto → recommended):** `RegistryBump` payload = bincode-serialized `Vec<PayloadNode>` + new_version: u64. Same bincode variant as snapshot body for format unity. Written inside the same `WalSink::append_and_await_ack` call the push path uses, so register gets the same durable-before-ACK semantics.

### 10. Test strategy

**Decision (auto → recommended):**
- Unit: snapshot round-trip (write → read yields byte-equal RegistryInner + state) in beava-persistence.
- Unit: corrupted snapshot (flip byte in header) → loader returns error.
- Integration: end-to-end in beava-server — start TestServer, register, push N events, trigger snapshot, restart TestServer, assert feature query returns same values.
- Integration: register → snapshot → register-additive → restart → both schema versions still queryable.
- Crash smoke: subprocess-style test (like `phase6_crash.rs`) — push events, kill before snapshot fires, restart, verify via WAL replay. Push events past snapshot, kill, restart, verify via snapshot + WAL replay.
- Criterion benches (see §8).

Target: +30-50 tests.

## Out of scope (deferred)

- **Incremental / delta snapshots** — full snapshot on cadence is fine for v0; defer to Phase 13+.
- **Background concurrent snapshot** (copy-on-write) — v0 is single-threaded apply; snapshot briefly pauses apply (sub-ms for typical workloads). Acceptable.
- **Snapshot compression** (zstd) — defer; measure first.
- **Remote / replicated snapshots** — out of OSS v0 scope (commercial tier).
- **Automatic backup rotation policy beyond `retain_count=2`** — operator's job.
- **Schema MIGRATIONS** (non-additive) — explicitly forbidden by product contract; Phase 5 registry-diff enforces.

## Canonical refs

- `.planning/ROADMAP.md` §"Phase 7: Snapshot + recovery + schema evolution" (lines 252-264)
- `.planning/REQUIREMENTS.md` SRV-REG-04, SRV-RECOV-01..05
- `.planning/phases/06-wal-idempotency/06-SUMMARY.md` (Phase 6 close; follow-ups listed)
- `crates/beava-persistence/src/lib.rs` (WAL public API surface)
- `crates/beava-persistence/src/fsync_worker.rs` (`WalSink::truncate_up_to`)
- `crates/beava-persistence/src/reader.rs` (`WalReader::read_all`)
- `crates/beava-server/src/lib.rs` (`AppState`)
- `crates/beava-server/src/registry_debug.rs` (`DevAggState`)
- `crates/beava-core/src/registry.rs` (`RegistryInner`, `Registry::apply_registration`)
- `crates/beava-core/src/agg_state_table.rs` (`AggStateTable`, `EntityKey`)
- `crates/beava-core/src/agg_op.rs` (`AggOp` — needs serde derives added)
- `CLAUDE.md` §Conventions (TDD red-green), §Performance Discipline

## Success criteria (from ROADMAP — restated for tracking)

1. Run 1M events through the server, snapshot fires, restart → all features replayable; values match pre-restart. **(Covered by integration tests + criterion.)**
2. Add a new feature (additive registration + version bump), snapshot, restart → new feature still present. **(Covered by schema-evolution integration test.)**
3. RTO: 10GB state snapshot + 1GB WAL tail → server online within 30s on NVMe. **(Asserted via extrapolation from criterion — 10GB not runnable in CI; document extrapolation in VERIFICATION.md.)**
4. Corrupt snapshot (flipped byte) detected + logged; operator can fall back to previous. **(Covered by corruption-probe test.)**

## Plan seed

Four plans envisioned (planner may resplit):
- **07-01** — Snapshot format + writer (bincode + versioned header + CRC, atomic rename, retention). Unit tests for round-trip + corruption.
- **07-02** — Add serde to AggOp + RegistryBump WAL record type. Recovery loader — snapshot read + state rebuild + registry-recompile.
- **07-03** — WAL replay integration + readiness flag + periodic snapshot tokio task + `truncate_up_to` wiring.
- **07-04** — Criterion benches, end-to-end crash tests, schema-evolution tests, phase summary.

## Auto-selection log

- [auto] Snapshot format: bincode 1.x + versioned header + CRC32C (recommended; leverages existing serde).
- [auto] Atomic write: write-tmp → fsync → rename → fsync-dir (recommended; standard durable-write protocol).
- [auto] Cadence: time-based, 30s default, single knob.
- [auto] Retention: N=2.
- [auto] Recovery: bind-block, pre-readiness.
- [auto] Schema evolution: registry serialized in snapshot; WAL `RegistryBump` records replayed on catch-up.
- [auto] Corruption: soft failover to prior, fail-fast if all corrupt.
- [auto] Benches: ≥3 criterion benches; baselines updated.
- [auto] Registry WAL format: bincode PayloadNode + version.
- [auto] Test strategy: unit + integration + crash smoke + criterion.

## Next step

Auto-advance to `gsd-plan-phase 7 --auto`.
