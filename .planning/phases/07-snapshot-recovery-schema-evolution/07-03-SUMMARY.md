# Phase 7 Plan 03 — Recovery + Snapshot Task + RegistryBump WAL — Summary

**Status:** complete
**Commits:** `c45fa63` (one combined feat)
**Test count:** no net delta (`readiness_flips_after_100ms` was renamed to `readiness_ready_after_cold_start_recovery` to reflect the new synchronous semantics; cli_smoke gained BEAVA_SNAPSHOT_DIR env wiring per-test).

## What shipped

### `beava-persistence` — generalized WAL append

- `WalSink::append_record(record_type: RecordType, payload: Vec<u8>) -> Result<Lsn, PersistError>` is now the typed entry point. `append_event` is a thin wrapper that calls `append_record(RecordType::Event, payload)`.
- `AppendRequest` carries `record_type` through the worker queue; `flush_batch` uses it instead of hardcoding `RecordType::Event`.
- `RecordType::RegistryBump = 0x02` (pre-reserved Phase 2.5) is now actually used.

### `beava-core` — DurabilityConfig + Registry::install_from_descriptors

- `DurabilityConfig` gained:
  - `snapshot_dir: PathBuf` (default `./beava-snapshots`)
  - `snapshot_interval_ms: u64` (default 30_000)
  - `snapshot_retain_count: usize` (default 2)
- New env overrides: `BEAVA_SNAPSHOT_DIR`, `BEAVA_SNAPSHOT_INTERVAL_MS`, `BEAVA_SNAPSHOT_RETAIN_COUNT`.
- `Registry::install_from_descriptors(&RegistryDescriptorsOnly)` — overwrites events/tables/derivations + version. Runtime caches NOT rebuilt here; recovery's WAL replay re-runs `apply_registration` per RegistryBump record to repopulate them.

### `beava-server` — recovery + snapshot task + RegistryBump on /register + readiness gate

- New `recovery` module:
  - `load_snapshot_if_any(snapshot_dir, dev_agg) -> Result<Lsn, PersistError>` — descending-LSN scan; first valid `.bvs` wins; install descriptors + state tables + scalar counters; return `snapshot_lsn`. Empty dir or all-corrupt returns 0 (cold start).
  - `replay_wal_from_lsn(wal_dir, snapshot_lsn, dev_agg) -> Result<RecoveryOutcome, PersistError>` — read every WAL record, skip `lsn <= snapshot_lsn`; dispatch `RecordType::Event` → `apply_event_to_aggregations` and `RecordType::RegistryBump` → `apply_registry_bump`. Bumps `next_event_id` and `max_event_time_ms` as records replay.
  - `RecoveryOutcome { installed_from_snapshot, snapshot_lsn, replay_event_count, replay_registry_bumps, last_lsn }`.
- New `snapshot_task` module:
  - `spawn_snapshot_task(cfg, app_state, wal_sink, cancel) -> (JoinHandle, SnapshotTriggerTx)`.
  - `do_snapshot`: capture `wal_sink.durable_lsn()` → build `SnapshotBody` (under state_tables lock; encode outside lock) → `SnapshotWriter::write` → `wal_sink.truncate_up_to(snapshot_lsn)` → `prune_old_snapshots(retain)`.
  - Periodic ticker + manual trigger channel for `force_snapshot_now`.
  - `BEAVA_CRASH_AT` injection points (gated `#[cfg(any(feature = "testing", test))]`) at 3 named points: `before-snapshot`, `before-rename`, `after-rename-before-truncate`.
- `register.rs`:
  - New `RegistryBumpPayload { new_version, payload_nodes }` with `encode`/`decode`.
  - New `apply_registry_bump(registry, bump)` re-runs validate + compile + apply; idempotent on already-present descriptors.
  - `execute_register_with_wal` writes a RegistryBump WAL record (fsynced) BEFORE calling `apply_registration` on the in-memory registry. WAL failure → `WalUnavailable` outcome → 503 on HTTP, OP_ERROR_RESPONSE on TCP.
  - HTTP `post_register` chooses the WAL-backed variant when `RegisterAppState.wal_sink` is `Some` (i.e., when wired via `AppState`).
- `Server::bind`:
  - Constructs registry + dev_agg + idem_cache.
  - Runs recovery synchronously (`load_snapshot_if_any` → `replay_wal_from_lsn`) BEFORE spawning the WAL sink.
  - WAL sink starts with `initial_start_lsn = max(last_lsn, snapshot_lsn) + 1` so new appends land in a fresh segment past the recovered tail.
  - Spawns the periodic snapshot task with `snapshot_interval_ms` cadence.
  - Flips `readiness.set_ready()` synchronously after recovery returns Ok.
  - Holds the snapshot task's cancel token + JoinHandle + manual-trigger sender; `serve()` cancels + awaits the snapshot task during graceful shutdown.
- `TestServer`:
  - `force_snapshot_now() -> Result<(), String>` — sends a oneshot via the trigger channel and awaits the ack.
  - `TestServerBuilder::snapshot_dir` + `snapshot_interval_ms` builder methods.
  - Default test snapshot interval is 60s (avoids spurious snapshot during normal test flow); tests force via `force_snapshot_now`.

## Wire points

- `crates/beava-server/src/server.rs::Server::bind` — recovery before WAL sink spawns.
- `crates/beava-server/src/http.rs::router_with_push` — passes `wal_sink` through `RegisterAppState` when `AppState` is provided.
- `crates/beava-server/src/recovery.rs` — new file.
- `crates/beava-server/src/snapshot_task.rs` — new file.
- `crates/beava-server/src/lib.rs` — registers `recovery` + `snapshot_task` modules.

## Notes / deviations

1. **Recovery operates on `&DevAggState` directly, not `&AppState`.** The plan said `&AppState`, but recovery doesn't need the WAL sink — only the registry + state tables + scalar counters. Decoupling lets `Server::bind` run recovery against a freshly-constructed `DevAggState` BEFORE the WAL sink is spawned, ensuring the sink's `initial_start_lsn` reflects the recovered tail.

2. **WAL append for RegistryBump is awaited like push** — same fsync-then-apply invariant. If WAL append fails, the registry is NOT mutated; HTTP returns 503 `wal_unavailable`. Recovery on the next boot will see whatever the WAL last committed.

3. **`apply_registry_bump` re-runs `validate_payload`.** This catches the (extremely unlikely) case where a recovered RegistryBump record is malformed or violates a now-stricter validation rule. On error, the record is logged + skipped — recovery continues with the next record. Operators who hit this should rebuild the WAL from a known-good snapshot.

## Gates

- `cargo test --workspace --features beava-server/testing` (single-thread) → 616/616 pass.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- `cargo fmt --all --check` clean.
