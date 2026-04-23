# Phase 7 — Partial Status (handoff)

**Updated:** 2026-04-23 (resume-agent session)
**Branch:** v2/greenfield
**Most recent commit:** `1c3ef60 feat(07-01): snapshot format + atomic writer + reader + retention`

## Planning — COMPLETE

All four plans drafted + committed:
- `07-CONTEXT.md` committed (commit `506cfa2`)
- `07-01-PLAN.md` committed (commit `0eb2eaf`) — snapshot primitives
- `07-02-PLAN.md` committed (commit `c490375`) — serde + SnapshotBody
- `07-03-PLAN.md` committed (commit `edff6fb`) — recovery, snapshot task, registry WAL, readiness
- `07-04-PLAN.md` committed (commit `dda8ebb`) — benches, smoke, crash probes, summaries

Gitignore fix for runtime WAL/snapshot dirs landed in `c72a7f7`. Stray
`crates/beava-server/beava-wal/` dir + leaked log file removed.

## Execution — PLAN 01 COMPLETE

### Plan 01 — DONE
- `test(07-01)` RED: 11 tests targeting not-yet-existing symbols (commit `fd27fd8`)
- `feat(07-01)` GREEN: all 11 tests pass (commit `1c3ef60`)
- Files: `crates/beava-persistence/src/{error.rs,lib.rs,snapshot.rs,snapshot_header.rs}`, `tests/snapshot_roundtrip.rs`, workspace `Cargo.toml` (bincode 1.3 added to `[workspace.dependencies]`)
- Test count: 590 → **601**
- `cargo fmt --all --check` clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean

### Plans 02, 03, 04 — NOT STARTED

Remaining work is substantial:
- **Plan 02** — serde derives across `AggOp`, `WindowedOp`, `Bucket`, `AggKind`,
  `EntityKey`, `Value` (in `row.rs`); new `beava-core::snapshot_body` module;
  ~16 round-trip tests. Watch out: `AggOp` currently holds `Arc<Expr>` via
  descriptors — plan's explicit note says snapshot carries AggOp STATE not
  descriptors (Arc<Expr> lives on `AggOpDescriptor`, excluded). Value enum
  might already be serde — verify before adding derives.
- **Plan 03** — generalize `WalSink::append_record(RecordType, payload)`;
  `DurabilityConfig` gains `snapshot_dir`, `snapshot_interval_ms`,
  `snapshot_retain_count`; `Registry::install_from_descriptors`; new
  `beava-server::recovery` module; new `beava-server::snapshot_task` module;
  `Server::bind` replaces the 100ms readiness stub with actual recovery;
  `register_router` writes `RegistryBump` WAL record before applying to
  in-memory registry; `TestServer::force_snapshot_now` test hook. Plan
  estimates 6 integration tests.
- **Plan 04** — 3 criterion benches (snapshot write, snapshot read, wal_replay);
  5 smoke tests (one per SC); 2 subprocess crash probes modeled on
  `phase6_crash.rs` / `phase6_crash_probe.rs`; `BEAVA_CRASH_AT` injection in
  snapshot_task; updated `.planning/perf-baselines.md`; `07-SUMMARY.md` +
  `07-VERIFICATION.md`.

## How to resume

From this branch, start fresh executor agent pointed at:
- `gsd-execute-phase 7` — should skip Plan 01 (already green) and start at Plan 02
- Or manually: `gsd-plan-check 7` then follow Plan 02's tasks 2a/2b in order.

Key cross-crate dependency to remember: `bincode = "1.3"` is already in
workspace deps; each crate that needs it should add `bincode = { workspace = true }`
to its Cargo.toml (Plan 02's green task is where beava-core picks it up).

## Known risks for remaining plans

1. **`AggOp` serde feasibility** — the `AggOp` enum is defined in `agg_op.rs`
   with variant state structs. Confirmed in plan 02's truths: each variant
   is "plain struct-of-POD" (no Arc). Should serialize cleanly. The
   `WindowedOp` is `Box<...>` — serde-derivable via default blanket impl.
2. **`Value` serde** — Plan says "CHECK if already serde. If not, add derives."
   Quick `grep -n 'derive' crates/beava-core/src/row.rs` will confirm.
3. **WalSink::append_record generalization** — existing `append_event`
   hardcodes `RecordType::Event` inside `flush_batch`. The refactor needs
   to thread the record type through `AppendRequest`. Internal-only change;
   external API preserved via `append_event` wrapper.
4. **Recovery LSN bump** — after replay returns `last_lsn`, the spawned
   `WalSink` must be configured with `initial_start_lsn = last_lsn + 1`.
   Current `Server::bind` spawns the sink BEFORE recovery; rework order so
   recovery runs first, THEN sink spawn sees the correct initial_start_lsn.
5. **macOS fsync WARNING** — hw-class-limited; snapshot write fsync inherits
   Phase 6's ~7.4 ms P50. Document as WARNING not BLOCKER in 07-VERIFICATION.

## Gates currently GREEN (after Plan 01 close)

- `cargo test --workspace --features beava-server/testing` — 601/601
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo fmt --all --check` — clean

Phase 7 success criteria UNVERIFIED (none of plans 02-04 executed yet).
