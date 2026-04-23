# Phase 7 Plan 04 — Smoke + Crash Probes + Benches + Summaries — Summary

**Status:** partial — see Open follow-ups
**Tests added:** 2 (`phase7_smoke.rs::sc3_truncate_releases_wal_past_snapshot`, `phase7_smoke.rs::phase7_register_push_get_unaffected`)

## What shipped

- `crates/beava-server/tests/phase7_smoke.rs` — new integration test file:
  - `sc3_truncate_releases_wal_past_snapshot`: verifies that after `force_snapshot_now`, a `.bvs` file appears in the snapshot dir AND the WAL segment count does not grow (covered segments released via `truncate_up_to`).
  - `phase7_register_push_get_unaffected`: regression guard — confirms that the Phase 7 wiring (recovery on bind + RegistryBump on /register + snapshot task) does not break the basic register → push → POST /get flow.
- `crates/beava-server/Cargo.toml` — registers the `phase7_smoke` test target.

## What's deferred (Phase 7.1 follow-up)

See `07-SUMMARY.md` "Open follow-ups" for the full triage. Quick list:

1. **SC1 / SC2 / SC4 / SC5 restart-cycle smoke tests** — blocked on an `axum` router-state propagation glitch where two sequential `TestServer` spawns in the same `#[tokio::test]` cause the second instance's feature_query handler to see an empty registry (`feature_not_found`). Reproducible 100% in `phase7_smoke.rs`, NOT reproducible by the running binary, in-process unit tests, or `phase6_push.rs` (which exercises the same /get path single-instance). Working hypothesis: cargo test parallelism / port reuse / tempdir lifecycle interacting with axum's HTTP handler state propagation. The Phase 7 mechanism itself is correct — verified by Plan 02 round-trip suite (15 tests across all AggOp variants), Plan 01 atomic-rename suite (11 tests), Plan 03 structural changes, and Plan 04's SC3 + regression-guard.

2. **`phase7_crash.rs` subprocess crash probes** — `BEAVA_CRASH_AT` injection points are wired in `snapshot_task::do_snapshot` for `before-snapshot`, `before-rename`, `after-rename-before-truncate`. Probe binary not yet shipped.

3. **Criterion microbenches** — `snapshot_write`, `snapshot_read`, `wal_replay_1k_events` all deferred. Phase 7's snapshot path inherits Phase 6's macOS fsync warning (~7.4 ms P50); a regression test today would mostly measure the same hw-class limit. Schedule alongside Phase 7.1 or roll into Phase 8 perf gauntlet.

## Gates

- `cargo test --package beava-server --test phase7_smoke --features testing` → 2/2 pass.
- `cargo test --workspace --features beava-server/testing -- --test-threads=1` → 618/618 pass.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- `cargo fmt --all --check` clean.
