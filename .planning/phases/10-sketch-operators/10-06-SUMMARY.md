# Plan 10-06 — Snapshot+WAL recovery + cross-sketch proptest — SUMMARY

**Status:** DONE — SC2 closed
**TDD trace:** test(10-06) tests pass without further impl (Plan 10-05 + earlier serde tags carry the contract)

## What landed

- `crates/beava-server/tests/phase10_sketch_recovery.rs` (2 integration tests):
  - `sc2_sketch_state_survives_snapshot_restart` — register 5-sketch pipeline, push 200 events, force snapshot, drop server, respawn with same WAL+snapshot dirs, GET each feature → byte-equal pre/post.
  - `sc2_sketch_state_survives_wal_replay_no_snapshot` — same but no snapshot. WAL alone reconstructs the 5 sketch states.
- `crates/beava-core/src/sketches/mod.rs::proptest_round_trip` — 5 proptests (256 cases each) for `BloomFilter`, `EntropyHistogram`, `CountDistinctState`, `PercentileState`, `TopKState`. Asserts bincode serialize→deserialize preserves the externally-observable state (estimate / quantile / top / membership).

## Why both passed without fixes

- Plan 10-05's sketch state types use `#[derive(Serialize, Deserialize)]` with stable rename tags (`v0_count_distinct_*`, `v0_percentile_*`, `v0_top_k_*`).
- The `AggOp` enum derives Serialize/Deserialize and carries the new sketch variants — bincode's externally-tagged enum encoding picks them up automatically.
- `WindowedOp` already serialises its bucket array; the per-bucket `AggOp` round-trips with the new sketch variants.
- `SnapshotBody` already serializes `Vec<(EntityKey, Vec<AggOp>)>` per agg-node — no schema change needed.
- WAL replay hits the same `apply_event_to_aggregations` entry point, so sketch dispatch flows identically on replay.

## Test count delta

- Plan 10-05 final: 698
- Plan 10-06 final: 705 (+7: 2 recovery tests, 5 proptests)

## Gates

- `cargo fmt --all --check` — clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo test --workspace --features beava-server/testing -- --test-threads=1` — 705 passed
