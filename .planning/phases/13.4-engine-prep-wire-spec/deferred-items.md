# Phase 13.4 — Deferred Items

Out-of-scope discoveries logged by parallel executors. Each item is owned by another plan
or the closure plan; do NOT fix from inside an executor whose scope is unrelated.

## Logged 2026-05-04 by Plan 13.4-02 executor

### Plan 01 op-rename lockstep gap

After Plan 01's GREEN landed (commit `8f47c97`), seven in-tree tests still use the OLD op
names (`avg`, `variance`, `stddev`, `count_distinct`, `percentile`) and now fail at
register-time with HTTP 400. They need lockstep updates to the NEW names (`mean`, `var`,
`std`, `n_unique`, `quantile`):

- `crates/beava-server/tests/phase5_smoke.rs::sc3_all_8_operators_e2e`
- `crates/beava-server/tests/phase10_sketch_recovery.rs::sc2_sketch_state_survives_snapshot_restart`
- `crates/beava-server/tests/phase10_sketch_recovery.rs::sc2_sketch_state_survives_wal_replay_no_snapshot`
- `crates/beava-server/tests/phase10_sketch_smoke.rs::phase10_sketch_pipeline_register_push_get_works`
- `crates/beava-core/src/agg_compile.rs::tests::rule11_count_distinct_op_name_recognized`
- `crates/beava-core/src/agg_compile.rs::tests::rule11_percentile_op_name_recognized_with_q`
- `crates/beava-core/src/agg_compile.rs::tests::rule11_percentile_q_out_of_range_rejected`

Owner: Plan 01 executor (or Plan 13.4-10 closure if Plan 01 is done). The Plan 01 task
text mentioned "lockstep updates" but these files were missed. The fix is mechanical:
swap each old op name for its new name.

### Plan 05 clippy gap

`crates/beava-server/tests/phase13_4_table_derivation_allowed.rs:66-70` has
`assert!(true, "...")` which trips `clippy::assertions_on_constants` under
`cargo clippy --workspace --all-targets --all-features -- -D warnings`. The fix is
either to delete the assertion or to add `#[allow(clippy::assertions_on_constants)]`
above it (or replace with a `let _ = "...";` to keep the message).

Owner: Plan 05 executor (or Plan 13.4-10 closure if Plan 05 is done).

### Plan 03 / Plan 07 mid-stream worktree breakage (RESOLVED — was transient)

While Plan 02 was active, Plan 03 had committed an `apply_shard.rs` arm that called
`dispatch_batch_get_sync(...)` before the function existed in `runtime_core_glue.rs`.
Plan 07 had a partially-edited `server.rs` (mid-merge with conflict markers from a
local stash). Both resolved when Plan 03 (`1f69ede`) and Plan 07 (`f719f6c`, `ba4cde6`)
landed their GREEN commits. Workspace builds clean now.

No action needed.
