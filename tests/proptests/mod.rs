// Phase 53-05 (W-3 revision): the harness lives in a single file with a
// file-level `#![cfg(not(feature = "state-inmem"))]` gate. Under the default
// (fjall) build the file compiles and uses `Shard::with_partition` +
// ephemeral fjall keyspaces; under `--features state-inmem` the file is
// skipped entirely (its `shard.state.iter()` call is fjall-only API).
//
// The pre-53-05 gate here was `#[cfg(feature = "state-inmem")]` — Plan 03B's
// interim before 53-05 re-ported the harness. 53-05 flips the gate so the
// default build is the one that runs the parity test, matching the Phase 53
// policy that fjall is the production backend.
pub mod sharding_parity;
