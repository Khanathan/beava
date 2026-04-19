// Phase 53-03B: legacy N=1↔N=8 proptest parity harness uses the AHashMap
// `Shard::new()` constructor; it is only reachable under the `state-inmem`
// build. Plan 05 re-ports the harness on top of `Shard::with_partition` /
// ephemeral fjall keyspaces for the default build.
#[cfg(feature = "state-inmem")]
pub mod sharding_parity;
