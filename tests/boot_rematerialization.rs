//! Phase 55 Wave 0 RED — SC-6 v8→v9 boot rematerialization + hard-fail on truncation.
//!
//! Contract (D-C1, D-C2, D-C3, Pitfall 3, Pitfall 7):
//!   - Snapshot schema_version 8 → 9. Loading v8 triggers automatic
//!     rematerialization: primary entity state reused from snapshot,
//!     downstream table state cleared + replayed via the new cascade
//!     path (main-thread single-writer; shard threads not yet spawned).
//!   - Truncated event logs (past the rebuild boundary) cause a boot
//!     hard-fail with an actionable error mentioning both
//!     "Event log truncated before LSN" AND "tally rebuild --from-source".
//!   - Pre-Phase-55 servers MUST reject v9 snapshots (no silent v8
//!     decode of v9 bytes — guards against pipeline-semantics drift).
//!   - state-inmem build (no event log on disk) skips rematerialization
//!     with a log line.
//!
//! Wave 3 (plan 55-03) lands the snapshot header bump + main-thread
//! rematerializer. Wave 0 landing: `#[ignore = "55-W3"]`.
//!
//! Run:
//!   cargo test --release --test boot_rematerialization -- --ignored

// NOTE: Several of these tests exercise v8→v9 migration and truncation
// paths that are meaningful only on the fjall backend. Gating the whole
// file preserves the Wave 0 "compiles on both builds" invariant by
// omitting the file from the state-inmem build entirely (mirrors the
// pattern used by sharding_parity.rs). The state-inmem-skip scenario
// itself is documented via comment below, not wired as a fjall-built
// test (Wave 3 may split it into a state-inmem-gated companion file).

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::spawn_two_shards;

/// SC-6 primary — v8 snapshot on disk with a downstream row planted on
/// the WRONG shard (simulating pre-Phase-55 sharding). Boot triggers
/// rematerialization; after boot the row lives on
/// `hash(output_key) % N`, nowhere else; subsequent snapshots encode
/// schema_version=9 and restart is a no-op.
#[test]
#[ignore = "55-W3"]
fn v8_snapshot_boots_and_rematerializes_to_v9() {
    // Build a v8 snapshot + matching primary event log in a tempdir.
    //   - Plant downstream row for key "merchant_X" on the WRONG shard
    //     (e.g. hash(input_user_id)%N rather than hash("merchant_X")%N).
    //   - Write snapshot outer format byte 0x08; SnapshotHeader without
    //     the Phase 55 schema_version field (serde default fills in 8).
    //   - Start server.
    //   - Assert: stderr contains "Pre-v9 snapshot detected; rematerializing"
    //   - Assert: read_entity_from_shard(correct, "merchant_X").is_some()
    //   - Assert: all other shards return None for "merchant_X".
    //   - Clean shutdown.
    //   - Assert: new snapshot outer byte == 0x09 AND schema_version == 9.
    //   - Restart.
    //   - Assert: stderr does NOT contain "rematerializing".
    unimplemented!("Wave 3 — SC-6 v8→v9 rematerialization");
}

/// D-C2 hard-fail — event log truncated past the rebuild boundary
/// causes boot to fail with an error whose string contains both
/// "Event log truncated before LSN" AND "tally rebuild --from-source".
#[test]
#[ignore = "55-W3"]
fn truncated_event_log_hard_fails_with_actionable_error() {
    // Write v8 snapshot + truncate per-shard primary event log past
    // rebuild boundary. Start server. Assert boot error message
    // includes both substrings above.
    unimplemented!("Wave 3 — SC-6 D-C2 truncation hard-fail with actionable error");
}

/// Pitfall 3 guard — loading a v9 snapshot via the pre-Phase-55 read
/// path MUST fail (NOT silently decode as v8). Prevents rollback-without-
/// rebuild from producing wrong downstream state.
#[test]
#[ignore = "55-W3"]
fn v8_server_rejects_v9_snapshot() {
    // Load a v9 snapshot using a load path clamped to LEGACY_V7_FORMAT
    // dispatch. Assert: returns None or explicit error, NOT a silent
    // V8 decode.
    unimplemented!("Wave 3 — Pitfall 3 guard (pre-55 rejects v9)");
}

/// Pitfall 7 guard — state-inmem build (no event log on disk) must
/// boot successfully; rematerialization step logs "skipped (state-inmem)".
///
/// Documented here for Wave 3 execution. The actual runtime path is
/// state-inmem-only so this compiled test is a stub; Wave 3 may move
/// the assertion into a `#[cfg(feature = "state-inmem")]` companion
/// file or conditionally gate just this function. Wave 0 landing
/// satisfies the plan's 4-tests requirement.
#[test]
#[ignore = "55-W3"]
fn state_inmem_build_skips_rematerialization() {
    unimplemented!("Wave 3 — state-inmem skips rematerialization (Pitfall 7)");
}
