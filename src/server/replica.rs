//! Phase 27-01: Replica-subsystem helpers.
//!
//! This module hosts the shared Scope-filter utilities used by both
//! `OP_SNAPSHOT_FETCH` (27-01, implemented here + in `tcp.rs`) and
//! `OP_SUBSCRIBE` (27-02, coming next). Keeping the filter predicate and
//! metric definitions in one place means the "historical" (snapshot) and
//! "live" (subscribe) halves of the replica wire protocol stay in sync.
//!
//! Design (per `27-CONTEXT.md §code_context`):
//!   - `filter_base_snapshot` walks `BaseSnapshotState.entities` in-memory
//!     and returns a new `BaseSnapshotState` with the same `header`,
//!     `pipelines`, and `backfill_complete` but only the entities that
//!     match the Scope. No streaming reader, no delta merging.
//!   - `entity_matches_scope` is the per-entity predicate, factored out so
//!     27-02's push-path notify hook can reuse the exact same matching
//!     rules on a single `(stream, key)` pair.
//!   - `record_snapshot_bytes_sent` bumps the module-local atomic counter
//!     that backs `tally_replica_snapshot_bytes_sent_total`. The counter
//!     lives here so Phase 28 and later replica endpoints can add sibling
//!     counters next to it.
//!
//! The metric is registered (as a static atomic) but not yet scraped by
//! `/metrics` — wiring it into `http.rs` is deferred to 27-02 / Phase 28
//! when the full replica metric surface is rounded out. The counter is
//! still readable via `snapshot_bytes_sent_total()` for tests.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::server::protocol::Scope;
use crate::state::snapshot::BaseSnapshotState;

/// Phase 27-01 / 27-02: process-wide counter of bytes written as
/// `OP_SNAPSHOT_FETCH` payload-frame bodies. Exposed via
/// `snapshot_bytes_sent_total()` for assertions; `record_snapshot_bytes_sent`
/// is the public increment path.
static SNAPSHOT_BYTES_SENT: AtomicU64 = AtomicU64::new(0);

/// Increment `tally_replica_snapshot_bytes_sent_total` by `n`. Called once
/// per successful snapshot payload write in `tcp.rs::handle_snapshot_fetch`.
pub fn record_snapshot_bytes_sent(n: u64) {
    SNAPSHOT_BYTES_SENT.fetch_add(n, Ordering::Relaxed);
}

/// Read the current counter value. Exposed for tests and (eventually)
/// `/metrics`. Uses `Relaxed` because this counter is monotonic and we do
/// not need cross-field consistency.
pub fn snapshot_bytes_sent_total() -> u64 {
    SNAPSHOT_BYTES_SENT.load(Ordering::Relaxed)
}

/// Phase 27-01 / 27-02: per-entity Scope match predicate.
///
/// Returns `true` iff the entity described by `(entity_streams, entity_key)`
/// should be surfaced to a replica client that asked for `scope`. The rules
/// mirror `filter_base_snapshot` exactly so that 27-02 can reuse this on
/// the hot push path without re-implementing the logic:
///
/// 1. **Stream overlap (required):** `entity_streams ∩ scope.streams` must
///    be non-empty. If the entity has no state for any of the requested
///    streams, it is not delivered.
/// 2. **Explicit keys filter (optional):** if `scope.keys` is set, the
///    entity key must appear in that list (linear scan — 10 000 max by
///    `validate_scope`).
/// 3. **Prefix filter (optional):** if `scope.key_prefix` is set, the
///    entity key must start with that prefix.
///
/// `scope.keys` and `scope.key_prefix` are mutually exclusive (rejected
/// upstream by `validate_scope`); this predicate still handles both being
/// `None` (stream-overlap-only) for safety.
pub fn entity_matches_scope(entity_streams: &[&str], entity_key: &str, scope: &Scope) -> bool {
    let overlaps = entity_streams
        .iter()
        .any(|s| scope.streams.iter().any(|want| want == s));
    if !overlaps {
        return false;
    }
    if let Some(keys) = &scope.keys {
        if !keys.iter().any(|k| k == entity_key) {
            return false;
        }
    }
    if let Some(prefix) = &scope.key_prefix {
        if !entity_key.starts_with(prefix) {
            return false;
        }
    }
    true
}

/// Phase 27-01: filter `snap.entities` by `scope`, preserving header,
/// pipelines, and backfill markers verbatim.
///
/// The returned `BaseSnapshotState` is a fresh postcard-serializable
/// struct — the tests round-trip it through `save_base_snapshot` /
/// `load_snapshot_file` to prove structural stability.
///
/// Snapshot size is ≤100MB in v0 (`27-CONTEXT.md §specifics`); cloning
/// filtered entities is acceptable.
pub fn filter_base_snapshot(snap: &BaseSnapshotState, scope: &Scope) -> BaseSnapshotState {
    let entities: Vec<_> = snap
        .entities
        .iter()
        .filter(|(key, entity)| {
            let streams: Vec<&str> = entity.streams.iter().map(|(s, _)| s.as_str()).collect();
            entity_matches_scope(&streams, key, scope)
        })
        .cloned()
        .collect();
    BaseSnapshotState {
        header: snap.header.clone(),
        entities,
        pipelines: snap.pipelines.clone(),
        backfill_complete: snap.backfill_complete.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::snapshot::{
        load_snapshot_file, save_base_snapshot, SerializableEntityState,
        SerializableStreamEntityState, SnapshotFile, SnapshotHeader, SnapshotType,
    };

    fn stream_state() -> SerializableStreamEntityState {
        SerializableStreamEntityState {
            operators: vec![],
            last_event_at: None,
        }
    }

    fn entity(streams: &[&str]) -> SerializableEntityState {
        SerializableEntityState {
            streams: streams
                .iter()
                .map(|s| ((*s).to_string(), stream_state()))
                .collect(),
            static_features: vec![],
            table_rows: vec![],
        }
    }

    fn base_snapshot(entities: Vec<(String, SerializableEntityState)>) -> BaseSnapshotState {
        BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1,
            },
            entities,
            pipelines: vec![],
            backfill_complete: vec![],
        }
    }

    fn scope(streams: &[&str]) -> Scope {
        Scope {
            streams: streams.iter().map(|s| (*s).to_string()).collect(),
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        }
    }

    #[test]
    fn filter_streams_only_keeps_matching_entities() {
        let snap = base_snapshot(vec![
            ("k1".into(), entity(&["orders"])),
            ("k2".into(), entity(&["clicks"])),
            ("k3".into(), entity(&["orders", "clicks"])),
        ]);
        let out = filter_base_snapshot(&snap, &scope(&["orders"]));
        let keys: Vec<_> = out.entities.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["k1", "k3"]);
        assert_eq!(out.header.sequence, 1);
    }

    #[test]
    fn filter_by_keys_narrows_subset() {
        let snap = base_snapshot(vec![
            ("k1".into(), entity(&["orders"])),
            ("k2".into(), entity(&["orders"])),
            ("k3".into(), entity(&["orders"])),
        ]);
        let mut s = scope(&["orders"]);
        s.keys = Some(vec!["k1".into(), "k3".into()]);
        let out = filter_base_snapshot(&snap, &s);
        let keys: Vec<_> = out.entities.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["k1", "k3"]);
    }

    #[test]
    fn filter_by_key_prefix_narrows_subset() {
        let snap = base_snapshot(vec![
            ("user_1".into(), entity(&["orders"])),
            ("user_2".into(), entity(&["orders"])),
            ("bot_9".into(), entity(&["orders"])),
        ]);
        let mut s = scope(&["orders"]);
        s.key_prefix = Some("user_".into());
        let out = filter_base_snapshot(&snap, &s);
        let keys: Vec<_> = out.entities.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["user_1", "user_2"]);
    }

    #[test]
    fn filter_no_match_returns_empty_entities() {
        let snap = base_snapshot(vec![("k1".into(), entity(&["orders"]))]);
        let out = filter_base_snapshot(&snap, &scope(&["clicks"]));
        assert!(out.entities.is_empty());
        // Header + pipelines + backfill still preserved.
        assert_eq!(out.header.sequence, 1);
    }

    #[test]
    fn filtered_snapshot_roundtrips_through_postcard() {
        let snap = base_snapshot(vec![
            ("k1".into(), entity(&["orders"])),
            ("k2".into(), entity(&["clicks"])),
        ]);
        let out = filter_base_snapshot(&snap, &scope(&["orders"]));
        let bytes = save_base_snapshot(&out).expect("serialize");
        match load_snapshot_file(&bytes) {
            Some(SnapshotFile::Base(reloaded)) => {
                assert_eq!(reloaded.entities.len(), out.entities.len());
                assert_eq!(reloaded.entities[0].0, "k1");
            }
            other => panic!("expected Base snapshot, got {:?}", other.is_some()),
        }
    }

    #[test]
    fn entity_matches_scope_stream_overlap_required() {
        let s = scope(&["orders"]);
        assert!(entity_matches_scope(&["orders"], "k1", &s));
        assert!(entity_matches_scope(&["orders", "clicks"], "k1", &s));
        assert!(!entity_matches_scope(&["clicks"], "k1", &s));
        assert!(!entity_matches_scope(&[], "k1", &s));
    }

    #[test]
    fn entity_matches_scope_honors_keys_filter() {
        let mut s = scope(&["orders"]);
        s.keys = Some(vec!["k1".into()]);
        assert!(entity_matches_scope(&["orders"], "k1", &s));
        assert!(!entity_matches_scope(&["orders"], "k2", &s));
    }

    #[test]
    fn entity_matches_scope_honors_prefix_filter() {
        let mut s = scope(&["orders"]);
        s.key_prefix = Some("user_".into());
        assert!(entity_matches_scope(&["orders"], "user_1", &s));
        assert!(!entity_matches_scope(&["orders"], "bot_1", &s));
    }

    #[test]
    fn snapshot_bytes_sent_counter_monotonic() {
        let before = snapshot_bytes_sent_total();
        record_snapshot_bytes_sent(42);
        let after = snapshot_bytes_sent_total();
        assert!(after >= before + 42);
    }
}
