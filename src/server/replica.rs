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
//!     that backs `beava_replica_snapshot_bytes_sent_total`. The counter
//!     lives here so Phase 28 and later replica endpoints can add sibling
//!     counters next to it.
//!
//! The metric is registered (as a static atomic) but not yet scraped by
//! `/metrics` — wiring it into `http.rs` is deferred to 27-02 / Phase 28
//! when the full replica metric surface is rounded out. The counter is
//! still readable via `snapshot_bytes_sent_total()` for tests.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::server::protocol::Scope;
use crate::server::signals::SharedRegistry;
use crate::state::snapshot::BaseSnapshotState;

/// Phase 27-02: bounded mpsc capacity for per-subscriber notification
/// channels. When this buffer fills, `notify_subscribers` drops the
/// subscriber (Rule A1 in `27-CONTEXT.md §Backpressure`).
pub const SUBSCRIBER_CHANNEL_CAPACITY: usize = 10_000;

/// Phase 27-01 / 27-02: process-wide counter of bytes written as
/// `OP_SNAPSHOT_FETCH` payload-frame bodies. Exposed via
/// `snapshot_bytes_sent_total()` for assertions; `record_snapshot_bytes_sent`
/// is the public increment path.
static SNAPSHOT_BYTES_SENT: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Phase 27-02: Replica subscribe metrics (process-wide atomic counters).
//
// `beava_replica_subscriptions_active` is a gauge sourced from
// `SubscriberRegistry::active_count()` at scrape time (no separate atomic).
// The drop counter has two reason labels ("backpressure" / "disconnect");
// the per-stream events-pushed counter is kept in a `DashMap<String,u64>`
// keyed by stream name — cardinality is bounded by the registered stream
// set (reviewed acceptable per user direction #4).
// ---------------------------------------------------------------------------

static DROPPED_BACKPRESSURE: AtomicU64 = AtomicU64::new(0);
static DROPPED_DISCONNECT: AtomicU64 = AtomicU64::new(0);

/// Per-stream `beava_replica_events_pushed_total{stream}` counter.
/// Lock-free reads via DashMap; init-on-first-push via `entry(..).or_insert`.
/// Backed by a `OnceLock` because `DashMap::new` is not `const fn` on the
/// currently-pinned dashmap version.
static EVENTS_PUSHED_BY_STREAM: std::sync::OnceLock<DashMap<String, AtomicU64>> =
    std::sync::OnceLock::new();

/// Phase 35-01: per-stream `beava_replica_log_entries_sent_total{stream}`
/// counter. One increment per event frame written by `handle_log_fetch`.
/// Same DashMap layout as `EVENTS_PUSHED_BY_STREAM`.
static LOG_ENTRIES_SENT_BY_STREAM: std::sync::OnceLock<DashMap<String, AtomicU64>> =
    std::sync::OnceLock::new();

/// Phase 36-01: per-stream `beava_replica_events_ingested_total{stream}`
/// counter. Bumped once per event routed through `replica_ingest`
/// (i.e., every CDC event the replica-client loop applies locally). Kept
/// separate from the normal `events_total` metric so operators can see
/// replica-sourced traffic distinctly from locally-accepted traffic.
static REPLICA_EVENTS_INGESTED_BY_STREAM: std::sync::OnceLock<DashMap<String, AtomicU64>> =
    std::sync::OnceLock::new();

fn replica_events_ingested_map() -> &'static DashMap<String, AtomicU64> {
    REPLICA_EVENTS_INGESTED_BY_STREAM.get_or_init(DashMap::new)
}

/// Phase 36-01: bump the per-stream replica-events-ingested counter by 1.
pub fn bump_replica_events_ingested(stream: &str) {
    let m = replica_events_ingested_map();
    if let Some(existing) = m.get(stream) {
        existing.fetch_add(1, Ordering::Relaxed);
        return;
    }
    m.entry(stream.to_owned())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

/// Phase 36-01: read snapshot of `(stream, count)` pairs for the per-stream
/// replica-events-ingested counter. Exposed for tests + `/metrics` wiring.
pub fn replica_events_ingested_snapshot() -> Vec<(String, u64)> {
    replica_events_ingested_map()
        .iter()
        .map(|kv| (kv.key().clone(), kv.value().load(Ordering::Relaxed)))
        .collect()
}

fn log_entries_sent_map() -> &'static DashMap<String, AtomicU64> {
    LOG_ENTRIES_SENT_BY_STREAM.get_or_init(DashMap::new)
}

/// Phase 35-01: read snapshot of `(stream, count)` pairs for the per-stream
/// log-entries-sent counter. Exposed for tests + `/metrics` wiring.
pub fn log_entries_sent_snapshot() -> Vec<(String, u64)> {
    log_entries_sent_map()
        .iter()
        .map(|kv| (kv.key().clone(), kv.value().load(Ordering::Relaxed)))
        .collect()
}

/// Phase 35-01: bump the per-stream log-entries-sent counter by 1.
/// Called once per event frame written in `handle_log_fetch`.
pub fn bump_log_entries_sent(stream: &str) {
    let m = log_entries_sent_map();
    if let Some(existing) = m.get(stream) {
        existing.fetch_add(1, Ordering::Relaxed);
        return;
    }
    m.entry(stream.to_owned())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

fn events_pushed_map() -> &'static DashMap<String, AtomicU64> {
    EVENTS_PUSHED_BY_STREAM.get_or_init(DashMap::new)
}

/// Read snapshot of `(reason, count)` pairs for the drop counter.
/// Only the two valid reasons ever appear.
pub fn subscribers_dropped_snapshot() -> Vec<(&'static str, u64)> {
    vec![
        ("backpressure", DROPPED_BACKPRESSURE.load(Ordering::Relaxed)),
        ("disconnect", DROPPED_DISCONNECT.load(Ordering::Relaxed)),
    ]
}

/// Read snapshot of `(stream, count)` pairs for the per-stream
/// events-pushed counter. Stable iteration order is not guaranteed
/// (DashMap internal order); Prometheus text doesn't require ordering.
pub fn events_pushed_snapshot() -> Vec<(String, u64)> {
    events_pushed_map()
        .iter()
        .map(|kv| (kv.key().clone(), kv.value().load(Ordering::Relaxed)))
        .collect()
}

fn bump_events_pushed(stream: &str) {
    let m = events_pushed_map();
    if let Some(existing) = m.get(stream) {
        existing.fetch_add(1, Ordering::Relaxed);
        return;
    }
    m.entry(stream.to_owned())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

/// Increment `beava_replica_snapshot_bytes_sent_total` by `n`. Called once
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

// ===========================================================================
// Phase 27-02: SubscriberRegistry + ReplicaSession + live-event hook.
// ===========================================================================

/// Phase 27-02: one event delivered over an `OP_SUBSCRIBE` socket. Produced
/// by `notify_subscribers` on the ingest path, consumed by the per-connection
/// drain task that writes event frames.
#[derive(Debug, Clone)]
pub struct ReplicaEvent {
    pub timestamp: SystemTime,
    pub stream: String,
    pub key: String,
    /// Serialized event JSON bytes (what the client posted), reused by the
    /// drain task as the event-frame payload.
    pub payload: Vec<u8>,
}

/// Phase 27-02: per-subscriber registry entry. One per live `OP_SUBSCRIBE`
/// connection. Held inside `SubscriberRegistry::sessions`. Lock-free reads
/// via DashMap.
#[derive(Debug)]
pub struct ReplicaSession {
    pub scope: Scope,
    pub sender: mpsc::Sender<ReplicaEvent>,
    /// Most recent drop reason, if any. Currently unused by the hot path but
    /// kept for future `/debug` introspection without breaking the struct.
    pub last_err: Option<String>,
}

/// Phase 27-02: process-wide registry of live `OP_SUBSCRIBE` sessions.
///
/// Lock-free: `DashMap<conn_id, ReplicaSession>` gives per-shard mutexes and
/// a non-blocking `iter` the ingest hook can walk without a global rwlock.
/// `next_id` is a plain `AtomicU64` — ids never reuse.
///
/// The ingest hot path calls `notify_subscribers(&self, stream, key, payload, now)`
/// which iterates the sessions, runs `entity_matches_scope` per session, and
/// `try_send`s the event on a match. A full buffer (> 10_000 pending events)
/// drops the subscriber with `reason="backpressure"` and emits an
/// `operational/warning` signal (per `27-CONTEXT.md §Backpressure A1`).
/// A closed receiver (drain task exited) drops with `reason="disconnect"`.
pub struct SubscriberRegistry {
    sessions: DashMap<u64, ReplicaSession>,
    /// Fast-path counter mirroring `sessions.len()`. The push hot path calls
    /// `notify_subscribers` once per cascaded operator (~25× per event), so
    /// the default `DashMap::is_empty()` check — which walks all shards and
    /// takes a brief shard read-lock each call — becomes ~20% of worker CPU
    /// when the registry is empty. A `Relaxed` atomic load on this field is
    /// ~2 ns, and exactness doesn't matter: we only ever compare to 0 for a
    /// short-circuit. The authoritative count is still `sessions.len()`.
    active: AtomicUsize,
    next_id: AtomicU64,
    signals: SharedRegistry,
}

impl std::fmt::Debug for SubscriberRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriberRegistry")
            .field("active", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl SubscriberRegistry {
    pub fn new(signals: SharedRegistry) -> Self {
        Self {
            sessions: DashMap::new(),
            active: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            signals,
        }
    }

    /// Insert a new session and return its fresh `conn_id`. The caller owns
    /// the corresponding `mpsc::Receiver<ReplicaEvent>` and must run the
    /// drain task that pulls from it.
    pub fn register(&self, scope: Scope, sender: mpsc::Sender<ReplicaEvent>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.insert(
            id,
            ReplicaSession {
                scope,
                sender,
                last_err: None,
            },
        );
        self.active.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Number of currently-registered subscribers. Backs the
    /// `beava_replica_subscriptions_active` gauge at scrape time.
    pub fn active_count(&self) -> usize {
        self.sessions.len()
    }

    /// Remove `conn_id` from the registry and bump the drop counter under
    /// the appropriate reason label. Safe to call from any thread; a
    /// missing entry is a no-op (idempotent). `reason` must be one of
    /// `"backpressure"` or `"disconnect"` — any other value counts as
    /// disconnect and a warning log is emitted at trace level in future
    /// hardening.
    pub fn drop_subscriber(&self, conn_id: u64, reason: &'static str) {
        // Removing the entry also drops the registry-held `Sender`; if the
        // drain task is still running it will observe the Receiver close
        // (channel-closed) naturally.
        let removed = self.sessions.remove(&conn_id).is_some();
        if !removed {
            // Already removed by a concurrent path — don't double-count.
            return;
        }
        self.active.fetch_sub(1, Ordering::Relaxed);
        match reason {
            "backpressure" => {
                DROPPED_BACKPRESSURE.fetch_add(1, Ordering::Relaxed);
                crate::server::signals::emit_replica_drop_backpressure(&self.signals, conn_id);
            }
            _ => {
                DROPPED_DISCONNECT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Phase 27-02 ingest-hook entry point. Called from
    /// `PipelineEngine::push_internal` after a successful push (primary or
    /// cascade downstream). Non-blocking: uses `try_send` only. A full
    /// queue drops the subscriber; it never back-pressures the caller.
    ///
    /// `stream` is the stream this push landed on; `key` is the entity key;
    /// `payload` is the serialized event JSON bytes; `now` is the event's
    /// logical timestamp (caller-provided — normally `SystemTime::now()`).
    pub fn notify_subscribers(&self, stream: &str, key: &str, payload: &[u8], now: SystemTime) {
        // Hot path: ~25 calls per cascaded event. `DashMap::is_empty()` walks
        // all shards + takes a brief read-lock each call (~300 ns), whereas a
        // `Relaxed` load on a mirrored counter is ~2 ns. At 124k eps × 25 ops
        // × 10 workers this is the difference between ~20% CPU and ~0.
        if self.active.load(Ordering::Relaxed) == 0 {
            return;
        }
        let stream_arr = [stream];
        // Collect drops locally so we don't mutate the map mid-iter.
        let mut to_drop: Vec<(u64, &'static str)> = Vec::new();

        for entry in self.sessions.iter() {
            let conn_id = *entry.key();
            let session = entry.value();
            if !entity_matches_scope(&stream_arr, key, &session.scope) {
                continue;
            }
            let ev = ReplicaEvent {
                timestamp: now,
                stream: stream.to_owned(),
                key: key.to_owned(),
                payload: payload.to_vec(),
            };
            match session.sender.try_send(ev) {
                Ok(()) => {
                    bump_events_pushed(stream);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    to_drop.push((conn_id, "backpressure"));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    to_drop.push((conn_id, "disconnect"));
                }
            }
        }

        for (conn_id, reason) in to_drop {
            self.drop_subscriber(conn_id, reason);
        }
    }
}

/// Shared handle used by the TCP dispatcher and the pipeline engine.
pub type SharedSubscriberRegistry = Arc<SubscriberRegistry>;

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

    // -----------------------------------------------------------------
    // Phase 27-02: SubscriberRegistry unit tests.
    // -----------------------------------------------------------------

    fn empty_signals() -> SharedRegistry {
        crate::server::signals::SignalRegistry::new_default().into_shared()
    }

    #[test]
    fn register_insert_then_drop_changes_active_count() {
        let reg = SubscriberRegistry::new(empty_signals());
        assert_eq!(reg.active_count(), 0);
        let (tx, _rx) = mpsc::channel(4);
        let id = reg.register(scope(&["orders"]), tx);
        assert_eq!(reg.active_count(), 1);
        reg.drop_subscriber(id, "disconnect");
        assert_eq!(reg.active_count(), 0);
    }

    #[test]
    fn notify_scope_mismatch_skips_try_send() {
        let reg = SubscriberRegistry::new(empty_signals());
        let (tx, mut rx) = mpsc::channel(4);
        reg.register(scope(&["orders"]), tx);
        reg.notify_subscribers("clicks", "k1", b"{}", SystemTime::now());
        // Try to drain the receiver — must be empty because scope didn't match.
        assert!(rx.try_recv().is_err());
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn notify_scope_match_delivers_event() {
        let reg = SubscriberRegistry::new(empty_signals());
        let (tx, mut rx) = mpsc::channel(4);
        reg.register(scope(&["orders"]), tx);
        let now = SystemTime::now();
        reg.notify_subscribers("orders", "u1", b"{\"k\":1}", now);
        let ev = rx.try_recv().expect("one event queued");
        assert_eq!(ev.stream, "orders");
        assert_eq!(ev.key, "u1");
        assert_eq!(ev.payload, b"{\"k\":1}");
        assert_eq!(ev.timestamp, now);
    }

    #[test]
    fn notify_backpressure_drops_subscriber() {
        let reg = SubscriberRegistry::new(empty_signals());
        // Tiny channel so we can saturate it deterministically without
        // pumping 10_000 events through the hot path.
        let (tx, _rx_held) = mpsc::channel(2);
        let id = reg.register(scope(&["orders"]), tx);
        assert_eq!(reg.active_count(), 1);
        let drops_before = DROPPED_BACKPRESSURE.load(Ordering::Relaxed);
        // Fill the buffer (2) and then force a 3rd push — must drop.
        reg.notify_subscribers("orders", "u1", b"{}", SystemTime::now());
        reg.notify_subscribers("orders", "u1", b"{}", SystemTime::now());
        reg.notify_subscribers("orders", "u1", b"{}", SystemTime::now());
        assert_eq!(reg.active_count(), 0, "subscriber dropped on Full");
        let drops_after = DROPPED_BACKPRESSURE.load(Ordering::Relaxed);
        assert!(
            drops_after > drops_before,
            "backpressure counter must increment: before={} after={}",
            drops_before,
            drops_after
        );
        // Confirm the double-remove guard: dropping again is a no-op.
        reg.drop_subscriber(id, "backpressure");
    }

    #[test]
    fn notify_channel_closed_drops_as_disconnect() {
        let reg = SubscriberRegistry::new(empty_signals());
        let (tx, rx) = mpsc::channel(4);
        reg.register(scope(&["orders"]), tx);
        drop(rx); // simulate drain task exit
        let disc_before = DROPPED_DISCONNECT.load(Ordering::Relaxed);
        reg.notify_subscribers("orders", "u1", b"{}", SystemTime::now());
        assert_eq!(reg.active_count(), 0);
        let disc_after = DROPPED_DISCONNECT.load(Ordering::Relaxed);
        assert!(disc_after > disc_before);
    }

    #[test]
    fn events_pushed_snapshot_reflects_stream_label() {
        let reg = SubscriberRegistry::new(empty_signals());
        let (tx, mut rx) = mpsc::channel(4);
        reg.register(scope(&["orders"]), tx);
        // Find the baseline for "orders" — prior tests in the same binary
        // may have bumped this counter already.
        let base = events_pushed_snapshot()
            .into_iter()
            .find(|(s, _)| s == "orders")
            .map(|(_, n)| n)
            .unwrap_or(0);
        reg.notify_subscribers("orders", "u1", b"{}", SystemTime::now());
        let _ = rx.try_recv();
        let after = events_pushed_snapshot()
            .into_iter()
            .find(|(s, _)| s == "orders")
            .map(|(_, n)| n)
            .unwrap_or(0);
        assert!(after > base);
    }
}
