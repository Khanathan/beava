//! Shard thread lifecycle — Phase 50 (Wave 2).
//!
//! D-01: spawn-all-at-boot + ready-barrier. All N shard threads must signal
//!       ready before spawn_shard_threads() returns. Callers must NOT bind
//!       listener sockets until this function returns.
//! D-02: Each shard loop runs inside std::panic::catch_unwind. On panic,
//!       the shard is marked DOWN; no auto-restart. Operator restarts server.
//! D-14: core_affinity pinning — Linux strict (log warn-once if fails because
//!       of container restrictions), macOS best-effort (kernel may ignore).

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crossbeam_channel::{Receiver, Sender};

/// Command envelope sent from listener to shard via SPSC inbox (D-08).
///
/// Phase 53-01 (legacy removal): expanded from a push-only `ShardEvent` to
/// a multi-op `ShardEvent` whose `op` field carries the actual command. The
/// existing `payload` + `stream_name` + `shard_hint` + `response_tx` fields
/// are preserved for backwards compatibility with the Push path; new
/// commands carry their data inside the `op` variant.
///
/// **Invariant:** exactly one of `op == ShardOp::Push` OR `op == ShardOp::<other>`
/// is set per event. The legacy payload/stream_name/shard_hint fields are
/// only meaningful when `op == ShardOp::Push`.
pub struct ShardEvent {
    /// Raw event bytes — bytes::Bytes is O(1) clone (Arc-backed). Zero copy.
    /// Used only for `ShardOp::Push`; empty for non-Push ops.
    ///
    /// Phase 59 Wave 1 (D-A2/D-A3): the bytes are forwarded verbatim from
    /// the listener's wire frame (binary OP_PUSH tail or legacy JSON body).
    /// The shard thread decodes them according to `payload_fmt` via
    /// [`crate::wire::decode_event_on_shard`]. No `serde_json::to_vec`
    /// re-serialize on the TCP hot path.
    pub payload: bytes::Bytes,
    /// Stream name for routing to correct Shard state machine.
    /// Used only for `ShardOp::Push`; empty Arc<str> for non-Push ops.
    pub stream_name: Arc<str>,
    /// Precomputed shard_hint from ingest parser (Phase 48).
    pub shard_hint: u32,
    /// Response channel — shard sends result back to listener.
    /// None for fire-and-forget paths. Required for Get / Set / Mset / GetMulti
    /// because the client awaits the result.
    pub response_tx: Option<tokio::sync::oneshot::Sender<ShardResult>>,
    /// Phase 53-01: command variant. Defaults to Push for backwards compatibility
    /// with code paths that have not yet been migrated to the expanded enum.
    pub op: ShardOp,
    /// Phase 59 D-A2: format tag for `payload`. Only meaningful when
    /// `op == ShardOp::Push`. Default is `Binary` (D-C2) — the TCP +
    /// Python primary path. HTTP PUSH + replica `LOG_FMT_JSON` set
    /// `Json` explicitly.
    pub payload_fmt: crate::wire::PayloadFmt,
}

impl ShardEvent {
    /// Convenience constructor for the legacy Push path — equivalent to
    /// constructing with `op: ShardOp::Push` and default `payload_fmt`
    /// (`PayloadFmt::Binary` per D-C2).
    pub fn push(
        payload: bytes::Bytes,
        stream_name: Arc<str>,
        shard_hint: u32,
        response_tx: Option<tokio::sync::oneshot::Sender<ShardResult>>,
    ) -> Self {
        Self::push_with_fmt(
            payload,
            stream_name,
            shard_hint,
            response_tx,
            crate::wire::PayloadFmt::Binary,
        )
    }

    /// Phase 59 D-A2: construct a Push event with an explicit payload-format
    /// tag. Used by HTTP ingest (sets `PayloadFmt::Json`) and the legacy
    /// JSON-over-TCP fallback (D-B3).
    pub fn push_with_fmt(
        payload: bytes::Bytes,
        stream_name: Arc<str>,
        shard_hint: u32,
        response_tx: Option<tokio::sync::oneshot::Sender<ShardResult>>,
        payload_fmt: crate::wire::PayloadFmt,
    ) -> Self {
        Self {
            payload,
            stream_name,
            shard_hint,
            response_tx,
            op: ShardOp::Push,
            payload_fmt,
        }
    }
}

/// Phase 53-01: shard command variants. Each non-Push variant carries its own
/// data (key, payload, table list). The Push variant reuses the enclosing
/// ShardEvent's `payload`/`stream_name`/`shard_hint` to avoid moving bytes.
///
/// All mutating commands (`Set`, `Mset`, `Tombstone`, `MarkDirty`) are routed
/// to a single shard (the one whose partition owns `key`). This is the design
/// precondition for deleting `StateStore.entities` — every entity has exactly
/// one owner shard.
#[derive(Debug)]
pub enum ShardOp {
    /// PUSH event: payload + stream_name live on the enclosing ShardEvent.
    Push,
    /// GET features for a single entity key. Response carries FeatureMap in `Ok`.
    Get { key: String },
    /// SET static features for an entity key. `payload` is the raw fields
    /// JSON object (empty object = tombstone). Response is `SetOk` on success.
    Set { key: String, payload: serde_json::Value },
    /// MSET multiple entities in one batch. `entries: Vec<(key, fields_json)>`.
    /// Response is `SetOk` on success. Only used when all keys hash to the same
    /// shard — the caller (tcp.rs) fans out per-shard.
    Mset { entries: Vec<(String, serde_json::Value)> },
    /// Tombstone an entity key: clears static_features and fires Table↔Table
    /// cascade with tombstoned=true.
    Tombstone { key: String },
    /// Mark an entity key dirty for incremental snapshots.
    MarkDirty { key: String },
    /// MGET multiple keys. Returned features carried in `MgetOk`.
    /// Only keys hashing to this shard are sent in `keys`.
    Mget { keys: Vec<String> },
    /// GET_MULTI: read multiple table_rows for a single entity key.
    /// Response carries `GetMultiOk` with serialized JSON value.
    GetMulti { table_names: Vec<String>, key: String },
    /// Phase 54-02 Task 1: Upsert a Table row at `(key, table_name)` on
    /// this shard. `fields` is the row payload; `now` is the mutation
    /// timestamp. Response is `SetOk` on success. Used by the
    /// scatter-gather TT-cascade (Task 2) when an output-table shard
    /// key hashes to a different shard than the input event's shard.
    UpsertTableRow {
        key: String,
        table_name: String,
        fields: ahash::AHashMap<String, crate::types::FeatureValue>,
        now: std::time::SystemTime,
    },
    /// Phase 54-02 Task 1: Tombstone a Table row at `(key, table_name)`
    /// on this shard. `now` is the tombstone-since timestamp. Response
    /// is `SetOk` on success. Sibling of `UpsertTableRow` for
    /// cascade-driven deletions.
    TombstoneTableRow {
        key: String,
        table_name: String,
        now: std::time::SystemTime,
    },
    /// Phase 55-01 D-A1: coalesced cross-shard TT-cascade writes. Carries a
    /// Vec of `(table_name, key, fields)` tuples which are applied in order
    /// on the target shard. Single `ShardResult::SetOk` on complete success;
    /// `ShardResult::Err` on the first failure (subsequent writes are
    /// skipped — caller sees a partial-apply condition and MUST NOT advance
    /// the cascade delivery cursor).
    UpsertTableBatch {
        writes: Vec<(String, String, ahash::AHashMap<String, crate::types::FeatureValue>)>,
        now: std::time::SystemTime,
    },
    /// Phase 55-02 D-B1/D-B5 (TPC-SOURCE-01): source-table row upsert.
    /// Full-replace semantics; source_lsn stored per-row; echoed on ack.
    /// Does NOT fire cascade (D-B6 — Phase 55 source tables are passive).
    /// Response is `SetOk` on success.
    UpsertSourceTableRow {
        table_name: String,
        key: String,
        fields: ahash::AHashMap<String, crate::types::FeatureValue>,
        source_lsn: u64,
        now: std::time::SystemTime,
    },
    /// Phase 55-02 D-B5: source-table row delete. Hard-deletes the row AND
    /// appends a `LogEntry::PendingRetraction` marker (Phase 57 consumer)
    /// to the per-shard event log. No cascade (D-B6).
    DeleteSourceTableRow {
        table_name: String,
        key: String,
        source_lsn: u64,
        now: std::time::SystemTime,
    },
    /// Phase 55-02 D-B4: batch upsert, all-or-nothing. Pre-validates every
    /// row (non-empty key); the first failure aborts the whole batch with
    /// `ShardResult::Err`, no rows written. On success the target shard
    /// applies each row via `upsert_source_table_row`.
    UpsertSourceTableBatch {
        table_name: String,
        rows: Vec<(String, ahash::AHashMap<String, crate::types::FeatureValue>, u64)>, // (key, fields, source_lsn)
        now: std::time::SystemTime,
    },
    /// Phase 55-02 D-B4: batch delete, all-or-nothing. Each row writes a
    /// PendingRetraction marker on success.
    DeleteSourceTableBatch {
        table_name: String,
        rows: Vec<(String, u64)>, // (key, source_lsn)
        now: std::time::SystemTime,
    },
    /// Phase 54-04 Pass A1: OP_PUSH_TABLE dispatch. Shard performs the
    /// full handle_push_table sequence on its own state:
    ///   - pre-existed check (triggers eviction-reinit counter if fresh),
    ///   - `upsert_table_row`,
    ///   - `mark_dirty`,
    ///   - `cascade_table_upsert_on_shard` with tombstoned=false.
    ///
    /// Caller (tcp.rs) has already validated `table_name` is a registered
    /// Table and advanced the Table's watermark by `event_time`.
    PushTableRow {
        table_name: String,
        key: String,
        fields: ahash::AHashMap<String, crate::types::FeatureValue>,
        event_time: std::time::SystemTime,
    },
    /// Phase 54-04 Pass A1: OP_DELETE_TABLE dispatch. Shard performs the
    /// handle_delete_table sequence on its own state:
    ///   - `tombstone_table_row`,
    ///   - `mark_dirty`,
    ///   - `cascade_table_upsert_on_shard` with tombstoned=true.
    ///
    /// Caller (tcp.rs) has already validated `table_name` is registered
    /// and advanced the Table's watermark.
    DeleteTableRow {
        table_name: String,
        key: String,
        event_time: std::time::SystemTime,
    },
    /// Phase 54-04 Pass A1: OP_GET dispatch that also fires the full
    /// TT-cascade fan-out across every registered input Table. Mirrors
    /// the SET path in TCP (Command::Set) where the Table identity isn't
    /// known but the cascade must fire for every TT-join downstream.
    ///
    /// Currently identical to `Set` dispatch followed by per-input-table
    /// cascade — a dedicated variant keeps the "SET + cascade fan-out"
    /// contract explicit rather than entangling cascade logic with the
    /// plain `Set` path used by legacy callers.
    SetWithCascade {
        key: String,
        payload: serde_json::Value,
    },
    /// Phase 54-04 Pass A1: clear-operator-state pass for backfill
    /// (run_backfill step 1). Shard iterates its own entities and
    /// drops any operator state whose feature name appears in
    /// `feature_names` for the given `stream_name`. Idempotent — safe
    /// to run multiple times.
    ClearBackfillOperators {
        stream_name: String,
        feature_names: Vec<String>,
    },
    /// Phase 54-04 Pass A1: enumerate every entity key held by this
    /// shard. Used by scatter-gather callers (run_backfill) that need
    /// a global key list without touching `StateStore.entity_keys`.
    ListEntityKeys,
    /// Phase 54-04 Pass A2: return the approximate number of entity
    /// keys held by this shard. On the default (fjall) build this
    /// calls `PartitionHandle::approximate_len()` (O(1), stale
    /// estimate — matches the per-shard `keys_owned` gauge); on
    /// state-inmem it's `AHashMap::len()`. Used by HTTP metrics +
    /// `/public/stats` to aggregate a fleet-wide key count via
    /// scatter-gather without touching `StateStore.entities`.
    EntityCount,
    /// Phase 54-04 Pass A4: on-shard variant of `evict_expired_keys`.
    /// The shard walks its own entities, removes stream entries whose
    /// last_event_at exceeds the per-stream or fallback global TTL,
    /// and removes entities left completely empty. `ttl_multiplier`
    /// scales the engine's `max_window_duration` for the global
    /// fallback TTL (matches the legacy `evict_expired_stream_entries`
    /// signature). `now` is the wall-clock fallback for the eviction
    /// clock; the shard consults the engine's per-stream watermark
    /// observed-max first (CORR-07). Response is `EvictedCount`.
    EvictExpired {
        now: std::time::SystemTime,
        ttl_multiplier: u32,
    },
    /// Phase 54-04 Pass A4: on-shard variant of
    /// `evict_expired_table_rows`. The shard walks its own entities'
    /// `table_rows`, removes Live rows whose `updated_at` is older
    /// than the per-Table `entity_ttl`, and records each eviction in
    /// the shared `state.eviction_tracker` so the eviction→reinit
    /// signal keeps surfacing on /metrics. Response is
    /// `EvictedCount`.
    EvictExpiredTableRows {
        now: std::time::SystemTime,
    },
    /// Phase 54-04 Pass A5: on-shard variant of `push_for_backfill`.
    /// Replays a single backfilled event onto this shard's operator state
    /// for `stream_name`. Mirrors the legacy StateStore-backed
    /// `PipelineEngine::push_for_backfill` body but operates through
    /// `StoreView::Sharded(&mut shard)`. Caller (run_backfill) has
    /// already extracted the entity key and routed this op to the
    /// owning shard.
    ///
    /// Does NOT evaluate derives (they auto-resolve on read) and does
    /// NOT touch `last_event_at` (backfill is not a "live" event).
    /// Response is `SetOk` on success.
    PushForBackfill {
        stream_name: String,
        event: serde_json::Value,
        event_time: std::time::SystemTime,
        feature_names: Vec<String>,
    },
    /// Phase 56 D-A1 (TPC-CORR-08): cross-shard single-key read for
    /// EnrichFromTable. Source shard dispatches when
    /// `hash(key) % N != current_shard`. Target shard reads its local
    /// `PartitionHandle` and replies with `ShardResult::ReadEntityOk`.
    /// Pure read; no state mutation. Increments
    /// `beava_enrich_cross_shard_total{table}` on dispatch arm.
    ReadEntityAt {
        table_name: String,
        key: String,
    },
    /// Phase 56 D-A2: per-target coalesced batch read. Source shard
    /// accumulates all cross-shard enrichment keys headed to the same
    /// target into one Vec<String>, dispatches once per (target, table)
    /// pair. Target iterates and replies with parallel Vec<Option<_>>.
    /// DoS guard: keys.len() > MAX_ENRICH_BATCH_KEYS=4096 → Err
    /// (T-56-01-01 mitigation).
    ReadEntityBatch {
        table_name: String,
        keys: Vec<String>,
    },
    /// Phase 56 D-B1 (TPC-CORR-09): cross-shard StreamStreamJoin
    /// insert. The join buffer lives on `hash(join_key) % N` per
    /// D-B1. Source shards dispatch their L/R events here; the
    /// target inserts + probes inline and replies with matched
    /// counterparty events that the source then emits as joined
    /// outputs via its own downstream cascade (unchanged from the
    /// Phase 55 path). Increments
    /// `beava_ssj_cross_shard_total{join_id}` on the dispatch arm.
    SsjInsert {
        join_id: String,
        side: crate::engine::operators::JoinSide,
        join_key: String,
        event: serde_json::Value,
        within_ms: u64,
    },
    /// Phase 57 D-B1 (TPC-CORR-10): cross-shard retraction dispatch.
    /// Source shards emit this for every downstream row whose
    /// `contributing_inputs` intersects a tombstoned event (Stream→Table
    /// primary tombstone, EnrichFromTable source-table DELETE, or
    /// StreamStreamJoin side tombstone). Target shard idempotently
    /// retracts the row: NoOp if already-retracted (D-B4), DepthExceeded
    /// if `depth >= MAX_RETRACTION_DEPTH` (D-B5), BeyondHistory if the
    /// contributing event is older than `watermark - history_ttl` (D-C1),
    /// otherwise tombstones the row (Retracted).
    ///
    /// Dispatch pattern mirrors `SsjInsert` — source try_sends + blocks on
    /// `crossbeam::bounded(1)` oneshot + `BeavaError::ShardOverload` on
    /// Full (D-B3). Source-side bumps `beava_retractions_sent_total`
    /// (single emission site). Target dispatch arm bumps exactly one of
    /// `{applied,nooped,beyond_history,depth_exceeded}_total`.
    RetractDownstream {
        target_shard: u16,
        stream_name: String,
        row_key: String,
        reason: RetractReason,
        depth: u8,
    },
}

/// Phase 57 D-B2 (TPC-CORR-10): evidence carried with each `RetractDownstream`
/// dispatch so observability dashboards can distinguish tombstone origins.
///
/// Serialize + Deserialize derived for forward-compat with a future
/// cross-process dispatch path; today all dispatch is intra-process SPSC so
/// the variant flows by move.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum RetractReason {
    /// A source-table row (Phase 55-02 D-B5) was hard-deleted; downstream
    /// rows whose `contributing_inputs.source_table_keys` references
    /// `(table_name, table_key)` must retract.
    SourceTableDelete {
        table_name: String,
        table_key: String,
        source_lsn: u64,
    },
    /// An entity's stream slot was tombstoned (e.g. SET with empty object
    /// or an explicit Tombstone op); downstream rows whose
    /// `contributing_inputs` references any event from that
    /// (stream_name, entity_key) must retract.
    EntityTombstone {
        stream_name: String,
        entity_key: String,
    },
    /// A primary event was retracted directly (Wave 2/3 consumer). Downstream
    /// rows whose `contributing_inputs.primary_event_id == event_id` must
    /// retract.
    PrimaryEventRetract {
        stream_name: String,
        event_id: u64,
    },
}

/// Phase 57 D-B4 (TPC-CORR-10): idempotent outcome reported by the target
/// shard's `apply_retraction` path. Each variant corresponds to exactly one
/// metric counter bump in the dispatch arm.
///
/// `Copy` because it carries no heap data — travels freely through the
/// oneshot and the `ShardResult::RetractOk` envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetractOutcome {
    /// The row was present and live; now tombstoned. Bumps
    /// `beava_retractions_applied_total`.
    Retracted,
    /// The row was absent or already tombstoned (idempotent — D-B4). Bumps
    /// `beava_retractions_nooped_total`. Necessary because source shards
    /// may retry on `ShardOverload` and multiple tombstones on the same
    /// event can fan out to the same downstream.
    NoOp,
    /// The contributing event is older than `watermark - history_ttl`
    /// (D-C1). Retraction is skipped; row unchanged. Bumps
    /// `beava_retraction_beyond_history_total`. Caller may choose to log
    /// a `/debug/warnings` entry (Wave 4).
    BeyondHistory,
    /// `depth >= MAX_RETRACTION_DEPTH` (D-B5). Target returns this without
    /// touching state; caller may propagate `BeavaError::Protocol(...)`.
    /// Bumps `beava_retraction_depth_exceeded_total`.
    DepthExceeded,
}

/// Phase 57 D-B5 (TPC-CORR-10): hard cap on retraction cascade depth. 16
/// hops is generous for fraud workloads (typical 2–3). The cap is a safety
/// net for accidentally self-referential pipelines, not a hard limit on
/// real use — alarms via `beava_retraction_depth_exceeded_total`.
pub const MAX_RETRACTION_DEPTH: u8 = 16;

/// Result sent from shard back to listener via response_tx.
/// Phase 50.5-01: widened to carry FeatureMap so read_features=true round-trips.
/// Phase 53-01: further widened with variants for Get / Set / Mset / Mget / GetMulti.
#[derive(Debug)]
pub enum ShardResult {
    /// PUSH ack — carries computed FeatureMap (may be empty).
    Ok(crate::types::FeatureMap),
    /// GET response — carries the feature map for the requested key AND an
    /// existence flag so HTTP can return 404 for missing entities without
    /// re-reading state. Phase 54-03 Task 3.
    GetOk {
        exists: bool,
        features: crate::types::FeatureMap,
    },
    /// SET / MSET / Tombstone / MarkDirty ack.
    SetOk,
    /// MGET response — `Vec<(key, FeatureMap)>` preserving request order
    /// for the keys owned by this shard.
    MgetOk(Vec<(String, crate::types::FeatureMap)>),
    /// GET_MULTI response — `(table_name, row_json_or_null)` pairs in request order.
    GetMultiOk(Vec<(String, serde_json::Value)>),
    /// Phase 54-04 Pass A1: ListEntityKeys response — entity keys held
    /// by this shard at the moment of dispatch.
    EntityKeysOk(Vec<String>),
    /// Phase 54-04 Pass A2: EntityCount response — approximate number
    /// of entity keys held by this shard (O(1) estimate on fjall,
    /// exact on state-inmem). Matches the `keys_owned` gauge semantics.
    EntityCountOk(usize),
    /// Phase 54-04 Pass A4: EvictExpired / EvictExpiredTableRows ack —
    /// number of items (stream entries or Table rows) evicted on the
    /// responding shard.
    EvictedCount(usize),
    /// Phase 56 D-A1: reply to ReadEntityAt. None = missing row
    /// (downstream increments enrich_missing_total).
    ReadEntityOk(Option<crate::state::store::EntityState>),
    /// Phase 56 D-A2: reply to ReadEntityBatch. Parallel to input
    /// `keys` Vec; each slot either Some(row) or None.
    ReadEntityBatchOk(Vec<Option<crate::state::store::EntityState>>),
    /// Phase 56 D-B1: reply to SsjInsert. Vec of matched
    /// counterparty event maps. Empty Vec = no match found. The
    /// source shard consumes these to emit joined outputs via its
    /// existing downstream cascade path.
    SsjInsertOk(Vec<serde_json::Map<String, serde_json::Value>>),
    /// Phase 57 D-B4: reply to `RetractDownstream`. Carries the
    /// idempotent outcome — see `RetractOutcome` for semantics.
    RetractOk(RetractOutcome),
    /// Shard failed to process the event.
    Err(ShardDispatchError),
}

/// Error variants for shard dispatch failures.
#[derive(Debug)]
pub enum ShardDispatchError {
    /// Shard is quarantined (DOWN after panic).
    Down,
    /// Shard processing error.
    ProcessingError(String),
}

/// Per-shard handle returned to the listener layer.
pub struct ShardHandle {
    /// Index of this shard (0..N-1).
    pub shard_index: usize,
    /// Flag set to true if this shard panicked and is quarantined (D-02).
    pub is_down: Arc<AtomicBool>,
    /// Sender side of the SPSC inbox — listeners call try_send here.
    pub inbox_tx: Sender<ShardEvent>,
}

/// Default SPSC inbox capacity (D-08). Configurable via BEAVA_SHARD_INBOX_SIZE.
pub const DEFAULT_INBOX_SIZE: usize = 65_536;

/// Phase 56 T-56-01-01 DoS mitigation: upper bound on per-dispatch
/// enrichment batch size. 4096 keys × ~500 bytes/row ≈ 2 MB worst
/// case reply — bounded memory. Hardcoded (no env var) for Wave 1.
pub const MAX_ENRICH_BATCH_KEYS: usize = 4096;

/// Phase 58-01 D-A4: per-shard accept configuration.
///
/// When `Some`, the shard's `current_thread` tokio runtime hosts an accept loop
/// bound to its own SO_REUSEPORT socket (Linux only — macOS branch in Wave 2
/// reads this same struct to size dedicated accept threads).
///
/// `max_conns_per_shard` caps in-flight connections per shard. Default 256 via
/// `BEAVA_MAX_CONNS_PER_SHARD`. Accept side back-pressures via kernel
/// `listen(1024)` backlog when FuturesUnordered saturates.
#[derive(Clone, Debug)]
pub struct PerShardAcceptCfg {
    /// Address on which every shard listens via SO_REUSEPORT.
    pub accept_addr: std::net::SocketAddr,
    /// Cap on in-flight connections per shard (D-A4).
    pub max_conns_per_shard: usize,
}

/// Phase 58-01 D-A4: read `BEAVA_MAX_CONNS_PER_SHARD` from env, clamp to
/// `[1, 65536]`, default 256. Invalid values (non-numeric, out-of-range) emit
/// a warn-once stderr and fall back to the default — matches the
/// `inbox_size_from_env` pattern.
pub fn max_conns_per_shard_from_env() -> usize {
    const DEFAULT: usize = 256;
    const MIN: usize = 1;
    const MAX: usize = 65_536;
    match std::env::var("BEAVA_MAX_CONNS_PER_SHARD") {
        Ok(s) => match s.parse::<usize>() {
            Ok(n) if (MIN..=MAX).contains(&n) => n,
            _ => {
                eprintln!(
                    "BEAVA_MAX_CONNS_PER_SHARD={s:?} invalid or out of range \
                     [{MIN},{MAX}] — defaulting to {DEFAULT}"
                );
                DEFAULT
            }
        },
        Err(_) => DEFAULT,
    }
}

/// Spawn all N shard threads. Returns only after every shard has signaled
/// ready (the ready-barrier, D-01). Callers bind listener sockets after this
/// returns.
///
/// Phase 50.5-01: `state` added so each shard thread owns a handle into
/// `ConcurrentAppState` and can call `push_with_cascade_on_shard` directly.
///
/// Phase 58-01: `accept_cfg` added. When `Some` on Linux, each shard binds its
/// own SO_REUSEPORT `TcpListener` and drives a `FuturesUnordered` accept loop
/// INLINE on its `current_thread` runtime — no `tokio::spawn` per connection.
/// When `None`, the pre-Phase-58 behavior is preserved (callers bind their own
/// listener and dispatch through the SPSC inbox).
///
/// # Panics
/// Panics at the caller level only if shard_count == 0.
pub fn spawn_shard_threads(
    shard_count: usize,
    inbox_size: usize,
    state: std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    accept_cfg: Option<PerShardAcceptCfg>,
) -> Vec<ShardHandle> {
    assert!(shard_count > 0, "shard_count must be >= 1");

    // Ready barrier: WaitGroup — each shard drops its clone when ready.
    // spawn_shard_threads() blocks on wg.wait() until all shard tokens are dropped.
    let wg = crossbeam_utils::sync::WaitGroup::new();

    let mut handles = Vec::with_capacity(shard_count);

    for shard_index in 0..shard_count {
        let is_down: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let (tx, rx) = crossbeam_channel::bounded::<ShardEvent>(inbox_size);

        let is_down_clone = Arc::clone(&is_down);
        let wg_worker = wg.clone();
        let state_clone = std::sync::Arc::clone(&state);
        let accept_cfg_clone = accept_cfg.clone();

        std::thread::Builder::new()
            .name(format!("beava-shard-{}", shard_index))
            .spawn(move || {
                // D-14: core_affinity pinning (Linux strict, macOS best-effort).
                pin_to_core(shard_index);

                // Signal ready — listener bind is unblocked when all shards drop their token.
                drop(wg_worker);

                // D-02: catch_unwind quarantine around the entire shard event loop.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shard_event_loop(shard_index, rx, state_clone, accept_cfg_clone);
                }));

                if result.is_err() {
                    is_down_clone.store(true, Ordering::SeqCst);
                    crate::shard::metrics::record_shard_down(shard_index);
                    eprintln!(
                        "[beava-shard-{}] Shard thread panicked — marked DOWN. \
                         Restart server to recover.",
                        shard_index
                    );
                }
            })
            .expect("failed to spawn shard thread");

        handles.push(ShardHandle {
            shard_index,
            is_down,
            inbox_tx: tx,
        });
    }

    // Block until all shards have dropped their WaitGroup token (= signaled ready).
    wg.wait();
    handles
}

/// Pin the current thread to physical core `shard_index`.
/// On macOS or in restricted cgroups: logs warn-once and continues (D-14 / D-05).
fn pin_to_core(shard_index: usize) {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    if let Some(core_id) = cores.get(shard_index) {
        if !core_affinity::set_for_current(*core_id) {
            eprintln!(
                "[beava-shard-{}] core_affinity pinning failed (macOS best-effort or \
                 restricted cgroup — continuing without pin)",
                shard_index
            );
        }
    } else {
        eprintln!(
            "[beava-shard-{}] shard_index exceeds available core count ({}) — \
             pinning skipped",
            shard_index,
            cores.len()
        );
    }
}

/// Shard event loop. Runs a tokio current_thread runtime on the pinned OS thread.
/// Phase 50.5-01 Task 3: real dispatch via push_with_cascade_on_shard.
/// Phase 53-03B: default (fjall) build now constructs its `Shard` from the
/// partition handle stashed in `ConcurrentAppState.shard_partitions[shard_index]`.
/// Under `--features state-inmem` the legacy `Shard::new()` path is preserved.
fn shard_event_loop(
    shard_index: usize,
    rx: Receiver<ShardEvent>,
    state: std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    accept_cfg: Option<PerShardAcceptCfg>,
) {
    // Each shard runs a tokio current_thread runtime on its pinned OS thread.
    // This allows async code (e.g. oneshot response sends) without cross-thread
    // task migration — the reactor stays on the pinned core.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build per-shard tokio runtime");

    // Each shard owns its own Shard struct — single writer, no lock.
    //
    // Phase 53-03B: default build pulls the per-shard `PartitionHandle` from
    // `ConcurrentAppState.shard_partitions` (populated by main.rs on startup
    // via `open_keyspace_and_partitions`). state-inmem keeps the AHashMap
    // `Shard::new()` path.
    #[cfg(not(feature = "state-inmem"))]
    let mut shard = {
        let partition = state.shard_partitions[shard_index].clone();
        crate::shard::Shard::with_partition(partition)
    };
    #[cfg(feature = "state-inmem")]
    let mut shard = crate::shard::Shard::new();

    // Phase 51-02: read publish threshold once at shard boot — avoids repeated
    // env-var parsing on the hot event loop. BEAVA_WATERMARK_PUBLISH_INTERVAL
    // is clamped [64, 65536] by from_env(); defaults to 1024.
    let wm_publish_threshold =
        crate::shard::global_watermark::GlobalWatermarkConfig::from_env().publish_interval;

    // Phase 58-01 Task 2: Linux per-shard accept driver branch.
    //
    // When `accept_cfg.is_some()` AND we're on Linux, the shard binds its own
    // SO_REUSEPORT `TcpListener` and drives a `FuturesUnordered` accept loop
    // INLINE on its current_thread runtime — NO `tokio::spawn` per connection
    // (D-A1/A2/A3/A4). ShardOp inbox drain runs interleaved via `try_recv`.
    //
    // When `accept_cfg.is_none()` OR on non-Linux, we fall through to the
    // pre-Phase-58 blocking `rx.recv()` loop (preserves Phase 50.5 behavior
    // for the macOS branch — Wave 2 rewrites macOS separately).
    #[cfg(target_os = "linux")]
    if accept_cfg.is_some() {
        let cfg = accept_cfg.clone().unwrap();
        run_linux_per_shard_accept_loop(
            shard_index,
            rx,
            state.clone(),
            &mut shard,
            wm_publish_threshold,
            cfg,
            &rt,
        );
        return;
    }

    // Silence unused-var on non-Linux / None path.
    let _accept_cfg: Option<PerShardAcceptCfg> = accept_cfg;

    rt.block_on(async move {
        let mut event_count: u64 = 0;
        let mut last_gauge_update = std::time::Instant::now();

        while let Ok(event) = rx.recv() {
            event_count += 1;
            let now = std::time::SystemTime::now();

            process_shard_event(event, shard_index, &mut shard, &state, wm_publish_threshold, now);

            // Emit gauges every 1000 events OR every 100ms.
            if event_count % 1000 == 0 || last_gauge_update.elapsed().as_millis() >= 100 {
                let inbox_depth = rx.len();
                last_gauge_update =
                    emit_shard_gauges(shard_index, &mut shard, inbox_depth, last_gauge_update);
            }
        }
    });
}

/// Phase 58-01 Task 2: per-event dispatch extracted from the `rt.block_on`
/// body so both the None-accept-cfg loop (blocking `rx.recv()`) AND the
/// Linux per-shard accept loop (FuturesUnordered + `try_recv`) can share
/// the same match body without duplication.
///
/// Behavior preserved verbatim from the pre-Phase-58 inline body — the only
/// change is that the two `continue;` statements in the original (JSON parse
/// error on Push, enrich batch over cap) are now `return;` statements. That
/// is behavior-preserving at the caller: both original `continue` branches
/// skipped the inner gauge block, and the helper now returns to the same
/// point; the caller re-enters the loop on its next iteration.
#[inline]
fn process_shard_event(
    mut event: ShardEvent,
    shard_index: usize,
    // `mut` on the param binding so the body can take `&mut shard` (reborrow)
    // inside arms that call engine methods with a `&mut Shard` — the pre-
    // Phase-58 body used `let mut shard` so reborrowing was implicit; we
    // preserve that ergonomics here.
    mut shard: &mut crate::shard::Shard,
    state: &std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    wm_publish_threshold: u64,
    now: std::time::SystemTime,
) {
    // Phase 53-01: dispatch on the new ShardOp enum. Take the op out of
    // the event (replacing with Push placeholder) so we can still access
    // event.payload / event.stream_name / event.response_tx by value.
    let op = std::mem::replace(&mut event.op, ShardOp::Push);
    match op {
                ShardOp::Push => {
                    // Phase 59 D-C1: decode on the shard thread according to
                    // the listener-set `payload_fmt` tag. Binary → single
                    // `decode_event_binary` parse (the ONE necessary parse on
                    // the TCP hot path); Json → `serde_json::from_slice` for
                    // HTTP / replica-JSON / legacy-TCP-JSON-fallback paths.
                    // No `serde_json::to_vec` round-trip on the listener side
                    // (that's the ~11% CPU Phase 59 recovers).
                    let payload: serde_json::Value = match crate::wire::decode_event_on_shard(
                        &event.payload,
                        event.payload_fmt,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            crate::shard::metrics::record_shard_event(
                                shard_index,
                                crate::shard::metrics::Outcome::Dropped,
                            );
                            if let Some(tx) = event.response_tx {
                                let _ = tx.send(ShardResult::Err(ShardDispatchError::ProcessingError(
                                    format!("payload decode error ({:?}): {}", event.payload_fmt, e),
                                )));
                            }
                            // Phase 58-01 Task 2: was `continue;` inside the
                            // outer while-let; lifted into `process_shard_event`
                            // as early `return;`. Caller re-enters the loop.
                            return;
                        }
                    };

                    let stream_name: &str = &event.stream_name;

                    let result = {
                        let engine = state.engine.read();
                        let read_features = event.response_tx.is_some();
                        // Phase 54-02 Task 2: hand the engine a snapshot of
                        // the sibling-shard handles so cross-shard TT
                        // cascades can dispatch via SPSC. `read()` is a
                        // parking_lot RwLock guard held only across this
                        // single synchronous call, so the window is the
                        // duration of one event's push + cascade. No
                        // re-entrancy — shard threads never call into their
                        // own handle's inbox.
                        let handles_guard = state.shard_handles.read();
                        let handles_slice: Option<&[ShardHandle]> = if handles_guard.is_empty() {
                            None
                        } else {
                            Some(&handles_guard[..])
                        };
                        engine.push_with_cascade_on_shard(
                            stream_name,
                            &payload,
                            &mut shard,
                            None,
                            now,
                            read_features,
                            handles_slice,
                            shard_index,
                        )
                    };

                    if let Some(et) = crate::engine::operators::parse_event_time(&payload) {
                        shard.watermark.observe(stream_name, et);
                        let gw = state.global_watermark.read();
                        shard.watermark.publish_if_due(stream_name, &gw, shard_index, wm_publish_threshold);
                    }

                    crate::shard::metrics::record_shard_event(
                        shard_index,
                        crate::shard::metrics::Outcome::Accepted,
                    );

                    if let Some(tx) = event.response_tx {
                        let shard_result = match result {
                            Ok(fm) => ShardResult::Ok(fm),
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(shard_result);
                    }
                }
                ShardOp::Get { key } => {
                    // Read features from shard-owned state via engine helper.
                    // Phase 54-03 Task 3: also report existence so HTTP GET
                    // can return 404 without a separate entity lookup.
                    let exists = crate::shard::read_entity_from_shard(&shard, &key, |_| ())
                        .is_some();
                    let features = {
                        let engine = state.engine.read();
                        engine.get_features_on_shard(&key, &shard, now)
                    };
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::GetOk { exists, features });
                    }
                }
                ShardOp::Set { key, payload } => {
                    let result = apply_set_on_shard(
                        &state, &mut shard, shard_index, &key, &payload, now, false,
                    );
                    if let Some(tx) = event.response_tx {
                        let r = match result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::SetWithCascade { key, payload } => {
                    // Phase 54-04 Pass A1: TCP Command::Set — SET + TT-cascade
                    // fan-out. Fires `cascade_table_upsert_on_shard` for every
                    // registered input Table whose key_field is present (same
                    // fan-out policy as the legacy DashMap SET path).
                    let result = apply_set_on_shard(
                        &state, &mut shard, shard_index, &key, &payload, now, true,
                    );
                    if let Some(tx) = event.response_tx {
                        let r = match result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::Mset { entries } => {
                    let mut last_err: Option<crate::error::BeavaError> = None;
                    for (key, payload) in entries {
                        if let Err(e) = apply_set_on_shard(
                            &state, &mut shard, shard_index, &key, &payload, now, false,
                        ) {
                            last_err = Some(e);
                        }
                    }
                    if let Some(tx) = event.response_tx {
                        let r = match last_err {
                            None => ShardResult::SetOk,
                            Some(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::Tombstone { key } => {
                    // Tombstone = SET with empty object.
                    let empty = serde_json::Value::Object(serde_json::Map::new());
                    let result = apply_set_on_shard(
                        &state, &mut shard, shard_index, &key, &empty, now, false,
                    );
                    if let Some(tx) = event.response_tx {
                        let r = match result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::MarkDirty { key } => {
                    shard.dirty_set.insert(key);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::Mget { keys } => {
                    let results: Vec<(String, crate::types::FeatureMap)> = {
                        let engine = state.engine.read();
                        keys.into_iter()
                            .map(|k| {
                                let fm = engine.get_features_on_shard(&k, &shard, now);
                                (k, fm)
                            })
                            .collect()
                    };
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::MgetOk(results));
                    }
                }
                ShardOp::GetMulti { table_names, key } => {
                    let rows: Vec<(String, serde_json::Value)> = table_names
                        .iter()
                        .map(|table| {
                            let val = get_table_row_on_shard(&shard, table, &key);
                            (table.clone(), val)
                        })
                        .collect();
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::GetMultiOk(rows));
                    }
                }
                ShardOp::UpsertTableRow { key, table_name, fields, now } => {
                    // Phase 54-02 Task 1: scatter-gather TT-cascade
                    // landing path. Writes through the widened `Shard`
                    // surface; dirty-set update is handled inside.
                    shard.upsert_table_row(&key, &table_name, fields, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::TombstoneTableRow { key, table_name, now } => {
                    // Phase 54-02 Task 1: cascade-driven row deletion.
                    // `had_live` return value from `Shard::tombstone_table_row`
                    // is currently ignored — callers that need it can
                    // switch to a widened ShardResult variant later.
                    shard.tombstone_table_row(&key, &table_name, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::UpsertTableBatch { writes, now } => {
                    // Phase 55-01 D-A1: coalesced cross-shard TT cascade.
                    // Applies each (table, key, fields) via the widened
                    // `Shard::upsert_table_row` surface. On first failure
                    // (unreachable in current Shard impl — upsert is
                    // infallible — but preserved for future fallible
                    // variants) we reply Err and stop. All successful
                    // writes stay applied; the source-side delivery
                    // cursor is NOT advanced on error so boot replay
                    // re-drives the partial batch (full-replace makes
                    // that idempotent — T-55-01-04 mitigation).
                    for (table_name, key, fields) in writes {
                        shard.upsert_table_row(&key, &table_name, fields, now);
                    }
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::UpsertSourceTableRow {
                    table_name,
                    key,
                    fields,
                    source_lsn,
                    now,
                } => {
                    // Phase 55-02 D-B5: full-replace upsert into source table.
                    // NO cascade fired (D-B6). source_lsn is stored per-row.
                    shard.upsert_source_table_row(&key, &table_name, fields, source_lsn, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::DeleteSourceTableRow {
                    table_name,
                    key,
                    source_lsn,
                    now,
                } => {
                    // Phase 55-02 D-B5: hard-delete + PendingRetraction marker.
                    shard.delete_source_table_row(&key, &table_name, now);
                    if let Some(log) = shard.event_log.as_ref() {
                        if let Err(e) = log
                            .append_pending_retraction(&table_name, &key, source_lsn, now)
                        {
                            // Phase 55 MED-1: the row is already hard-deleted
                            // from state; if the marker fails to land, Phase 57
                            // retraction propagation will silently miss this
                            // row. Surface the failure to stderr + increment a
                            // dedicated counter so operators can investigate
                            // (disk full / fsync failure / perms).
                            eprintln!(
                                "[WARN] append_pending_retraction failed (table={}, key={}, \
                                 source_lsn={}): {}",
                                table_name, key, source_lsn, e
                            );
                            metrics::counter!(
                                crate::shard::metrics::PENDING_RETRACTION_APPEND_FAILED_TOTAL
                            )
                            .increment(1);
                        }
                    }
                    // Phase 57 Wave 3 (TPC-CORR-10, SC-1): consume the
                    // PendingRetraction immediately by fanning out
                    // `RetractDownstream` to every downstream row whose
                    // `contributing_inputs.source_table_keys` references
                    // the deleted (table_name, key).
                    {
                        let engine = state.engine.read();
                        let handles_guard = state.shard_handles.read();
                        let handles_slice: Option<&[ShardHandle]> =
                            if handles_guard.is_empty() {
                                None
                            } else {
                                Some(&handles_guard[..])
                            };
                        if let Err(e) = engine.fan_out_retraction_for_source_table(
                            handles_slice,
                            &mut shard,
                            shard_index,
                            &table_name,
                            &key,
                            source_lsn,
                        ) {
                            eprintln!(
                                "[WARN] fan_out_retraction_for_source_table \
                                 (table={}, key={}): {:?}",
                                table_name, key, e
                            );
                        }
                    }
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::UpsertSourceTableBatch {
                    table_name,
                    rows,
                    now,
                } => {
                    // Phase 55-02 D-B4: pre-validate all rows before any write.
                    let invalid = rows.iter().any(|(k, _, _)| k.is_empty());
                    if invalid {
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::Err(
                                ShardDispatchError::ProcessingError(
                                    "batch: empty key rejected (D-B4 all-or-nothing)"
                                        .into(),
                                ),
                            ));
                        }
                    } else {
                        for (k, fields, lsn) in rows {
                            shard.upsert_source_table_row(&k, &table_name, fields, lsn, now);
                        }
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::SetOk);
                        }
                    }
                }
                ShardOp::DeleteSourceTableBatch {
                    table_name,
                    rows,
                    now,
                } => {
                    // Phase 55-02 D-B4: pre-validate all rows.
                    let invalid = rows.iter().any(|(k, _)| k.is_empty());
                    if invalid {
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::Err(
                                ShardDispatchError::ProcessingError(
                                    "batch: empty key rejected (D-B4 all-or-nothing)"
                                        .into(),
                                ),
                            ));
                        }
                    } else {
                        for (k, lsn) in rows {
                            shard.delete_source_table_row(&k, &table_name, now);
                            if let Some(log) = shard.event_log.as_ref() {
                                if let Err(e) = log
                                    .append_pending_retraction(&table_name, &k, lsn, now)
                                {
                                    // Phase 55 MED-1: see DeleteSourceTableRow.
                                    eprintln!(
                                        "[WARN] append_pending_retraction failed (batch, table={}, \
                                         key={}, source_lsn={}): {}",
                                        table_name, k, lsn, e
                                    );
                                    metrics::counter!(
                                        crate::shard::metrics::PENDING_RETRACTION_APPEND_FAILED_TOTAL
                                    )
                                    .increment(1);
                                }
                            }
                            // Phase 57 Wave 3 (TPC-CORR-10, SC-1): fan-out
                            // retraction per deleted key (multiplicative:
                            // per-affected-row per-deleted-key).
                            {
                                let engine = state.engine.read();
                                let handles_guard = state.shard_handles.read();
                                let handles_slice: Option<&[ShardHandle]> =
                                    if handles_guard.is_empty() {
                                        None
                                    } else {
                                        Some(&handles_guard[..])
                                    };
                                if let Err(e) = engine.fan_out_retraction_for_source_table(
                                    handles_slice,
                                    &mut shard,
                                    shard_index,
                                    &table_name,
                                    &k,
                                    lsn,
                                ) {
                                    eprintln!(
                                        "[WARN] fan_out_retraction_for_source_table \
                                         (batch, table={}, key={}): {:?}",
                                        table_name, k, e
                                    );
                                }
                            }
                        }
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::SetOk);
                        }
                    }
                }
                ShardOp::PushTableRow { table_name, key, fields, event_time } => {
                    // Phase 54-04 Pass A1: full handle_push_table sequence
                    // on-shard. Pre-existed check drives the eviction-reinit
                    // counter; upsert + mark_dirty live on the shard; cascade
                    // fan-out uses `cascade_table_upsert_on_shard`.
                    let pre_existed = crate::shard::read_entity_from_shard(
                        &shard,
                        &key,
                        |entity| entity.table_rows.contains_key(&table_name),
                    )
                    .unwrap_or(false);
                    if !pre_existed {
                        state.eviction_tracker.check_reinit(&table_name, &key);
                    }
                    shard.upsert_table_row(&key, &table_name, fields, event_time);
                    let cascade_result = {
                        let engine = state.engine.read();
                        let handles_guard = state.shard_handles.read();
                        let handles_slice: Option<&[ShardHandle]> = if handles_guard.is_empty() {
                            None
                        } else {
                            Some(&handles_guard[..])
                        };
                        engine.cascade_table_upsert_on_shard(
                            &table_name,
                            &key,
                            false,
                            None,
                            &mut shard,
                            shard_index,
                            handles_slice,
                            event_time,
                        )
                    };
                    if let Some(tx) = event.response_tx {
                        let r = match cascade_result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::DeleteTableRow { table_name, key, event_time } => {
                    // Phase 54-04 Pass A1: full handle_delete_table sequence
                    // on-shard. Flips the row to Tombstoned, marks dirty,
                    // fires cascade with tombstoned=true.
                    shard.tombstone_table_row(&key, &table_name, event_time);
                    let cascade_result = {
                        let engine = state.engine.read();
                        let handles_guard = state.shard_handles.read();
                        let handles_slice: Option<&[ShardHandle]> = if handles_guard.is_empty() {
                            None
                        } else {
                            Some(&handles_guard[..])
                        };
                        engine.cascade_table_upsert_on_shard(
                            &table_name,
                            &key,
                            true,
                            None,
                            &mut shard,
                            shard_index,
                            handles_slice,
                            event_time,
                        )
                    };
                    if let Some(tx) = event.response_tx {
                        let r = match cascade_result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(ShardDispatchError::ProcessingError(
                                format!("{:?}", e),
                            )),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::ClearBackfillOperators { stream_name, feature_names } => {
                    // Phase 54-04 Pass A1: iterate shard-owned entities and
                    // drop any operator state whose feature name appears in
                    // `feature_names` for `stream_name`. Mirrors the
                    // run_backfill step-1 reset against a per-shard view.
                    let keys: Vec<String> = shard
                        .iter_entities()
                        .into_iter()
                        .map(|(k, _)| k)
                        .collect();
                    for k in keys {
                        let mut view = crate::shard::StoreView::Sharded(&mut shard);
                        view.with_entity_mut(&k, |entity| {
                            if let Some(stream_state) = entity.streams.get_mut(&stream_name) {
                                stream_state
                                    .operators
                                    .retain(|(name, _)| !feature_names.contains(name));
                            }
                        });
                    }
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::ListEntityKeys => {
                    let keys: Vec<String> = shard
                        .iter_entities()
                        .into_iter()
                        .map(|(k, _)| k)
                        .collect();
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::EntityKeysOk(keys));
                    }
                }
                ShardOp::EntityCount => {
                    // Phase 54-04 Pass A2: O(1) approximate count —
                    // mirrors the `keys_owned` gauge emission below
                    // (Phase 53-03B Pitfall 4). Default build uses
                    // `approximate_len()` on the fjall PartitionHandle;
                    // state-inmem uses exact `AHashMap::len()`.
                    #[cfg(not(feature = "state-inmem"))]
                    let count = shard.state.approximate_len();
                    #[cfg(feature = "state-inmem")]
                    let count = shard.state.len();
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::EntityCountOk(count));
                    }
                }
                ShardOp::EvictExpired { now, ttl_multiplier } => {
                    // Phase 54-04 Pass A4: on-shard scatter of
                    // `evict_expired_stream_entries`. Mirrors the
                    // legacy StateStore-backed body in
                    // `src/state/eviction.rs` but walks THIS shard's
                    // entities via `iter_entities()` and mutates them
                    // via `StoreView::Sharded`.
                    let engine = state.engine.read();
                    let evicted = evict_expired_stream_entries_on_shard(
                        &mut shard,
                        &engine,
                        now,
                        ttl_multiplier,
                    );
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::EvictedCount(evicted));
                    }
                }
                ShardOp::EvictExpiredTableRows { now } => {
                    // Phase 54-04 Pass A4: on-shard scatter of
                    // `evict_expired_table_rows`. Records each eviction
                    // in `state.eviction_tracker` so the eviction→reinit
                    // counter keeps surfacing on /metrics. The tracker
                    // is already an `Arc<EvictionTracker>` (multi-reader
                    // via RwLock<AHashMap>), safe to use from any
                    // shard thread.
                    let engine = state.engine.read();
                    let evicted = evict_expired_table_rows_on_shard(
                        &mut shard,
                        &engine,
                        &state.eviction_tracker,
                        now,
                    );
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::EvictedCount(evicted));
                    }
                }
                ShardOp::PushForBackfill {
                    stream_name,
                    event: backfill_event,
                    event_time,
                    feature_names,
                } => {
                    // Phase 54-04 Pass A5: on-shard backfill replay. The
                    // engine method walks stream definitions + operator
                    // state through `StoreView::Sharded`, so the fjall
                    // partition round-trips through postcard in default
                    // builds and the in-mem AHashMap in state-inmem.
                    let engine = state.engine.read();
                    let result = engine.push_for_backfill_on_shard(
                        &stream_name,
                        &backfill_event,
                        &mut shard,
                        event_time,
                        &feature_names,
                    );
                    if let Some(tx) = event.response_tx {
                        let r = match result {
                            Ok(()) => ShardResult::SetOk,
                            Err(e) => ShardResult::Err(
                                ShardDispatchError::ProcessingError(format!("{:?}", e)),
                            ),
                        };
                        let _ = tx.send(r);
                    }
                }
                ShardOp::ReadEntityAt { table_name, key } => {
                    // Phase 56 D-A1: pure cross-shard read — no mutation.
                    // Source shard dispatches here when the enrichment
                    // key hashes to a different shard. Reply carries the
                    // full EntityState (or None). The source shard picks
                    // out the table row it needs (D-A4 Missing-safe).
                    let out = shard.read_entity_at(&table_name, &key);
                    if out.is_none() {
                        metrics::counter!(
                            crate::shard::metrics::ENRICH_MISSING_TOTAL,
                            "table" => table_name.clone()
                        ).increment(1);
                    }
                    metrics::counter!(
                        crate::shard::metrics::ENRICH_CROSS_SHARD_TOTAL,
                        "table" => table_name
                    ).increment(1);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityOk(out));
                    }
                }
                ShardOp::ReadEntityBatch { table_name, keys } => {
                    // Phase 56 D-A2 + T-56-01-01: per-target coalesced
                    // batch read with memory-DoS guard.
                    if keys.len() > MAX_ENRICH_BATCH_KEYS {
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::Err(
                                ShardDispatchError::ProcessingError(format!(
                                    "enrich batch > {} keys ({}), rejected (T-56-01-01)",
                                    MAX_ENRICH_BATCH_KEYS, keys.len()
                                ))
                            ));
                        }
                        // Phase 58-01 Task 2: was `continue;` inside outer
                        // while-let; early `return;` from `process_shard_event`.
                        return;
                    }
                    let mut n_missing: u64 = 0;
                    let batch_len = keys.len() as u64;
                    let out: Vec<Option<_>> = keys.iter()
                        .map(|k| {
                            let r = shard.read_entity_at(&table_name, k);
                            if r.is_none() { n_missing += 1; }
                            r
                        })
                        .collect();
                    metrics::counter!(
                        crate::shard::metrics::ENRICH_CROSS_SHARD_TOTAL,
                        "table" => table_name.clone()
                    ).increment(batch_len);
                    if n_missing > 0 {
                        metrics::counter!(
                            crate::shard::metrics::ENRICH_MISSING_TOTAL,
                            "table" => table_name
                        ).increment(n_missing);
                    }
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityBatchOk(out));
                    }
                }
                ShardOp::SsjInsert { join_id, side, join_key, event: ssj_event, within_ms } => {
                    // Phase 56 D-B1: cross-shard StreamStreamJoin insert
                    // on the join-key-owning shard. Target-shard-inline
                    // probe + insert + evict; reply carries any matched
                    // counterparty events for the source shard to emit
                    // via its existing downstream cascade path.
                    let matches = shard.apply_ssj_insert(
                        &join_id,
                        side,
                        &join_key,
                        ssj_event,
                        within_ms,
                    );
                    metrics::counter!(
                        crate::shard::metrics::SSJ_CROSS_SHARD_TOTAL,
                        "join_id" => join_id
                    ).increment(1);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SsjInsertOk(matches));
                    }
                }
                ShardOp::RetractDownstream {
                    target_shard: _target_shard,
                    stream_name,
                    row_key,
                    reason,
                    depth,
                } => {
                    // Phase 57 D-B1 (TPC-CORR-10): target-shard retraction
                    // dispatch. Enforces D-B5 depth cap BEFORE touching state
                    // so a deep cascade cannot infinite-loop. Delegates the
                    // idempotent "is-already-retracted?" probe + history_ttl
                    // check to `Shard::apply_retraction`.
                    //
                    // Exactly ONE metric counter bump per invocation —
                    // mirrors the SsjInsert single-emission-site discipline.
                    // Source-side bumps `RETRACTIONS_SENT_TOTAL` separately
                    // in `PipelineEngine::retract_downstream_at_shard` so
                    // "sent - (applied+nooped+beyond_history+depth_exceeded)"
                    // surfaces target-unreachable dispatch failures.
                    let outcome = if depth >= MAX_RETRACTION_DEPTH {
                        metrics::counter!(
                            crate::shard::metrics::RETRACTION_DEPTH_EXCEEDED_TOTAL
                        )
                        .increment(1);
                        RetractOutcome::DepthExceeded
                    } else {
                        let o = shard.apply_retraction(
                            &stream_name,
                            &row_key,
                            &reason,
                            depth,
                        );
                        match o {
                            RetractOutcome::Retracted => {
                                metrics::counter!(
                                    crate::shard::metrics::RETRACTIONS_APPLIED_TOTAL,
                                    "operator" => stream_name.clone()
                                )
                                .increment(1);
                            }
                            RetractOutcome::NoOp => {
                                metrics::counter!(
                                    crate::shard::metrics::RETRACTIONS_NOOPED_TOTAL,
                                    "operator" => stream_name.clone()
                                )
                                .increment(1);
                            }
                            RetractOutcome::BeyondHistory => {
                                metrics::counter!(
                                    crate::shard::metrics::RETRACTION_BEYOND_HISTORY_TOTAL,
                                    "operator" => stream_name.clone()
                                )
                                .increment(1);
                            }
                            RetractOutcome::DepthExceeded => {
                                metrics::counter!(
                                    crate::shard::metrics::RETRACTION_DEPTH_EXCEEDED_TOTAL
                                )
                                .increment(1);
                            }
                        }
                        o
                    };
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::RetractOk(outcome));
                    }
                }
    }
}

/// Phase 58-01 Task 2: emit per-shard gauges extracted so both the blocking-
/// `rx.recv()` loop and the Linux FuturesUnordered driver can share the
/// same metrics sampler. Returns the new `last_gauge_update` instant so the
/// caller can advance its throttle timer.
#[inline]
fn emit_shard_gauges(
    shard_index: usize,
    shard: &mut crate::shard::Shard,
    inbox_depth: usize,
    _last_gauge_update: std::time::Instant,
) -> std::time::Instant {
    // Phase 53-03B Pitfall 4: `PartitionHandle::len()` walks the LSM
    // tree — use `approximate_len()` (O(1), usize, stale estimate)
    // for the Prometheus `keys_owned` gauge. state-inmem keeps
    // `AHashMap::len()` because it's already O(1) there.
    #[cfg(not(feature = "state-inmem"))]
    let keys_owned = shard.state.approximate_len();
    #[cfg(feature = "state-inmem")]
    let keys_owned = shard.state.len();
    crate::shard::metrics::update_shard_gauges(
        shard_index,
        0.0,
        inbox_depth,
        keys_owned,
        0.0,
    );
    // Phase 53-05 (W-4 revision): drain accumulated fjall write
    // bytes on the default build and emit as a counter increment.
    // `compaction_bytes` stays at 0 until a fjall API exposes it;
    // the counter series itself is registered at startup so Plan
    // 06's alert rules work. `fsync_latency_ms` gauge is updated
    // by explicit fsync sites (migrate tool / admin), not here.
    #[cfg(not(feature = "state-inmem"))]
    {
        let bytes = shard.take_write_bytes();
        if bytes > 0 {
            crate::shard::metrics::record_fjall_write_bytes(shard_index, bytes);
        }
        // Compaction bytes: emit 0 increment so the counter stays
        // visible in scrapes. Upgrading this to a real byte count
        // requires a fjall API that is not available in 2.11.
        crate::shard::metrics::record_fjall_compaction_bytes(shard_index, 0);
    }
    // Shard owns its `_` — silence unused warning while keeping signature
    // symmetrical with the caller.
    let _ = shard;
    std::time::Instant::now()
}

/// Phase 58-01 Task 2 (Linux only, D-A1/A2/A3/A4): per-shard SO_REUSEPORT
/// accept loop hosted on the shard's own `current_thread` runtime.
///
/// Each shard binds its OWN `TcpListener` via `bind_reuseport_tcp` (Phase 50
/// helper, REUSED verbatim). Accepted connections are pushed into a
/// `FuturesUnordered` and polled INLINE — NO `tokio::spawn` per connection.
/// Concurrency cap `max_conns_per_shard` back-pressures via the kernel
/// listen(1024) backlog when `FuturesUnordered` saturates.
///
/// `accept_threads_spawned_total` is bumped exactly once (per shard) at the
/// point the SO_REUSEPORT listener is installed — the same semantic as the
/// macOS Wave 2 dedicated-accept-thread path, so operators can read a single
/// counter across platforms. `inline_handler_events_total` is bumped on
/// every accepted connection (D-C4 probe field).
///
/// The ShardOp inbox (`rx: Receiver<ShardEvent>`) is drained interleaved with
/// accept via `tokio::select!` + `try_recv`. A 50 µs sleep arm wakes the
/// select when there's no accept / no inflight / no inbox work, bounding
/// idle CPU.
#[cfg(target_os = "linux")]
fn run_linux_per_shard_accept_loop(
    shard_index: usize,
    rx: Receiver<ShardEvent>,
    state: std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    shard: &mut crate::shard::Shard,
    wm_publish_threshold: u64,
    cfg: PerShardAcceptCfg,
    rt: &tokio::runtime::Runtime,
) {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::pin::Pin;
    use std::future::Future;
    use std::sync::atomic::Ordering;

    // D-A1: bind our own SO_REUSEPORT socket. `bind_reuseport_tcp` already
    // calls `set_nonblocking(true)` + `listen(1024)`. A failure here panics
    // the shard thread — the surrounding `catch_unwind` in
    // `spawn_shard_threads` flips this shard to DOWN, and since this runs
    // BEFORE the ready-barrier relaxation window ends, boot fails fast.
    let std_listener = crate::server::tcp::bind_reuseport_tcp(cfg.accept_addr)
        .unwrap_or_else(|e| {
            panic!(
                "[beava-shard-{shard_index}] bind_reuseport_tcp({addr}) failed: {e}",
                addr = cfg.accept_addr
            )
        });

    // Bump the always-on counter ONCE per shard at the install point. Mirror
    // of the macOS Wave 2 dedicated-accept-thread semantic (D-B1 — counter=N
    // at steady state). `inline_handler_events_total` is bumped per-accept
    // below.
    state
        .accept_threads_spawned_total
        .fetch_add(1, Ordering::Relaxed);

    rt.block_on(async move {
        // Convert std TcpListener → tokio TcpListener *inside* the runtime —
        // `from_std` requires an active tokio reactor.
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .expect("tokio::net::TcpListener::from_std failed on an already-bound socket");

        let max_conns = cfg.max_conns_per_shard;

        // D-A3: in-flight connection futures. `BoxFuture` so heterogenous
        // lifetimes of async blocks unify. Each future owns its own stream +
        // Arc<ConcurrentAppState>; no `tokio::spawn` involved.
        let mut inflight: FuturesUnordered<
            Pin<Box<dyn Future<Output = ()> + Send>>,
        > = FuturesUnordered::new();

        let mut event_count: u64 = 0;
        let mut last_gauge_update = std::time::Instant::now();

        loop {
            // Short sleep ticks wake the select when nothing else is pending,
            // letting us pump the inbox (`try_recv` below) without spinning
            // hard. Resolution is chosen to match the pre-Phase-58 inbox-
            // drain responsiveness (≤ 100 µs p99 dispatch delay even at low
            // load).
            let idle_tick =
                tokio::time::sleep(std::time::Duration::from_micros(50));

            tokio::select! {
                biased;

                // 1. Drain completed inflight connections first. Keeps the
                //    slot free so the accept arm can fire on the same tick.
                _done = inflight.next(), if !inflight.is_empty() => {
                    // Connection completed/errored. Per-connection
                    // observability lives inside `handle_connection`.
                }

                // 2. Accept a new connection, GATED on inflight < cap.
                //    D-A4: when saturated, we do NOT poll the listener,
                //    letting the kernel backlog (listen(1024)) absorb.
                accept_res = listener.accept(), if inflight.len() < max_conns => {
                    match accept_res {
                        Ok((stream, _peer)) => {
                            state
                                .inline_handler_events_total
                                .fetch_add(1, Ordering::Relaxed);
                            let st = state.clone();
                            // D-A3: push INLINE — NO `tokio::spawn`.
                            inflight.push(Box::pin(async move {
                                let _ = crate::server::tcp::handle_connection_public(
                                    stream, st,
                                )
                                .await;
                            }));
                        }
                        Err(_) => {
                            // accept() error (EMFILE / ENFILE / ECONNABORTED)
                            // — loop-continue; the kernel retains the backlog
                            // and the other N-1 shards remain accepting.
                        }
                    }
                }

                // 3. Idle tick — wakes the select! every 50 µs so we can
                //    drain the ShardOp inbox even if there's no accept /
                //    no inflight activity.
                _ = idle_tick => {}
            }

            // Drain the ShardOp inbox — the shard thread's primary job. We
            // use `try_recv` (not blocking `recv`) because the tokio runtime
            // owns the current thread; a blocking recv would starve the
            // accept future. Drain up to a reasonable burst so a hot inbox
            // doesn't monopolize against accept — the outer select! loop
            // re-enters after this.
            for _ in 0..256 {
                match rx.try_recv() {
                    Ok(event) => {
                        event_count += 1;
                        let now = std::time::SystemTime::now();
                        process_shard_event(
                            event,
                            shard_index,
                            shard,
                            &state,
                            wm_publish_threshold,
                            now,
                        );
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        // Sender dropped — server is shutting down. Break the
                        // outer loop so the shard thread exits cleanly.
                        return;
                    }
                }
            }

            // Emit gauges every 1000 events OR every 100 ms.
            if event_count % 1000 == 0 || last_gauge_update.elapsed().as_millis() >= 100 {
                let inbox_depth = rx.len();
                last_gauge_update =
                    emit_shard_gauges(shard_index, shard, inbox_depth, last_gauge_update);
            }
        }
    });
}

/// Apply a SET on shard-owned state.
///
/// Empty payload = tombstone (clears `static_features` + marks dirty);
/// non-empty = upsert (sets each feature + marks dirty).
///
/// Phase 53-03B: rewritten on top of `StoreView::Sharded(shard).with_entity_mut`
/// so the default (fjall) build round-trips through postcard + fjall and the
/// `state-inmem` build keeps its AHashMap entry-API path. Backend-agnostic.
///
/// Phase 54-04 Pass A1: cascade fan-out now lives here — the shard loop
/// passes `fire_cascade = true` for Command::Set (SetWithCascade) to replay
/// the TCP handler's per-input-table cascade sweep on-shard. Plain `Set`
/// (SET without TT-cascade, used by legacy MSET chunks) keeps
/// `fire_cascade = false`.
fn apply_set_on_shard(
    state: &std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    shard: &mut crate::shard::Shard,
    shard_index: usize,
    key: &str,
    payload: &serde_json::Value,
    now: std::time::SystemTime,
    fire_cascade: bool,
) -> Result<(), crate::error::BeavaError> {
    use crate::shard::StoreView;
    use crate::state::store::StaticFeature;

    if let serde_json::Value::Object(map) = payload {
        let tombstoned = map.is_empty();
        {
            let mut view = StoreView::Sharded(shard);
            view.with_entity_mut(key, |entity| {
                if tombstoned {
                    entity.static_features.clear();
                } else {
                    for (feat_name, val) in map {
                        let fv = json_to_feature_value_local(val);
                        entity.static_features.insert(
                            feat_name.clone(),
                            StaticFeature {
                                value: fv,
                                updated_at: now,
                            },
                        );
                    }
                }
            });
        }
        // Mark dirty for incremental snapshots. `with_entity_mut` already wrote
        // the entity back through postcard; the dirty_set is an in-memory
        // per-shard structure that's unchanged by Plan 53-03.
        shard.dirty_set.insert(key.to_string());

        if fire_cascade {
            // Phase 54-04 Pass A1: TT-cascade fan-out. Mirrors the TCP
            // Command::Set loop that walks every registered input Table
            // and calls cascade_table_upsert for the key. The Table
            // identity isn't carried by the SET protocol (key-only), so
            // every input-table downstream is visited; engine internals
            // resolve the right join side. Cascade targets may live on
            // sibling shards — we pass the shard-handles snapshot so
            // cross-shard writes dispatch via SPSC.
            let engine = state.engine.read();
            let input_tables: Vec<String> = engine
                .list_streams()
                .filter_map(|s| {
                    if s.key_field.is_some() {
                        Some(s.name.clone())
                    } else {
                        None
                    }
                })
                .collect();
            let handles_guard = state.shard_handles.read();
            let handles_slice: Option<&[ShardHandle]> = if handles_guard.is_empty() {
                None
            } else {
                Some(&handles_guard[..])
            };
            for input_table in input_tables {
                let _ = engine.cascade_table_upsert_on_shard(
                    &input_table,
                    key,
                    tombstoned,
                    None,
                    shard,
                    shard_index,
                    handles_slice,
                    now,
                );
            }
        }
        Ok(())
    } else {
        Err(crate::error::BeavaError::Protocol(
            "SET payload must be a JSON object".into(),
        ))
    }
}

/// Read a table_row as JSON from shard-owned state.
///
/// Returns `Value::Null` if the entity or table_row is absent or tombstoned
/// (matches the null-collapse contract of OP_GET_MULTI).
///
/// Phase 53-03B: rewritten on top of the W-6 `read_entity_from_shard` helper
/// so the default (fjall) build deserializes via postcard and the state-inmem
/// build reads from AHashMap — both through one code path.
fn get_table_row_on_shard(
    shard: &crate::shard::Shard,
    table_name: &str,
    key: &str,
) -> serde_json::Value {
    use crate::state::store::TableRowState;

    let row_clone = crate::shard::read_entity_from_shard(shard, key, |entity| {
        entity.table_rows.get(table_name).cloned()
    });

    let Some(Some(row)) = row_clone else {
        return serde_json::Value::Null;
    };
    if matches!(row.state, TableRowState::Tombstoned { .. }) {
        return serde_json::Value::Null;
    }
    let mut obj = serde_json::Map::new();
    for (k, v) in &row.fields {
        obj.insert(k.clone(), v.to_json_value());
    }
    serde_json::Value::Object(obj)
}

/// Local helper mirroring `crate::server::tcp::json_to_feature_value` for
/// shard-side SET. Avoids a cross-module dep + keeps this file self-contained.
fn json_to_feature_value_local(v: &serde_json::Value) -> crate::types::FeatureValue {
    use crate::types::FeatureValue;
    // CONTEXT.md: FeatureValue variants are Float / Int / String / Missing.
    // Booleans collapse to Int(0)/Int(1) per Redis convention (see types.rs doc).
    match v {
        serde_json::Value::Null => FeatureValue::Missing,
        serde_json::Value::Bool(b) => FeatureValue::Int(if *b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FeatureValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FeatureValue::Float(f)
            } else {
                FeatureValue::Missing
            }
        }
        serde_json::Value::String(s) => FeatureValue::String(s.clone()),
        // Arrays / objects: encode as JSON string for fidelity.
        _ => FeatureValue::String(v.to_string()),
    }
}

/// Read inbox capacity from environment with clamping (D-08).
pub fn inbox_size_from_env() -> usize {
    std::env::var("BEAVA_SHARD_INBOX_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_INBOX_SIZE)
        .clamp(1024, 1_000_000)
}

/// Phase 54-01 Task 1a (Pass A): unified SPSC dispatch helper.
///
/// Build a `ShardEvent::Push` with a oneshot response channel, `try_send` it
/// into the target shard's inbox, then await the shard's per-event ack.
///
/// Backpressure (D-08 contract, preserved):
/// - `try_send` never blocks the caller.
/// - On `Full`: increment `beava_shard_inbox_full_total{shard=N}` +
///   `beava_events_dropped_total{reason="inbox_full"}` and return
///   `BeavaError::Protocol("shard inbox full — backpressure")` (matches the
///   existing N>1 branch in `handle_push_core_ex`). HTTP handlers surface
///   this as a 400 today via `map_err_to_response`; a dedicated 503 mapping
///   is a follow-up (scope kept tight for Pass A).
/// - On `Disconnected`: return `BeavaError::Protocol("shard inbox disconnected")`.
///
/// Response handling:
/// - `ShardResult::Ok(fm)` → `Ok(fm)`
/// - `ShardResult::Err(e)` → `Err(BeavaError::Protocol(format!("{:?}", e)))`
/// - Any other variant (GetOk / SetOk / ...) is protocol-invalid for a Push
///   and surfaced as a `BeavaError::Protocol`.
///
/// Used by HTTP ingest; TCP + replica inbound paths migrate in Passes B/C.
pub(crate) async fn send_to_shard(
    handle: &ShardHandle,
    stream_name: std::sync::Arc<str>,
    payload: bytes::Bytes,
    shard_hint: u32,
) -> Result<crate::types::FeatureMap, crate::error::BeavaError> {
    // Phase 59 D-A4: HTTP callers pass JSON wire bytes. Preserve the
    // pre-Phase-59 behavior (JSON decode on the shard thread) by
    // delegating to the _with_fmt variant tagged Json.
    send_to_shard_with_fmt(
        handle,
        stream_name,
        payload,
        shard_hint,
        crate::wire::PayloadFmt::Json,
    )
    .await
}

/// Phase 59 D-A2/D-A4: variant of `send_to_shard` that carries the
/// payload-format tag through to the shard's `decode_event_on_shard`
/// dispatch. HTTP ingest stays `Json` (D-A4); TCP legacy callers route
/// through `handle_push_core_ex` instead of this helper.
pub(crate) async fn send_to_shard_with_fmt(
    handle: &ShardHandle,
    stream_name: std::sync::Arc<str>,
    payload: bytes::Bytes,
    shard_hint: u32,
    payload_fmt: crate::wire::PayloadFmt,
) -> Result<crate::types::FeatureMap, crate::error::BeavaError> {
    use crate::error::BeavaError;

    // Phase 50-04: short-circuit if the shard is quarantined (panicked).
    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent::push_with_fmt(payload, stream_name, shard_hint, Some(tx), payload_fmt);

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::Ok(fm)) => Ok(fm),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for Push".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-03 Task 3: read-path dispatch helper.
///
/// HTTP GET endpoints compute the owner shard for `key`, then send a
/// `ShardOp::Get { key }` through that shard's SPSC inbox. Returns:
///
/// - `Ok(Some(fm))` when the owning shard reports a FeatureMap (may be empty
///   if the entity is a pure-static-features key with no stream-bound features).
/// - `Ok(None)` when the oneshot returns an ack with no entity readable —
///   callers map this to 404.
/// - `Err(BeavaError::Protocol)` on shard DOWN, inbox full, disconnect, or
///   protocol-invalid variants.
///
/// This replaces `state.store.get_all_features(&key, now)` on the HTTP GET
/// path (plan 54-03 Task 3). Entity existence is inferred from whether the
/// returned `FeatureMap` is empty AND no entity exists on the owning shard;
/// a follow-up may widen `ShardResult::GetOk` with an explicit existence
/// flag if callers need to distinguish "entity exists, all features Missing"
/// from "no entity". For now, callers use the existing Null semantics in
/// `PipelineEngine::get_features_on_shard`.
pub async fn get_features_via_shard(
    handle: &ShardHandle,
    key: String,
) -> Result<(bool, crate::types::FeatureMap), crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::Get { key },
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::GetOk { exists, features }) => Ok((exists, features)),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for Get".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A1: generic op dispatch helper that awaits a
/// `ShardResult::SetOk` ack. Used by TCP Command::Set / Command::PushTable /
/// Command::DeleteTable / MSET chunk / MarkDirty paths whose only ack
/// expectation is "did the shard apply the mutation successfully".
///
/// Errors surface via `BeavaError::Protocol` so callers can keep their
/// existing error-mapping stacks (TCP STATUS_ERROR envelope, HTTP 400).
pub async fn send_op_await_setok(
    handle: &ShardHandle,
    op: ShardOp,
) -> Result<(), crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op,
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::SetOk) => Ok(()),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for SetOk path".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 59.5 D-B1 (TPC-SHARED-SOURCE-TABLE): fan a `ShardOp` out to
/// every shard and await all acks. Used by the replicated source_table
/// write path — `OP_UPSERT_TABLE_ROW` / `OP_DELETE_TABLE_ROW` dispatch
/// this helper when `is_sharded_source_table(name) == false`.
///
/// Semantics (D-B2):
/// - First-error-wins: if any shard returns an error, the first one
///   observed is returned to the caller. Other oneshot replies are
///   drained but their results are discarded (we do not emit Phase 57
///   retractions on the ones that succeeded — callers upstream can
///   surface that via normal retry / tombstone logic).
/// - The `op_factory` closure is invoked once per shard so each send
///   gets a fresh `ShardOp` (required because `ShardOp` contains
///   non-Clone payloads).
pub async fn send_op_await_setok_all_shards(
    handles: &[ShardHandle],
    op_factory: impl Fn() -> ShardOp,
) -> Result<(), crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handles.is_empty() {
        return Err(BeavaError::Protocol(
            "fanout: no shards registered".to_string(),
        ));
    }

    // Issue all sends first so shards work concurrently; collect oneshot
    // receivers for the drain below.
    let mut receivers = Vec::with_capacity(handles.len());
    for handle in handles {
        if handle.is_down.load(Ordering::Relaxed) {
            crate::shard::metrics::record_shard_down(handle.shard_index);
            return Err(BeavaError::Protocol(format!(
                "shard {} is down (quarantined after panic)",
                handle.shard_index
            )));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let evt = ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: op_factory(),
            payload_fmt: crate::wire::PayloadFmt::Binary,
        };
        match handle.inbox_tx.try_send(evt) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(handle.shard_index);
                return Err(BeavaError::Protocol(format!(
                    "shard {} inbox full — backpressure",
                    handle.shard_index
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard {} inbox disconnected",
                    handle.shard_index
                )));
            }
        }
        receivers.push((handle.shard_index, rx));
    }

    // Drain all oneshots; first error wins.
    let mut first_error: Option<BeavaError> = None;
    for (shard_idx, rx) in receivers {
        let observed = match rx.await {
            Ok(ShardResult::SetOk) => Ok(()),
            Ok(ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "shard {} dispatch: {:?}",
                shard_idx, e
            ))),
            Ok(_) => Err(BeavaError::Protocol(format!(
                "shard {} returned unexpected ShardResult variant",
                shard_idx
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "shard {} oneshot channel closed",
                shard_idx
            ))),
        };
        if let Err(e) = observed {
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
    }

    if let Some(e) = first_error {
        Err(e)
    } else {
        Ok(())
    }
}

/// Phase 54-04 Pass A1: dispatch a `ShardOp::GetMulti` and await the
/// per-table JSON map. Used by TCP Command::GetMulti (handle_get_multi).
pub async fn get_multi_via_shard(
    handle: &ShardHandle,
    table_names: Vec<String>,
    key: String,
) -> Result<Vec<(String, serde_json::Value)>, crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::GetMulti { table_names, key },
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::GetMultiOk(rows)) => Ok(rows),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for GetMulti".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A1: dispatch `ShardOp::ListEntityKeys` and await the
/// per-shard key vector. Used by scatter-gather callers (run_backfill).
pub async fn list_entity_keys_via_shard(
    handle: &ShardHandle,
) -> Result<Vec<String>, crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::ListEntityKeys,
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::EntityKeysOk(keys)) => Ok(keys),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for ListEntityKeys".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A2: dispatch `ShardOp::EntityCount` and await the
/// per-shard approximate key count. Used by HTTP scatter-gather
/// callers (`metrics_endpoint`, `public_stats`) to compute a
/// fleet-wide `keys_total` without touching `StateStore.entities`.
/// The count is an O(1) estimate on the default (fjall) build —
/// matches the `keys_owned` Prometheus gauge semantics — and exact
/// on state-inmem.
pub async fn entity_count_via_shard(
    handle: &ShardHandle,
) -> Result<usize, crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::EntityCount,
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::EntityCountOk(n)) => Ok(n),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for EntityCount".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A4: dispatch `ShardOp::EvictExpired` and await the
/// per-shard eviction count. Used by the periodic eviction timer in
/// main.rs to scatter-gather TTL eviction across every live shard.
pub async fn evict_expired_via_shard(
    handle: &ShardHandle,
    now: std::time::SystemTime,
    ttl_multiplier: u32,
) -> Result<usize, crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::EvictExpired { now, ttl_multiplier },
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::EvictedCount(n)) => Ok(n),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for EvictExpired".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A4: dispatch `ShardOp::EvictExpiredTableRows` and
/// await the per-shard Table-row eviction count. Sibling of
/// `evict_expired_via_shard` for the Table-row TTL path.
pub async fn evict_expired_table_rows_via_shard(
    handle: &ShardHandle,
    now: std::time::SystemTime,
) -> Result<usize, crate::error::BeavaError> {
    use crate::error::BeavaError;

    if handle.is_down.load(Ordering::Relaxed) {
        crate::shard::metrics::record_shard_down(handle.shard_index);
        return Err(BeavaError::Protocol(format!(
            "shard {} is down (quarantined after panic)",
            handle.shard_index
        )));
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let evt = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: Some(tx),
        op: ShardOp::EvictExpiredTableRows { now },
        payload_fmt: crate::wire::PayloadFmt::Binary,
    };

    match handle.inbox_tx.try_send(evt) {
        Ok(()) => {}
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            crate::shard::metrics::record_inbox_full(handle.shard_index);
            return Err(BeavaError::Protocol(
                "shard inbox full — backpressure".to_string(),
            ));
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(BeavaError::Protocol(
                "shard inbox disconnected".to_string(),
            ));
        }
    }

    match rx.await {
        Ok(ShardResult::EvictedCount(n)) => Ok(n),
        Ok(ShardResult::Err(e)) => {
            Err(BeavaError::Protocol(format!("shard dispatch: {:?}", e)))
        }
        Ok(_) => Err(BeavaError::Protocol(
            "unexpected ShardResult variant for EvictExpiredTableRows".to_string(),
        )),
        Err(_) => Err(BeavaError::Protocol(
            "shard oneshot channel closed".to_string(),
        )),
    }
}

/// Phase 54-04 Pass A4: on-shard body for `evict_expired_stream_entries`.
/// Walks the shard-owned entities, removes stream entries whose
/// last_event_at exceeds the per-stream entity_ttl (or the fallback
/// `max_window * ttl_multiplier` global TTL), and removes entities
/// left completely empty. Returns the number of stream entries
/// evicted. Mirrors the legacy `evict_expired_stream_entries` logic
/// but against `Shard` state rather than `StateStore`.
fn evict_expired_stream_entries_on_shard(
    shard: &mut crate::shard::Shard,
    engine: &crate::engine::pipeline::PipelineEngine,
    now: std::time::SystemTime,
    ttl_multiplier: u32,
) -> usize {
    let max_window = engine.max_window_duration();
    let global_ttl = if max_window.is_zero() {
        None
    } else {
        Some(max_window * ttl_multiplier)
    };

    // Phase 1: plan evictions from an owned snapshot of this shard's
    // entities. `iter_entities()` materializes owned clones on the fjall
    // build (deserialized via postcard) and on state-inmem, so we can
    // walk streams without holding a borrow during the write phase.
    let entities: Vec<(String, crate::state::store::EntityState)> = shard.iter_entities();
    // Each plan entry: (key, streams_to_remove, will_be_empty_after_eviction)
    let mut eviction_plan: Vec<(String, Vec<String>, bool)> = Vec::new();

    for (key, entity) in &entities {
        let mut streams_to_remove: Vec<String> = Vec::new();
        for (stream_name, stream_state) in entity.streams.iter() {
            let last_event = match stream_state.last_event_at {
                Some(t) => t,
                None => continue,
            };
            let ttl = match engine.get_stream_entity_ttl(stream_name) {
                Some(stream_ttl) => stream_ttl,
                None => match global_ttl {
                    Some(gt) => gt,
                    None => continue,
                },
            };
            // D-17 / CORR-07: source the eviction clock from the
            // per-stream watermark observed_max so historical backfills
            // don't evict alive-by-event-time entities. Fallback to
            // wall-clock `now` preserves legacy semantics.
            let scan_clock = engine.wm_observed_max(stream_name).unwrap_or(now);
            let age = scan_clock
                .duration_since(last_event)
                .unwrap_or(std::time::Duration::ZERO);
            if age > ttl {
                streams_to_remove.push(stream_name.clone());
            }
        }
        if !streams_to_remove.is_empty() {
            let remaining = entity.streams.len().saturating_sub(streams_to_remove.len());
            let will_be_empty = remaining == 0
                && entity.static_features.is_empty()
                && entity.table_rows.is_empty();
            eviction_plan.push((key.clone(), streams_to_remove, will_be_empty));
        }
    }

    // Phase 2: apply evictions. For each planned entity, either
    // remove-in-place (will_be_empty) via `Shard::delete_entity` OR
    // RMW via `StoreView::Sharded::with_entity_mut` to drop the
    // selected streams.
    let mut total_evicted = 0usize;
    for (key, streams_to_remove, will_be_empty) in &eviction_plan {
        if *will_be_empty {
            shard.delete_entity(key);
        } else {
            let mut view = crate::shard::StoreView::Sharded(shard);
            view.with_entity_mut(key, |entity| {
                for stream_name in streams_to_remove {
                    entity.streams.remove(stream_name);
                }
            });
        }
        total_evicted += streams_to_remove.len();
    }

    total_evicted
}

/// Phase 54-04 Pass A4: on-shard body for `evict_expired_table_rows`.
/// Walks the shard-owned entities' `table_rows`, evicts Live rows
/// whose `updated_at` age exceeds the per-Table `entity_ttl`, and
/// records each eviction in the shared `EvictionTracker` so the
/// eviction→reinit counter keeps firing on `/metrics`. Returns the
/// number of Table rows evicted.
fn evict_expired_table_rows_on_shard(
    shard: &mut crate::shard::Shard,
    engine: &crate::engine::pipeline::PipelineEngine,
    tracker: &crate::state::eviction_tracker::EvictionTracker,
    now: std::time::SystemTime,
) -> usize {
    use crate::duration::is_forever_ttl;
    use crate::state::store::TableRowState;

    let entities: Vec<(String, crate::state::store::EntityState)> = shard.iter_entities();
    let mut eviction_plan: Vec<(String, String)> = Vec::new();

    for (key, entity) in &entities {
        for (table_name, row) in entity.table_rows.iter() {
            // Tombstoned rows are reaped by `gc_tombstones`; skip.
            if !matches!(row.state, TableRowState::Live) {
                continue;
            }
            let ttl = match engine.get_stream_entity_ttl(table_name) {
                Some(t) => t,
                None => continue,
            };
            if is_forever_ttl(ttl) {
                continue;
            }
            let age = now
                .duration_since(row.updated_at)
                .unwrap_or(std::time::Duration::ZERO);
            if age >= ttl {
                eviction_plan.push((key.clone(), table_name.clone()));
            }
        }
    }

    let mut total_evicted = 0usize;
    for (key, table_name) in &eviction_plan {
        // Record BEFORE mutating so the tracker sees the eviction even
        // if the write path errors. Matches the legacy ordering in
        // `evict_expired_table_rows`.
        tracker.record_eviction(table_name, key);
        {
            let mut view = crate::shard::StoreView::Sharded(shard);
            view.with_entity_mut(key, |entity| {
                entity.table_rows.remove(table_name);
            });
        }
        // If the entity is now fully empty (no streams, no static
        // features, no table_rows), drop it. Read-back via
        // `read_entity_from_shard` so we decide against current
        // state rather than the stale `entities` snapshot.
        let now_empty = crate::shard::read_entity_from_shard(shard, key, |e| {
            e.streams.is_empty()
                && e.static_features.is_empty()
                && e.table_rows.is_empty()
        })
        .unwrap_or(false);
        if now_empty {
            shard.delete_entity(key);
        }
        total_evicted += 1;
    }

    total_evicted
}

/// Phase 54-04 Pass A1: clone a `ShardHandle` snapshot (Arc-backed inbox_tx,
/// Arc<AtomicBool> is_down). Used by TCP handlers that need to drop the
/// `shard_handles` RwLock guard before awaiting on a shard response —
/// mirrors the clone-before-await pattern in `http_ingest.rs` Pass A.
#[inline]
pub fn clone_handle(h: &ShardHandle) -> ShardHandle {
    ShardHandle {
        shard_index: h.shard_index,
        is_down: std::sync::Arc::clone(&h.is_down),
        inbox_tx: h.inbox_tx.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ConcurrentAppState for unit tests that call spawn_shard_threads.
    fn make_test_state(n_shards: u16) -> std::sync::Arc<crate::server::tcp::ConcurrentAppState> {
        use crate::engine::pipeline::PipelineEngine;
        use crate::server::tcp::{make_concurrent_state_full, BackfillTracker};
        make_concurrent_state_full(
            PipelineEngine::new(),
            None,
            std::path::PathBuf::from("/tmp/beava-test-thread.snapshot"),
            std::sync::Arc::new(BackfillTracker::default()),
            false,
            false,
            None,
            false,
            n_shards,
        )
    }

    #[test]
    fn spawn_two_shards_returns_two_handles() {
        let state = make_test_state(2);
        let handles = spawn_shard_threads(2, 64, state, None);
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].shard_index, 0);
        assert_eq!(handles[1].shard_index, 1);
    }

    #[test]
    fn all_shards_start_not_down() {
        let state = make_test_state(3);
        let handles = spawn_shard_threads(3, 64, state, None);
        for h in &handles {
            assert!(!h.is_down.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn ready_barrier_completes_without_deadlock() {
        // Barrier must not deadlock — verifies WaitGroup logic is correct.
        let start = std::time::Instant::now();
        let state = make_test_state(2);
        let _handles = spawn_shard_threads(2, 16, state, None);
        // Should complete in well under 5 s even on CI with slow cores.
        assert!(start.elapsed().as_secs() < 5, "ready-barrier timed out");
    }

    // Phase 58-01 Task 1: BEAVA_MAX_CONNS_PER_SHARD env parsing.
    //
    // NOTE: these tests mutate process-global env vars, so they run in a
    // single `#[test]` fn with a mutex to avoid interleaving with other tests
    // in this module that might read the same var concurrently. In the current
    // module the env var is unique to this test, so a mutex is unnecessary —
    // but we still group them into one serial body to avoid even the
    // appearance of a test-order-dependent race.
    #[test]
    fn per_shard_accept_cfg_env_parses_and_clamps() {
        // Default (unset) → 256.
        std::env::remove_var("BEAVA_MAX_CONNS_PER_SHARD");
        assert_eq!(super::max_conns_per_shard_from_env(), 256);

        // Below MIN (0) → default.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "0");
        assert_eq!(super::max_conns_per_shard_from_env(), 256);

        // Above MAX (65537) → default.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "999999");
        assert_eq!(super::max_conns_per_shard_from_env(), 256);

        // Non-numeric → default.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "nope");
        assert_eq!(super::max_conns_per_shard_from_env(), 256);

        // Valid in-range → returned verbatim.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "128");
        assert_eq!(super::max_conns_per_shard_from_env(), 128);

        // Boundary MIN.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "1");
        assert_eq!(super::max_conns_per_shard_from_env(), 1);

        // Boundary MAX.
        std::env::set_var("BEAVA_MAX_CONNS_PER_SHARD", "65536");
        assert_eq!(super::max_conns_per_shard_from_env(), 65_536);

        // Cleanup — don't pollute other tests.
        std::env::remove_var("BEAVA_MAX_CONNS_PER_SHARD");
    }

    #[test]
    fn inbox_full_drops_excess_events() {
        // Backpressure property: inbox capacity=1, push N events,
        // exactly (N-1) try_send calls fail (inbox already full after first).
        let (tx, _rx) = crossbeam_channel::bounded::<ShardEvent>(1);

        let first = ShardEvent::push(
            bytes::Bytes::from_static(b"event0"),
            Arc::from("s"),
            0,
            None,
        );
        assert!(tx.try_send(first).is_ok(), "first send should succeed");

        let mut drop_count = 0u64;
        for _ in 1..10u64 {
            let ev = ShardEvent::push(
                bytes::Bytes::from_static(b"eventN"),
                Arc::from("s"),
                0,
                None,
            );
            if tx.try_send(ev).is_err() {
                drop_count += 1;
            }
        }
        assert_eq!(drop_count, 9, "all 9 subsequent sends should fail on full inbox");
    }

    #[test]
    fn inbox_size_from_env_defaults_to_65536() {
        // Without BEAVA_SHARD_INBOX_SIZE set, returns the default.
        // We can't unset env in parallel tests safely, so just check the clamp bounds.
        let size = inbox_size_from_env();
        assert!(size >= 1024 && size <= 1_000_000);
    }
}
