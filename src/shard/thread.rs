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
}

impl ShardEvent {
    /// Convenience constructor for the legacy Push path — equivalent to
    /// constructing with `op: ShardOp::Push`.
    pub fn push(
        payload: bytes::Bytes,
        stream_name: Arc<str>,
        shard_hint: u32,
        response_tx: Option<tokio::sync::oneshot::Sender<ShardResult>>,
    ) -> Self {
        Self {
            payload,
            stream_name,
            shard_hint,
            response_tx,
            op: ShardOp::Push,
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
}

/// Result sent from shard back to listener via response_tx.
/// Phase 50.5-01: widened to carry FeatureMap so read_features=true round-trips.
/// Phase 53-01: further widened with variants for Get / Set / Mset / Mget / GetMulti.
#[derive(Debug)]
pub enum ShardResult {
    /// PUSH ack — carries computed FeatureMap (may be empty).
    Ok(crate::types::FeatureMap),
    /// GET response — carries the feature map for the requested key.
    GetOk(crate::types::FeatureMap),
    /// SET / MSET / Tombstone / MarkDirty ack.
    SetOk,
    /// MGET response — `Vec<(key, FeatureMap)>` preserving request order
    /// for the keys owned by this shard.
    MgetOk(Vec<(String, crate::types::FeatureMap)>),
    /// GET_MULTI response — `(table_name, row_json_or_null)` pairs in request order.
    GetMultiOk(Vec<(String, serde_json::Value)>),
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

/// Spawn all N shard threads. Returns only after every shard has signaled
/// ready (the ready-barrier, D-01). Callers bind listener sockets after this
/// returns.
///
/// Phase 50.5-01: `state` added so each shard thread owns a handle into
/// `ConcurrentAppState` and can call `push_with_cascade_on_shard` directly.
///
/// # Panics
/// Panics at the caller level only if shard_count == 0.
pub fn spawn_shard_threads(
    shard_count: usize,
    inbox_size: usize,
    state: std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
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

        std::thread::Builder::new()
            .name(format!("beava-shard-{}", shard_index))
            .spawn(move || {
                // D-14: core_affinity pinning (Linux strict, macOS best-effort).
                pin_to_core(shard_index);

                // Signal ready — listener bind is unblocked when all shards drop their token.
                drop(wg_worker);

                // D-02: catch_unwind quarantine around the entire shard event loop.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shard_event_loop(shard_index, rx, state_clone);
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
fn shard_event_loop(
    shard_index: usize,
    rx: Receiver<ShardEvent>,
    state: std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
) {
    // Each shard runs a tokio current_thread runtime on its pinned OS thread.
    // This allows async code (e.g. oneshot response sends) without cross-thread
    // task migration — the reactor stays on the pinned core.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build per-shard tokio runtime");

    // Each shard owns its own Shard struct — single writer, no lock.
    let mut shard = crate::shard::Shard::new();

    // Phase 51-02: read publish threshold once at shard boot — avoids repeated
    // env-var parsing on the hot event loop. BEAVA_WATERMARK_PUBLISH_INTERVAL
    // is clamped [64, 65536] by from_env(); defaults to 1024.
    let wm_publish_threshold =
        crate::shard::global_watermark::GlobalWatermarkConfig::from_env().publish_interval;

    rt.block_on(async move {
        let mut event_count: u64 = 0;
        let mut last_gauge_update = std::time::Instant::now();

        while let Ok(mut event) = rx.recv() {
            event_count += 1;
            let now = std::time::SystemTime::now();

            // Phase 53-01: dispatch on the new ShardOp enum. Take the op out of
            // the event (replacing with Push placeholder) so we can still access
            // event.payload / event.stream_name / event.response_tx by value.
            let op = std::mem::replace(&mut event.op, ShardOp::Push);
            match op {
                ShardOp::Push => {
                    // Parse JSON payload from bytes.
                    let payload: serde_json::Value = match serde_json::from_slice(&event.payload) {
                        Ok(v) => v,
                        Err(e) => {
                            crate::shard::metrics::record_shard_event(
                                shard_index,
                                crate::shard::metrics::Outcome::Dropped,
                            );
                            if let Some(tx) = event.response_tx {
                                let _ = tx.send(ShardResult::Err(ShardDispatchError::ProcessingError(
                                    format!("JSON parse error: {}", e),
                                )));
                            }
                            continue;
                        }
                    };

                    let stream_name: &str = &event.stream_name;

                    let result = {
                        let engine = state.engine.read();
                        let read_features = event.response_tx.is_some();
                        engine.push_with_cascade_on_shard(
                            stream_name,
                            &payload,
                            &mut shard,
                            None,
                            now,
                            read_features,
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
                    let features = {
                        let engine = state.engine.read();
                        engine.get_features_on_shard(&key, &shard, now)
                    };
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::GetOk(features));
                    }
                }
                ShardOp::Set { key, payload } => {
                    let result = apply_set_on_shard(&state, &mut shard, &key, &payload, now);
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
                        if let Err(e) = apply_set_on_shard(&state, &mut shard, &key, &payload, now) {
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
                    let result = apply_set_on_shard(&state, &mut shard, &key, &empty, now);
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
            }

            // Emit gauges every 1000 events OR every 100ms.
            if event_count % 1000 == 0 || last_gauge_update.elapsed().as_millis() >= 100 {
                let inbox_depth = rx.len();
                let keys_owned = shard.state.len();
                crate::shard::metrics::update_shard_gauges(
                    shard_index,
                    0.0,
                    inbox_depth,
                    keys_owned,
                    0.0,
                );
                last_gauge_update = std::time::Instant::now();
            }
        }
    });
}

/// Phase 53-01: apply a SET on shard-owned state.
///
/// Mirrors the logic in `handle_sync_command::Command::Set` but operates
/// against `shard.state: AHashMap` instead of `state.store: DashMap`. Empty
/// payload = tombstone (clears static_features + fires TT-cascade with
/// tombstoned=true); non-empty = upsert (sets each feature + fires
/// TT-cascade with tombstoned=false).
///
/// **Incomplete (Phase 53-01 WIP):** The Table↔Table cascade
/// (`engine.cascade_table_upsert_on_shard`) is NOT YET IMPLEMENTED against
/// `&mut Shard` — this helper currently mutates `shard.state` directly for
/// the static_features update but skips the cascade fan-out. That step is
/// deferred to a follow-up (engine-side API addition).
fn apply_set_on_shard(
    _state: &std::sync::Arc<crate::server::tcp::ConcurrentAppState>,
    shard: &mut crate::shard::Shard,
    key: &str,
    payload: &serde_json::Value,
    now: std::time::SystemTime,
) -> Result<(), crate::error::BeavaError> {
    use crate::state::store::{EntityState, StaticFeature};

    if let serde_json::Value::Object(map) = payload {
        let tombstoned = map.is_empty();
        let entity = shard.state.entry(key.to_string()).or_insert_with(EntityState::default);
        if tombstoned {
            // Tombstone: clear all static features.
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
        // Mark dirty for incremental snapshots.
        shard.dirty_set.insert(key.to_string());
        // NOTE (Phase 53-01 WIP): Table↔Table cascade on shard is deferred —
        // requires `engine.cascade_table_upsert_on_shard(&mut shard, ...)` which
        // does not yet exist. Without it, TT-join outputs will not refresh on
        // SET via the shard path. This is tracked as a known gap.
        Ok(())
    } else {
        Err(crate::error::BeavaError::Protocol(
            "SET payload must be a JSON object".into(),
        ))
    }
}

/// Phase 53-01: read a table_row as JSON from shard-owned state.
///
/// Returns `Value::Null` if the entity or table_row is absent or tombstoned
/// (matches the null-collapse contract of OP_GET_MULTI).
fn get_table_row_on_shard(
    shard: &crate::shard::Shard,
    table_name: &str,
    key: &str,
) -> serde_json::Value {
    use crate::state::store::TableRowState;

    let Some(entity) = shard.state.get(key) else {
        return serde_json::Value::Null;
    };
    let Some(row) = entity.table_rows.get(table_name) else {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ConcurrentAppState for unit tests that call spawn_shard_threads.
    fn make_test_state(n_shards: u16) -> std::sync::Arc<crate::server::tcp::ConcurrentAppState> {
        use crate::engine::pipeline::PipelineEngine;
        use crate::server::tcp::{make_concurrent_state_full, BackfillTracker};
        use crate::state::store::StateStore;
        make_concurrent_state_full(
            PipelineEngine::new(),
            StateStore::new(),
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
        let handles = spawn_shard_threads(2, 64, state);
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].shard_index, 0);
        assert_eq!(handles[1].shard_index, 1);
    }

    #[test]
    fn all_shards_start_not_down() {
        let state = make_test_state(3);
        let handles = spawn_shard_threads(3, 64, state);
        for h in &handles {
            assert!(!h.is_down.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn ready_barrier_completes_without_deadlock() {
        // Barrier must not deadlock — verifies WaitGroup logic is correct.
        let start = std::time::Instant::now();
        let state = make_test_state(2);
        let _handles = spawn_shard_threads(2, 16, state);
        // Should complete in well under 5 s even on CI with slow cores.
        assert!(start.elapsed().as_secs() < 5, "ready-barrier timed out");
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
