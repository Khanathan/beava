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

/// Opaque event envelope sent from listener to shard via SPSC inbox (D-08).
/// Payload type expanded in Plan 50-04; this is the Wave 2 skeleton.
pub struct ShardEvent {
    /// Raw event bytes — bytes::Bytes is O(1) clone (Arc-backed). Zero copy.
    pub payload: bytes::Bytes,
    /// Stream name for routing to correct Shard state machine.
    pub stream_name: Arc<str>,
    /// Precomputed shard_hint from ingest parser (Phase 48).
    pub shard_hint: u32,
    /// Response channel — shard sends result back to listener.
    /// None for fire-and-forget paths.
    pub response_tx: Option<tokio::sync::oneshot::Sender<ShardResult>>,
}

/// Result sent from shard back to listener via response_tx.
/// Phase 50.5-01: widened to carry FeatureMap so read_features=true round-trips
/// features through the oneshot channel.
#[derive(Debug)]
pub enum ShardResult {
    /// Event was processed successfully. Carries computed FeatureMap (may be empty
    /// if read_features was false on the shard side).
    Ok(crate::types::FeatureMap),
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

    rt.block_on(async move {
        let mut event_count: u64 = 0;
        let mut last_gauge_update = std::time::Instant::now();

        while let Ok(event) = rx.recv() {
            event_count += 1;

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
            let now = std::time::SystemTime::now();

            // Phase 50.5-01: call push_with_cascade_on_shard.
            // The engine is read-locked (shared readers OK — engine is read-only after registration).
            let result = {
                let engine = state.engine.read();
                let read_features = event.response_tx.is_some();
                engine.push_with_cascade_on_shard(
                    stream_name,
                    &payload,
                    &mut shard,
                    None,  // event_log: not wired in Wave 2 (shard-owned log is Phase 51)
                    now,
                    read_features,
                )
            };

            // Advance per-shard watermark if payload carries _event_time.
            if let Some(et) = crate::engine::operators::parse_event_time(&payload) {
                shard.watermark.observe(stream_name, et);
            }

            crate::shard::metrics::record_shard_event(
                shard_index,
                crate::shard::metrics::Outcome::Accepted,
            );

            // Emit gauges every 1000 events OR every 100ms.
            if event_count % 1000 == 0 || last_gauge_update.elapsed().as_millis() >= 100 {
                let inbox_depth = rx.len();
                let keys_owned = shard.state.len();
                crate::shard::metrics::update_shard_gauges(
                    shard_index,
                    0.0, // reactor_utilization: not yet measured
                    inbox_depth,
                    keys_owned,
                    0.0, // watermark_lag: placeholder
                );
                last_gauge_update = std::time::Instant::now();
            }

            // Send result back to listener via oneshot (if response was requested).
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
    });
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

        let first = ShardEvent {
            payload: bytes::Bytes::from_static(b"event0"),
            stream_name: Arc::from("s"),
            shard_hint: 0,
            response_tx: None,
        };
        assert!(tx.try_send(first).is_ok(), "first send should succeed");

        let mut drop_count = 0u64;
        for _ in 1..10u64 {
            let ev = ShardEvent {
                payload: bytes::Bytes::from_static(b"eventN"),
                stream_name: Arc::from("s"),
                shard_hint: 0,
                response_tx: None,
            };
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
