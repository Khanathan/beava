//! Phase 54-00 Task 3 — Replica ingest routing RED test (drives Wave 1 Task 3).
//!
//! Protects against the "silent regression" flagged in 54-RESEARCH.md Risk #3:
//! when Wave 1 rewires the N=1 hot path through the shard thread,
//! `PipelineEngine::push_internal_on_shard` will be the mutation path — but
//! currently it has NO `notify_subscribers` call. Without a parallel hook on
//! the shard path, every live `OP_SUBSCRIBE` session goes silent.
//!
//! **Scope deviation from plan text:** The plan proposes testing at N=1, but
//! at Phase 53 HEAD N=1 still uses the LEGACY `push_with_cascade_no_features`
//! path which DOES call `notify_subscribers` (pipeline.rs:1198). So an N=1
//! test would pass today. The real-today RED condition is at N>1, where the
//! shard path IS live: a subscriber registered for shard-owned keys NEVER
//! sees events because `push_internal_on_shard` (pipeline.rs:1939) skips the
//! notify hook. We test at N=2. After Wave 1 deletes the legacy path, this
//! test also guards N=1.
//!
//! Test command: `cargo test --release --test replica_ingest_routing`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::Scope;
use beava::server::replica::{ReplicaEvent, SubscriberRegistry};
use beava::server::signals::SignalRegistry;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

const TEST_ADMIN: &str = "test-admin-54-00-replica-routing";

use std::sync::OnceLock;
static REGISTRY_MAP: OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<SubscriberRegistry>>>,
> = OnceLock::new();

fn build_two_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-54-00-replica-{tag}.snapshot")),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        2,
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "replica_stream".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Build and wire a SubscriberRegistry.
    let signals = SignalRegistry::new_default().into_shared();
    let registry = Arc::new(SubscriberRegistry::new(signals));
    state
        .engine
        .write()
        .install_subscribers(Arc::clone(&registry));

    *state.shard_handles.write() =
        beava::shard::thread::spawn_shard_threads(2, 65_536, state.clone());
    beava::server::shard_probe::init_route_counters(2);

    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(2);

    REGISTRY_MAP
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap()
        .insert(tag.to_string(), registry);

    state
}

fn get_registry(tag: &str) -> Arc<SubscriberRegistry> {
    REGISTRY_MAP
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .get(tag)
        .cloned()
        .expect("registry registered for this tag")
}

fn scope_for(streams: &[&str]) -> Scope {
    Scope {
        streams: streams.iter().map(|s| s.to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "eager".to_string(),
    }
}

/// At N>1, a push that transits the shard-thread path MUST fire
/// `notify_subscribers` so live `OP_SUBSCRIBE` sessions receive the event.
///
/// Phase 53 HEAD: FAILS. `push_internal_on_shard` (src/engine/pipeline.rs:1939)
/// has NO `notify_subscribers` call. Subscribers silently miss every event at
/// N>1.
///
/// Phase 54-01 Task 3 (Wave 1 GREEN): PASSES. The notify hook is mirrored on
/// the shard path (parallel to `push_internal`'s hook at pipeline.rs:1198).
#[tokio::test]
async fn replica_push_fires_notify_on_shard_path() {
    let tag = "shard_notify";
    let state = build_two_shard_state(tag);
    let registry = get_registry(tag);

    // Register a subscriber session scoped to `replica_stream`.
    let (tx, mut rx) = mpsc::channel::<ReplicaEvent>(64);
    let _conn_id = registry.register(scope_for(&["replica_stream"]), tx);

    // Push 20 events with diverse user_ids so both shards receive some.
    let now = std::time::SystemTime::now();
    for i in 0..20u32 {
        let user_id = format!("repu_{:04}", i);
        let payload = serde_json::json!({ "user_id": user_id, "amount": i });

        // At N>1, handle_push_core_ex routes to the shard via SPSC.
        let _ = beava::server::tcp::handle_push_core_ex(
            &state,
            "replica_stream",
            &payload,
            &[],
            now,
            false,
            None,
        );
    }

    // Give the shard threads time to drain SPSC inboxes and process events.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Drain the subscriber channel with a bounded timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
    let mut received: Vec<ReplicaEvent> = Vec::new();
    while received.len() < 20 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(ev)) => received.push(ev),
            Ok(None) => break,
            Err(_) => {} // timeout; retry until deadline
        }
    }

    assert!(
        !received.is_empty(),
        "TPC-ARCH-01 (replica silent-regression guard): 0 of 20 events reached the \
         subscriber session. At N=2, the shard-thread mutation path \
         (src/engine/pipeline.rs::push_internal_on_shard) does NOT call \
         `notify_subscribers` — live OP_SUBSCRIBE sessions silently miss every \
         event. Wave 1 plan 54-01 Task 3 must port the notify hook to the shard \
         path (parallel to push_internal's hook at pipeline.rs:1198)."
    );
}
