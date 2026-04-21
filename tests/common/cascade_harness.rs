//! Phase 55 Wave 0/1 — shared cascade harness for RED tests.
//!
//! Wave 0 landed these helpers as `unimplemented!()` stubs. Wave 1 (plan
//! 55-01) fills in real wiring:
//!   - `spawn_two_shards(inbox_cap)` builds a two-shard fixture with a
//!     fake sibling drain thread backed by a fresh fjall keyspace.
//!   - `hash_key_to_shard` mirrors production routing
//!     (`beava::routing::shard_hint_for_event`).
//!   - `drain_sibling_inbox` services `UpsertTableRow`, `TombstoneTableRow`,
//!     and the new `UpsertTableBatch` against a local Shard.
//!
//! The returned harness is "bare" — no real EngineState wired up. RED
//! tests that exercise the full pipeline use the engine / pipeline
//! directly against `TwoShardHarness.input_shard` + `sibling_shards`.

#![allow(dead_code)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use beava::shard::Shard;
use fjall::Keyspace;
use tempfile::TempDir;

/// Two-shard fixture handle. Fields are public so test bodies can borrow
/// the input shard mutably + pass `sibling_handles` into the engine's
/// cascade surface.
pub struct TwoShardHarness {
    /// Shard 0 — the "source" shard. Tests drive cascades from here.
    pub input_shard: Shard,
    /// Handles slice — two entries (shard 0 + shard 1). Index 0's sender
    /// is a live SPSC into a thread-less drain; index 1's is the sibling.
    pub shard_handles: Vec<ShardHandle>,
    /// Drain threads for the sibling shard(s). Calling `finish()` drops
    /// the handles + senders and joins the drain thread, returning the
    /// sibling's Shard so the test can assert against it.
    drain_handle: Option<JoinHandle<Shard>>,
    /// Senders we kept for explicit drop ordering — need to drop every
    /// clone before the drain thread's `rx.recv()` returns Disconnected.
    input_sender: crossbeam_channel::Sender<ShardEvent>,
    sibling_sender: crossbeam_channel::Sender<ShardEvent>,
    /// Keyspace guard — keep fjall alive for the duration of the test.
    _keyspace: Arc<Keyspace>,
    /// TempDir guard — drop removes the on-disk keyspace at end-of-test.
    _tmp: TempDir,
}

impl TwoShardHarness {
    /// Drop the handles + senders, join the drain thread, return the
    /// sibling shard so assertions can inspect it.
    pub fn finish(mut self) -> Shard {
        // Drop every shard-handle sender clone first.
        self.shard_handles.clear();
        // Drop our retained sender handles.
        drop(self.input_sender);
        drop(self.sibling_sender);
        // Join the drain thread.
        self.drain_handle
            .take()
            .expect("drain handle present")
            .join()
            .expect("drain thread clean exit")
    }
}

/// Phase 55-01 Wave 1 implementation — build a real two-shard fixture.
///
/// Uses `ephemeral_test_keyspace(2)` to get a fresh fjall keyspace with
/// two partitions. Shard 0 is handed back as `input_shard` (mutable from
/// the test). Shard 1 is wrapped in a drain thread that services TT
/// cascade ops (`UpsertTableRow`, `TombstoneTableRow`, `UpsertTableBatch`).
pub fn spawn_two_shards(inbox_cap: usize) -> TwoShardHarness {
    let (ks, partitions, tmp, _cfg) = super::ephemeral_test_keyspace(2);
    let mut parts = partitions.into_iter();
    let input_shard = Shard::with_partition(parts.next().unwrap());
    let sibling_shard = Shard::with_partition(parts.next().unwrap());

    let (input_tx, _input_rx) = crossbeam_channel::bounded::<ShardEvent>(inbox_cap);
    let (sibling_tx, sibling_rx) = crossbeam_channel::bounded::<ShardEvent>(inbox_cap);

    let shard_handles = vec![
        ShardHandle {
            shard_index: 0,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: input_tx.clone(),
        },
        ShardHandle {
            shard_index: 1,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: sibling_tx.clone(),
        },
    ];

    let drain_handle = drain_sibling_inbox(sibling_shard, sibling_rx);

    TwoShardHarness {
        input_shard,
        shard_handles,
        drain_handle: Some(drain_handle),
        input_sender: input_tx,
        sibling_sender: sibling_tx,
        _keyspace: ks,
        _tmp: tmp,
    }
}

/// Compute the shard slot for a given key under N-way sharding. Uses the
/// same routing primitive as production ingest (`shard_hint_for_event`)
/// so harness-level shard assignments match the engine's runtime routing.
///
/// Keyed as `{ "__k": key }` so `shard_hint_for_event(.., Some("__k"))`
/// sees the full key string verbatim.
pub fn hash_key_to_shard(key: &str, n: usize) -> usize {
    let ev = serde_json::json!({ "__k": key });
    (beava::routing::shard_hint_for_event(&ev, Some("__k")) as usize) % n.max(1)
}

/// Spawn a drain thread for the sibling shard. Services TT cascade ops
/// (`UpsertTableRow`, `TombstoneTableRow`, `UpsertTableBatch`) against a
/// local `Shard`, replying `ShardResult::SetOk` on the oneshot. Exits
/// cleanly when all senders drop.
pub fn drain_sibling_inbox(
    shard: Shard,
    rx: crossbeam_channel::Receiver<ShardEvent>,
) -> JoinHandle<Shard> {
    std::thread::spawn(move || {
        let mut shard = shard;
        while let Ok(mut event) = rx.recv() {
            let op = std::mem::replace(&mut event.op, ShardOp::Push);
            match op {
                ShardOp::UpsertTableRow {
                    key,
                    table_name,
                    fields,
                    now,
                } => {
                    shard.upsert_table_row(&key, &table_name, fields, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::TombstoneTableRow {
                    key,
                    table_name,
                    now,
                } => {
                    shard.tombstone_table_row(&key, &table_name, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::UpsertTableBatch { writes, now } => {
                    for (table_name, key, fields) in writes {
                        shard.upsert_table_row(&key, &table_name, fields, now);
                    }
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                _ => {
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
            }
        }
        shard
    })
}

/// Deterministic search for a `(k1, k2)` pair whose shard slots differ
/// under N-way sharding. At N=2 the first few candidates hit (~50%
/// probability per pair); loop bounds are generous for N up to 64.
pub fn pick_two_keys_hashing_to_different_shards(n: usize) -> (String, String) {
    for i in 0u32..4096 {
        for j in 0u32..4096 {
            let k1 = format!("k{i:04}");
            let k2 = format!("m{j:04}");
            if hash_key_to_shard(&k1, n) != hash_key_to_shard(&k2, n) {
                return (k1, k2);
            }
        }
    }
    panic!("pick_two_keys_hashing_to_different_shards: no pair found at n={n}");
}

/// Deterministic search for a `(k1, k2)` pair whose shard slots are
/// equal under N-way sharding. Companion to the split-pair finder above;
/// used by the same-shard fast-path RED test.
pub fn pick_two_keys_hashing_to_same_shard(n: usize) -> (String, String) {
    for i in 0u32..4096 {
        for j in 0u32..4096 {
            let k1 = format!("u{i:04}");
            let k2 = format!("v{j:04}");
            if hash_key_to_shard(&k1, n) == hash_key_to_shard(&k2, n) {
                return (k1, k2);
            }
        }
    }
    panic!("pick_two_keys_hashing_to_same_shard: no pair found at n={n}");
}

// ---------------------------------------------------------------------------
// Minimal engine builder: Txn (shard_key=user_id) → MerchantActivity (key=
// merchant_id). Lifted from tests/cross_shard_tt_cascade.rs and repurposed
// so Wave 1 SC-1 tests can flip GREEN without forking the fixture.
// ---------------------------------------------------------------------------

use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};

/// Build an engine for SC-1 tests: primary Txn Table (keyed by user_id)
/// plus `MerchantActivity = TableTableJoin(Txn, Txn)` keyed by
/// merchant_id. Uses Txn as both left and right of an INNER join so a
/// single primary PUSH fires exactly one cascade output per matching
/// merchant_id (the production case has a distinct right Table but for
/// ownership-test purposes this scheme is sufficient).
pub fn make_tt_cascade_engine() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    engine
        .register(StreamDefinition {
            name: "Txn".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: Vec::new(),
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
            salt: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            group_by_keys: None,
            features: vec![(
                "joined".into(),
                FeatureDef::TableTableJoin {
                    left_table: "Txn".into(),
                    right_table: "Txn".into(),
                    on: vec!["user_id".into()],
                    join_type: JoinType::Inner,
                    left_fields: vec!["amount".into()],
                    right_fields: vec![("user_id".into(), "user_id".into())],
                },
            )],
            depends_on: Some(vec!["Txn".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
            salt: None,
        })
        .unwrap();

    engine
}

/// Seed a Txn table row into `shard` at `user_id` carrying `amount`.
pub fn seed_txn_row(shard: &mut Shard, user_id: &str, amount: i64) {
    use ahash::AHashMap;
    use beava::shard::StoreView;
    use beava::types::FeatureValue;
    let mut fields = AHashMap::new();
    fields.insert("amount".into(), FeatureValue::Int(amount));
    fields.insert("user_id".into(), FeatureValue::String(user_id.into()));
    let mut view = StoreView::Sharded(shard);
    view.upsert_table_row(user_id, "Txn", fields, std::time::SystemTime::now());
}
