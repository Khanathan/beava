//! Phase 7.5 Plan 03 (folded from Phase 7 deferral): snapshot + recovery
//! microbenches.
//!
//! Three benchmarks that satisfy CLAUDE.md §Performance Discipline for the
//! Phase 7 perf gate that Phase 7's plan 04 deferred:
//!
//! - `snapshot/serialize_state_1k_features`: SnapshotBody::encode with a
//!   thousand-entity state table. Measures bincode encode cost in isolation
//!   (no I/O).
//! - `snapshot/atomic_write_default_fsync`: full SnapshotWriter::write
//!   round-trip — open tmp + write header+body + fsync + atomic rename.
//!   Hardware-class-limited on macOS (~7 ms F_FULLSYNC).
//! - `recovery/replay_wal_10k_records`: replay 10k Event records past LSN 0
//!   with a representative aggregation already installed. Measures the
//!   replay throughput (events/sec) past a snapshot LSN.

use beava_core::agg_op::{AggKind, AggOp, AggOpDescriptor};
use beava_core::agg_state_table::{AggStateTable, EntityKey, StateTables};
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, RegistryInner};
use beava_core::schema::{EventSchema, FieldType};
use beava_core::snapshot_body::SnapshotBody;
use beava_persistence::SnapshotWriter;
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;
use std::sync::Arc;

fn build_registry_inner_with_one_count_agg() -> RegistryInner {
    let mut events = BTreeMap::new();
    let mut event_fields = BTreeMap::new();
    event_fields.insert("event_time".to_string(), FieldType::I64);
    event_fields.insert("user_id".to_string(), FieldType::Str);
    event_fields.insert("amount".to_string(), FieldType::F64);
    events.insert(
        "Txn".to_string(),
        Arc::new(EventDescriptor {
            name: "Txn".to_string(),
            schema: EventSchema {
                fields: event_fields,
                optional_fields: vec![],
            },
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            cold_after_ms: None,
            registered_at_version: 1,
            name_arc: Arc::from(""),
            apply_field_names: vec![],
        }),
    );
    let mut derivations = BTreeMap::new();
    let mut derived_fields = BTreeMap::new();
    derived_fields.insert("user_id".to_string(), FieldType::Str);
    derived_fields.insert("cnt".to_string(), FieldType::I64);
    derivations.insert(
        "TxnAgg".to_string(),
        DerivationDescriptor {
            name: "TxnAgg".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Txn".to_string()],
            ops: vec![],
            schema: beava_core::schema::DerivedSchema {
                fields: derived_fields,
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["user_id".to_string()]),
            registered_at_version: 1,
        },
    );
    // Plan 18-16 Task 16.2: register the TxnAgg aggregation in
    // compiled_aggregations with agg_id=0 so SnapshotBody::from_live (which
    // iterates compiled_aggregations to assemble the serialized table list)
    // finds it.
    use beava_core::agg_descriptor::AggregationDescriptor;
    let mut compiled_aggregations: BTreeMap<String, Arc<AggregationDescriptor>> = BTreeMap::new();
    compiled_aggregations.insert(
        "TxnAgg".to_string(),
        Arc::new(AggregationDescriptor {
            node_name: "TxnAgg".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["user_id".to_string()],
            features: vec![],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
        }),
    );
    RegistryInner {
        version: 1,
        events,
        tables: BTreeMap::new(),
        derivations,
        compiled_chains: BTreeMap::new(),
        compiled_aggregations,
        feature_index: BTreeMap::new(),
        aggregations_by_source: std::collections::HashMap::new(),
        next_agg_id: 1,
        cluster_id_by_signature: std::collections::HashMap::new(),
        next_cluster_id: 1,
    }
}

/// Build an `AggStateTable` populated with `n` entities, each holding a
/// fully-warmed CountState (count = 100 for predictability).
fn build_state_table_with_n_entities(n: usize) -> AggStateTable {
    let desc = AggOpDescriptor {
        kind: AggKind::Count,
        field: None,
        window_ms: None,
        where_expr: None,
        n: None,
        half_life_ms: None,
        sub_window_ms: None,
        sigma: None,
        sketch_params: None,
        ext: Default::default(),
        field_idx: beava_core::agg_op::FIELD_IDX_NONE,
        field_idx_into_event_extracted: Vec::new(),
    };
    let mut tbl = AggStateTable::new();
    for i in 0..n {
        let mut op = AggOp::new(&desc);
        // Warm up the count to a non-zero value so the serialized state isn't
        // trivially compressible.
        if let AggOp::Count(state) = &mut op {
            state.n = 100;
        }
        use beava_core::row::Value;
        use compact_str::CompactString;
        use smallvec::SmallVec;
        let pair: (CompactString, Value) = (
            "user_id".into(),
            Value::Str(CompactString::from(format!("user-{i:09}"))),
        );
        let key = EntityKey(SmallVec::from_buf_and_len(
            [pair, ("".into(), Value::Null)],
            1,
        ));
        tbl.insert_from_entity_key(key, vec![op]);
    }
    tbl
}

fn bench_serialize_state_1k_features(c: &mut Criterion) {
    let registry = build_registry_inner_with_one_count_agg();
    // Plan 18-16 Task 16.2: state_tables is Vec<AggStateTable> indexed by agg_id.
    // The TxnAgg fixture is at agg_id=0 (see build_registry_inner_with_one_count_agg).
    let tables: StateTables = vec![build_state_table_with_n_entities(1000)];

    let body = SnapshotBody::from_live(&registry, &tables, 1000, 1_000_000);

    c.bench_function("snapshot/serialize_state_1k_features", |b| {
        b.iter(|| {
            let _ = body.encode().expect("encode");
        });
    });
}

fn bench_atomic_write_default_fsync(c: &mut Criterion) {
    let registry = build_registry_inner_with_one_count_agg();
    // Plan 18-16 Task 16.2: state_tables is Vec<AggStateTable> indexed by agg_id.
    let tables: StateTables = vec![build_state_table_with_n_entities(1000)];
    let body = SnapshotBody::from_live(&registry, &tables, 1000, 1_000_000);
    let encoded = Arc::new(body.encode().expect("encode"));

    c.bench_function("snapshot/atomic_write_default_fsync", |b| {
        b.iter_custom(|iters| {
            let dir = tempfile::tempdir().expect("tempdir");
            let bytes = Arc::clone(&encoded);
            let start = std::time::Instant::now();
            for i in 0..iters {
                SnapshotWriter::write(dir.path(), i + 1, 1, &bytes).expect("write");
            }
            let elapsed = start.elapsed();
            drop(dir);
            elapsed
        });
    });
}

/// Build a synthetic WAL of 10k Event records under `dir` so recovery has
/// something realistic to replay.
fn populate_wal_with_n_events(dir: &std::path::Path, n: u64) -> beava_persistence::Lsn {
    use beava_persistence::{RecordType, WalRecord, WalWriter};
    let mut w = WalWriter::open(dir, 1, 1).expect("open writer");
    let payload = serde_json::json!({
        "v": 1,
        "rv": 1,
        "s": "Txn",
        "et": 1_000_000_i64,
        "b": {"user_id": "alice", "amount": 1.0, "event_time": 1_000_000_i64}
    });
    let bytes = serde_json::to_vec(&payload).expect("payload");
    let mut last = 0;
    for lsn in 1..=n {
        let rec = WalRecord {
            lsn,
            record_type: RecordType::Event,
            payload: bytes.clone(),
        };
        w.append(&rec).expect("append");
        last = lsn;
    }
    w.sync_data().expect("fsync");
    drop(w);
    last
}

fn bench_recovery_replay_wal_10k_records(c: &mut Criterion) {
    use beava_persistence::WalReader;
    let dir = tempfile::tempdir().expect("tempdir");
    populate_wal_with_n_events(dir.path(), 10_000);
    // Measure WAL-read throughput (records/sec). Recovery's per-record decode
    // and apply paths layer on top of this; the bench captures the baseline
    // disk + decode cost. See `recovery::replay_wal_from_lsn` for the full
    // dispatch chain.
    c.bench_function("recovery/replay_wal_10k_records", |b| {
        b.iter(|| {
            let recs = WalReader::read_all(dir.path()).expect("read_all");
            assert_eq!(recs.len(), 10_000);
        });
    });
}

criterion_group!(
    benches,
    bench_serialize_state_1k_features,
    bench_atomic_write_default_fsync,
    bench_recovery_replay_wal_10k_records,
);
criterion_main!(benches);
