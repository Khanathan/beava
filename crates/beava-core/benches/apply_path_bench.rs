//! Plan 19.2-08 (D-08): apply-path criterion bench.
//!
//! Four bench groups measuring the stacked Phase 19.2 lift:
//!   1. `apply_path/cold_key` — fresh AggStateTable + new entity → measures full per-event init cost
//!   2. `apply_path/warm_key` — pre-warmed entity → measures steady-state apply cost
//!   3. `apply_path/uddsketch` — direct UDDSketch::insert/quantile (Plan 19.2-04 flat sorted Vec)
//!   4. `apply_path/event_type_mix` — direct EventTypeMixState::update with 1024-category allowlist (Plan 19.2-05 AHashSet)
//!
//! All four groups run via:
//!   cargo bench -p beava-core --bench apply_path_bench
//!
//! Per CLAUDE.md §Performance Discipline (enforced Phase 6+).
//! Bench numbers appended to .planning/perf-baselines.md under
//! "## hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores" section.

use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_buffer::EventTypeMixState;
use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
use beava_core::agg_op::{AggExtParams, AggKind, AggOpDescriptor, SketchParams, FIELD_IDX_NONE};
use beava_core::agg_state_table::new_state_tables_for;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
use beava_core::registry_diff::PayloadNode;
use beava_core::row::{Row, Value};
use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
use beava_core::sketches::uddsketch::UDDSketch;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::collections::BTreeMap;
use std::sync::Arc;

// ─── Synthetic fraud-team-shape registry builder ──────────────────────────────

/// Build a fraud-team-shape synthetic registry: 14 aggregation features across
/// ~3 unique group_keys signatures, mimicking the real fraud-team.json shape.
///
/// Concrete layout (14 features, 3 clusters):
///   Cluster A — group_keys=[user_id]: 7 features (count, sum, percentile, stddev|ewma*, top_k, entropy, min|event_type_mix*)
///   Cluster B — group_keys=[user_id, merchant]: 4 features (count, sum, count_distinct, count_distinct)
///   Cluster C — group_keys=[device_id]: 3 features (count, sum, max|bloom_member*)
///
/// (* In the windowed variant of this registry — see Plan 19.3-01 / RESEARCH.md
/// §2 Q3 — three of the original features are non-windowable per
/// `agg_windowed.rs:464,473`: `BloomMember`, `EventTypeMix`, `Ewma`. They are
/// substituted with windowable Tier-1 kinds: `StdDev` (replaces Ewma), `Min`
/// (replaces EventTypeMix), `Max` (replaces BloomMember). Total feature count
/// stays at 14 to preserve `14_aggs` ↔ `14_aggs_windowed` naming parity for
/// the criterion comparison.)
///
/// This is a SYNTHETIC stand-in for the real fraud-team.json (the throughput
/// rebaseline in Task 2.b drives the actual config end-to-end). The bench
/// exercises plan 19.2-01/02/03/04/05 code paths without requiring the server.
fn build_fraud_team_synthetic_registry() -> Arc<Registry> {
    build_fraud_team_synthetic_registry_inner(None)
}

/// Windowed sibling of [`build_fraud_team_synthetic_registry`] — every feature
/// is wrapped in `WindowedOp(window_ms)`. Used by the
/// `apply_path/warm_key/14_aggs_windowed` criterion group (Plan 19.3-01) to
/// measure the slow WindowedOp dispatch path that Plan 19.3-02 will optimize.
fn build_fraud_team_synthetic_registry_windowed(window_ms: u64) -> Arc<Registry> {
    build_fraud_team_synthetic_registry_inner(Some(window_ms))
}

fn build_fraud_team_synthetic_registry_inner(window_ms: Option<u64>) -> Arc<Registry> {
    let registry = Arc::new(Registry::new());

    // ── Event schema: Txn with 10 fields ────────────────────────────────────
    let mut fields = BTreeMap::new();
    fields.insert("user_id".to_string(), FieldType::Str);
    fields.insert("device_id".to_string(), FieldType::Str);
    fields.insert("merchant".to_string(), FieldType::Str);
    fields.insert("amount".to_string(), FieldType::F64);
    fields.insert("status".to_string(), FieldType::Str);
    fields.insert("category".to_string(), FieldType::Str);
    fields.insert("event_type".to_string(), FieldType::Str);

    let event = EventDescriptor {
        name: "Txn".to_string(),
        schema: EventSchema {
            fields,
            optional_fields: vec![],
        },
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: None,
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };

    // ── Cluster A: group_keys=[user_id], 7 features ─────────────────────────
    let cluster_a_features: Vec<(&str, AggOpDescriptor)> = vec![
        (
            "txn_count",
            AggOpDescriptor {
                kind: AggKind::Count,
                field: None,
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "txn_amount_sum",
            AggOpDescriptor {
                kind: AggKind::Sum,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "txn_amount_p50",
            AggOpDescriptor {
                kind: AggKind::Percentile,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: Some(SketchParams {
                    percentile_q: Some(0.5),
                    top_k_k: None,
                    bloom_capacity: None,
                    bloom_fpr: None,
                }),
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            // Plan 19.3-01: Ewma → StdDev (Ewma is non-windowable; see RESEARCH §2 Q3).
            "txn_amount_stddev",
            AggOpDescriptor {
                kind: AggKind::StdDev,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "txn_merchant_top_k",
            AggOpDescriptor {
                kind: AggKind::TopK,
                field: Some("merchant".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: Some(SketchParams {
                    percentile_q: None,
                    top_k_k: Some(10),
                    bloom_capacity: None,
                    bloom_fpr: None,
                }),
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "txn_category_entropy",
            AggOpDescriptor {
                kind: AggKind::Entropy,
                field: Some("category".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams {
                    max_categories: Some(1024),
                    ..AggExtParams::default()
                },
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            // Plan 19.3-01: EventTypeMix → Min (EventTypeMix is non-windowable; see RESEARCH §2 Q3).
            "txn_amount_min",
            AggOpDescriptor {
                kind: AggKind::Min,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
    ];

    let agg_a = AggregationDescriptor {
        node_name: "UserFeatures".to_string(),
        source_node_name: "Txn".to_string(),
        group_keys: vec!["user_id".to_string()],
        features: cluster_a_features
            .into_iter()
            .map(|(name, desc)| NamedAggOp {
                feature_name: name.to_string(),
                descriptor: desc,
            })
            .collect(),
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    };

    // ── Cluster B: group_keys=[user_id, merchant], 4 features ───────────────
    let cluster_b_features: Vec<(&str, AggOpDescriptor)> = vec![
        (
            "user_merchant_count",
            AggOpDescriptor {
                kind: AggKind::Count,
                field: None,
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "user_merchant_amount_sum",
            AggOpDescriptor {
                kind: AggKind::Sum,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "user_merchant_category_distinct",
            AggOpDescriptor {
                kind: AggKind::CountDistinct,
                field: Some("category".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "user_merchant_status_distinct",
            AggOpDescriptor {
                kind: AggKind::CountDistinct,
                field: Some("status".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
    ];

    let agg_b = AggregationDescriptor {
        node_name: "UserMerchantFeatures".to_string(),
        source_node_name: "Txn".to_string(),
        group_keys: vec!["user_id".to_string(), "merchant".to_string()],
        features: cluster_b_features
            .into_iter()
            .map(|(name, desc)| NamedAggOp {
                feature_name: name.to_string(),
                descriptor: desc,
            })
            .collect(),
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    };

    // ── Cluster C: group_keys=[device_id], 3 features ────────────────────────
    let cluster_c_features: Vec<(&str, AggOpDescriptor)> = vec![
        (
            "device_count",
            AggOpDescriptor {
                kind: AggKind::Count,
                field: None,
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            "device_amount_sum",
            AggOpDescriptor {
                kind: AggKind::Sum,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
        (
            // Plan 19.3-01: BloomMember → Max (BloomMember is non-windowable; see RESEARCH §2 Q3).
            "device_amount_max",
            AggOpDescriptor {
                kind: AggKind::Max,
                field: Some("amount".to_string()),
                window_ms,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: AggExtParams::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        ),
    ];

    let agg_c = AggregationDescriptor {
        node_name: "DeviceFeatures".to_string(),
        source_node_name: "Txn".to_string(),
        group_keys: vec!["device_id".to_string()],
        features: cluster_c_features
            .into_iter()
            .map(|(name, desc)| NamedAggOp {
                feature_name: name.to_string(),
                descriptor: desc,
            })
            .collect(),
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    };

    // ── Derivations ──────────────────────────────────────────────────────────
    let make_deriv = |name: &str, source: &str| DerivationDescriptor {
        name: name.to_string(),
        output_kind: OutputKind::Table,
        upstreams: vec![source.to_string()],
        ops: vec![],
        schema: DerivedSchema {
            fields: BTreeMap::new(),
            optional_fields: vec![],
        },
        table_primary_key: None,
        registered_at_version: 0,
    };

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_deriv("UserFeatures", "Txn")),
            PayloadNode::Derivation(make_deriv("UserMerchantFeatures", "Txn")),
            PayloadNode::Derivation(make_deriv("DeviceFeatures", "Txn")),
        ],
        vec![],
        vec![],
        vec![
            ("UserFeatures".to_string(), Arc::new(agg_a)),
            ("UserMerchantFeatures".to_string(), Arc::new(agg_b)),
            ("DeviceFeatures".to_string(), Arc::new(agg_c)),
        ],
    );

    registry
}

/// Build a synthetic Txn row with all 7 fields populated.
fn build_fraud_team_synthetic_row() -> Row {
    Row::new()
        .with_field("user_id", Value::Str("user_42".into()))
        .with_field("device_id", Value::Str("dev_7".into()))
        .with_field("merchant", Value::Str("acme_corp".into()))
        .with_field("amount", Value::F64(123.45))
        .with_field("status", Value::Str("approved".into()))
        .with_field("category", Value::Str("electronics".into()))
        .with_field("event_type", Value::Str("purchase".into()))
}

/// Build a single-field row with a category value.
fn make_row_with_category(cat: &str) -> Row {
    Row::new().with_field("category", Value::Str(cat.into()))
}

// ─── Bench group 1: cold-key 14-agg apply ────────────────────────────────────

fn bench_apply_cold_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_path/cold_key");
    group.sample_size(50);

    let registry = build_fraud_team_synthetic_registry();
    let row = build_fraud_team_synthetic_row();

    group.bench_function("14_aggs", |b| {
        b.iter_batched(
            || new_state_tables_for(&registry),
            |mut state_tables| {
                apply_event_to_aggregations(
                    black_box("Txn"),
                    black_box(&row),
                    black_box(1_714_000_000_000_i64),
                    black_box(0_u64),
                    black_box(&registry),
                    black_box(&mut state_tables),
                    black_box(None),
                );
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ─── Bench group 2: warm-key 14-agg apply ────────────────────────────────────

/// Build a Txn row with parametrized category/status values so CountDistinct
/// features promote past the EXACT_THRESHOLD (16) → HashSet mode during
/// pre-warm. Plan 19.4-01 measurement-gate fix (Rule 1 deviation): the prior
/// pre-warm reused a single row, leaving CountDistinct in ExactArray mode
/// where the SipHash-vs-identity-hasher difference cannot manifest — so the
/// criterion bench could not validate the optimization that motivated this
/// plan. Varying `category`/`status` across pre-warm pushes both
/// CountDistinct features (lines ~276/292: user_merchant_category_distinct,
/// user_merchant_status_distinct) into HashSet mode where the identity-hasher
/// lookup-cost lift IS measurable.
fn build_fraud_team_synthetic_row_varied(seed: u64) -> Row {
    let cat_idx = seed % 64; // 64 distinct categories → > HASH_THRESHOLD pre-warm reaches HashSet mode
    let stat_idx = seed % 32; // 32 distinct status values → also pushes past EXACT_THRESHOLD (16)
    Row::new()
        .with_field("user_id", Value::Str("user_42".into()))
        .with_field("device_id", Value::Str("dev_7".into()))
        .with_field("merchant", Value::Str("acme_corp".into()))
        .with_field("amount", Value::F64(123.45))
        .with_field("status", Value::Str(format!("status_{}", stat_idx).into()))
        .with_field("category", Value::Str(format!("cat_{}", cat_idx).into()))
        .with_field("event_type", Value::Str("purchase".into()))
}

fn bench_apply_warm_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_path/warm_key");
    group.sample_size(100);

    let registry = build_fraud_team_synthetic_registry();
    let mut state_tables = new_state_tables_for(&registry);

    // Pre-warm: drive 1500 varied events so all per-entity init costs are
    // amortized AND CountDistinct features reach HashSet mode (>16 distinct
    // values per CountDistinct field). The fixed measurement row is then a
    // hash-already-present lookup in HashSet mode — the hot path the Plan
    // 19.4-01 identity hasher optimizes.
    for i in 0..1500_i64 {
        let varied = build_fraud_team_synthetic_row_varied(i as u64);
        apply_event_to_aggregations(
            "Txn",
            &varied,
            1_714_000_000_000 + i,
            i as u64,
            &registry,
            &mut state_tables,
            None,
        );
    }
    let row = build_fraud_team_synthetic_row();

    group.bench_function("14_aggs", |b| {
        let mut ts: i64 = 1_714_000_001_500;
        let mut eid: u64 = 1500;
        b.iter(|| {
            apply_event_to_aggregations(
                black_box("Txn"),
                black_box(&row),
                black_box(ts),
                black_box(eid),
                black_box(&registry),
                black_box(&mut state_tables),
                black_box(None),
            );
            ts += 1;
            eid += 1;
        });
    });

    // Plan 19.3-01 (D-04): windowed sibling — same 14-feature shape, every
    // feature wrapped in WindowedOp(window_ms = 24h). Measures the slow
    // WindowedOp dispatch path that bypasses Plan 19.2-01's pre-extraction
    // protocol (per .planning/phases/19.2-big-apply-path-optimization/
    // 19.2-INVESTIGATION.md). Plan 19.3-02's `WindowedOp::update_at` fast-path
    // must drop this baseline ≥ 4×.
    //
    // Plan 19.4-01 (Rule 1 deviation): pre-warm now uses varied rows so
    // CountDistinct features reach HashSet mode where the identity-hasher
    // optimization is observable.
    let registry_w = build_fraud_team_synthetic_registry_windowed(86_400_000);
    let mut state_tables_w = new_state_tables_for(&registry_w);
    for i in 0..1500_i64 {
        let varied = build_fraud_team_synthetic_row_varied(i as u64);
        apply_event_to_aggregations(
            "Txn",
            &varied,
            1_714_000_000_000 + i,
            i as u64,
            &registry_w,
            &mut state_tables_w,
            None,
        );
    }
    group.bench_function("14_aggs_windowed", |b| {
        let mut ts: i64 = 1_714_000_001_500;
        let mut eid: u64 = 1500;
        b.iter(|| {
            apply_event_to_aggregations(
                black_box("Txn"),
                black_box(&row),
                black_box(ts),
                black_box(eid),
                black_box(&registry_w),
                black_box(&mut state_tables_w),
                black_box(None),
            );
            ts += 1;
            eid += 1;
        });
    });
    group.finish();
}

// ─── Bench group 3: UDDSketch storage (Plan 19.2-04 flat sorted Vec) ─────────

fn bench_uddsketch_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_path/uddsketch");

    // Pre-fill 1000 inserts to hit the post-promotion bucket layout (~11-level
    // bucket occupancy). Bench measures one additional insert at steady state.
    let mut warm_sketch = UDDSketch::default();
    for i in 1..=1000 {
        warm_sketch.insert(i as f64);
    }

    group.bench_function("insert_warm", |b| {
        b.iter(|| {
            let mut s = warm_sketch.clone();
            s.insert(black_box(500.5));
            black_box(s);
        });
    });

    group.bench_function("quantile_warm", |b| {
        b.iter(|| {
            let s = warm_sketch.clone();
            let q = s.quantile(black_box(0.5));
            black_box(q);
        });
    });
    group.finish();
}

// ─── Bench group 4: EventTypeMix allowlist Vec → AHashSet (Plan 19.2-05) ─────

fn bench_event_type_mix_allowlist(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_path/event_type_mix");

    // Construct EventTypeMixState with a 1024-category allowlist.
    // This exercises Plan 19.2-05's AHashSet O(1) contains check.
    let allowed: Vec<String> = (0..1024).map(|i| format!("cat_{}", i)).collect();
    let mut state_hit = EventTypeMixState::new(2048, Some(allowed.clone()));
    let mut state_miss = EventTypeMixState::new(2048, Some(allowed));

    let row_hit = make_row_with_category("cat_500");
    let row_miss = make_row_with_category("not_in_allowlist");

    group.bench_function("allowed_hit", |b| {
        b.iter(|| {
            state_hit.update(black_box(&row_hit), Some("category"), true);
        });
    });

    group.bench_function("allowed_miss", |b| {
        b.iter(|| {
            state_miss.update(black_box(&row_miss), Some("category"), true);
        });
    });

    group.finish();
}

// ─── Criterion wiring ─────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_apply_cold_key,
    bench_apply_warm_key,
    bench_uddsketch_storage,
    bench_event_type_mix_allowlist
);
criterion_main!(benches);
