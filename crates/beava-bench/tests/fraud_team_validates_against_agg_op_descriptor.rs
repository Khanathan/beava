//! Plan 19.1-02 — fraud-team.json validation regression test.
//!
//! Reads `crates/beava-bench/configs/fraud-team.json`, deserializes the
//! `register.nodes` array as `Vec<PayloadNode>`, runs the same
//! `register_validate::validate_payload` pass that `POST /register` runs, and
//! asserts the validation succeeds with zero errors.
//!
//! This is the regression guard for the realistic fraud-team primary tuning
//! benchmark (per memory `project_fraud_team_primary_bench`) — without this
//! test, fraud-team.json could silently drift away from `AggOpDescriptor`
//! schemas and break Plans 19.1-04 (lazy buckets) / 19.1-05 (re-baseline
//! matrix) downstream.
//!
//! See `.planning/phases/19.1-realistic-bench-rebaseline/19.1-02-PLAN.md`
//! for the audit list (D-10) and the rationale (D-11/12/15).

use beava_core::agg_op::AggOp;
use beava_core::register_validate::{validate_payload, ValidationError};
use beava_core::registry::{Registry, RegistryInner};
use beava_core::registry_diff::PayloadNode;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[test]
fn test_fraud_team_registers_clean() {
    // Resolve fraud-team.json path from CARGO_MANIFEST_DIR so the test runs
    // from any cwd (workspace root, crate root, IDE, CI).
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest.join("configs/fraud-team.json");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("failed to read {path:?}: {e}");
    });

    // Top-level fraud-team.json wraps the register payload in
    //   { "name": "fraud-team", "register": { "nodes": [...] }, ... }
    // so we have to descend into `register.nodes` to get the validator input.
    let outer: serde_json::Value =
        serde_json::from_str(&raw).expect("fraud-team.json must be valid JSON");
    let nodes_value = outer
        .get("register")
        .and_then(|r| r.get("nodes"))
        .and_then(|n| n.as_array())
        .unwrap_or_else(|| {
            panic!("fraud-team.json must contain register.nodes (array)");
        });

    // Deserialize the nodes array to typed PayloadNode list. Any deserialization
    // error here is itself a validation failure (e.g., missing required fields,
    // invalid kind discriminator).
    let mut nodes: Vec<PayloadNode> = Vec::with_capacity(nodes_value.len());
    let mut deserialization_failures: Vec<String> = Vec::new();
    for (idx, n) in nodes_value.iter().enumerate() {
        match serde_json::from_value::<PayloadNode>(n.clone()) {
            Ok(node) => nodes.push(node),
            Err(e) => {
                let name = n.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                let kind = n.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
                deserialization_failures.push(format!("nodes[{idx}] kind={kind} name={name}: {e}"));
            }
        }
    }
    if !deserialization_failures.is_empty() {
        panic!(
            "fraud-team.json has {} node(s) that failed PayloadNode deserialization:\n  - {}",
            deserialization_failures.len(),
            deserialization_failures.join("\n  - ")
        );
    }

    // Run the live validation path. Empty registry simulates a fresh
    // /register call (which is exactly what the bench harness does on every
    // boot via beava-bench-v18).
    let registry = RegistryInner::default();
    let result = validate_payload(&registry, nodes);

    let validated = match result {
        Ok(v) => v,
        Err(errors) => {
            let mut msg = format!(
                "fraud-team.json failed register_validate with {} error(s):",
                errors.len()
            );
            for ValidationError { code, path, reason } in &errors {
                msg.push_str(&format!("\n  - [{code:?}] {path}\n      reason: {reason}",));
            }
            panic!("{msg}");
        }
    };

    // Beyond the validator: also exercise apply_registration + AggOp::new for
    // every compiled aggregation. The validator (`compile_aggregations_from_nodes`)
    // does not actually construct live state — it only builds descriptors. The
    // panic-prone path is `AggOp::new(&desc)` which reads `desc.ext.{lat_field,
    // lon_field, k, n, precision, ...}` via `unwrap_or` defaults. If a descriptor
    // built from JSON has an unexpected combination, this is where it surfaces.
    let live_registry = Registry::new();
    let (nodes, chains, schemas, aggs) = validated.into_parts();
    // Note: keep an Arc'd copy of the agg descriptors before apply_registration
    // moves them, so we can iterate features afterwards.
    let agg_desc_clones: Vec<Arc<beava_core::agg_descriptor::AggregationDescriptor>> =
        aggs.iter().map(|(_, d)| Arc::clone(d)).collect();
    live_registry.apply_registration(nodes, chains, schemas, aggs);

    // Construct one AggOp per compiled feature — same path the apply hot loop
    // hits on first event for a new entity. Catches descriptors that pass
    // validation but explode at AggOp::new (e.g., bad sketch params).
    for agg in &agg_desc_clones {
        for named in &agg.features {
            let _live = AggOp::new(&named.descriptor);
        }
    }
}
