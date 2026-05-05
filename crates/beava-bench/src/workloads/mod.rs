//! Workload registry.
//!
//! A [`Workload`] bundles a register payload + event generator. The CLI mode
//! modules call [`load_by_name`] to obtain one and feed it to the harness.
//!
//! Workloads are backed by JSON config files under `crates/beava-bench/configs/`
//! (small.json, medium.json, large.json, fraud-team.json) carrying the real
//! wire-shape register payload. The dataset shapes (`adtech`, `fraud`,
//! `ecommerce`) reuse existing configs — adtech → `medium-with-sketches`,
//! fraud → `fraud-team` (the canonical primary tuning shape), ecommerce →
//! `large-with-sketches`.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use serde_json::{Map, Value};

pub mod adtech;
pub mod ecommerce;
pub mod fraud;

/// A workload bundles a register payload (wire-shape) + event generator.
pub struct Workload {
    pub name: String,
    pub register_payload: Value, // The full {"nodes": [...]} object posted to /register.
    pub derivations: Vec<DerivationInfo>,
    pub event_generator: EventGenFn,
}

pub type EventGenFn =
    Box<dyn Fn(u64) -> Box<dyn Iterator<Item = GeneratedEvent> + Send> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct DerivationInfo {
    pub name: String,
    pub op_chain: Vec<String>,
}

impl DerivationInfo {
    pub fn op_kinds(&self) -> impl Iterator<Item = &str> {
        self.op_chain.iter().map(String::as_str)
    }
}

#[derive(Debug, Clone)]
pub struct GeneratedEvent {
    pub event_name: String,
    pub fields: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PipelineConfig {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub description: String,
    pub register: Value,
    pub event_name: String,
    #[allow(dead_code)]
    pub features: Vec<String>,
    pub key_field: String,
    pub extra_fields: Map<String, Value>,
}

pub fn load_by_name(name: &str) -> Result<Workload> {
    match name {
        "adtech" => adtech::build_adtech_workload(),
        "fraud" => fraud::build_fraud_workload(),
        "ecommerce" => ecommerce::build_ecommerce_workload(),
        "small" | "medium" | "large" => load_legacy_size_workload(name),
        _ => Err(anyhow!(
            "unknown workload {:?}; valid: adtech | fraud | ecommerce | small | medium | large",
            name
        )),
    }
}

pub(crate) fn load_legacy_size_workload(size: &str) -> Result<Workload> {
    load_workload_from_config(size, size)
}

/// Generic loader: reads `configs/{config_name}.json` and builds a workload
/// using the config's register payload + a synthetic event generator that
/// stuffs the config's `extra_fields` with random values matching their
/// declared type.
pub(crate) fn load_workload_from_config(
    workload_name: &str,
    config_name: &str,
) -> Result<Workload> {
    let manifest =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| "crates/beava-bench".to_string());
    let path = PathBuf::from(manifest)
        .join("configs")
        .join(format!("{config_name}.json"));
    let bytes = std::fs::read(&path).with_context(|| format!("read config {}", path.display()))?;
    let cfg: PipelineConfig = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse config {}", path.display()))?;
    let derivations = derivation_info_from_register(&cfg.register);
    let event_name = cfg.event_name.clone();
    let key_field = cfg.key_field.clone();
    let extra_fields = cfg.extra_fields.clone();

    let event_generator: EventGenFn = Box::new(move |n_events: u64| {
        let mut rng = StdRng::seed_from_u64(0xCAFE_BABE);
        let mut remaining = n_events;
        let event_name = event_name.clone();
        let key_field = key_field.clone();
        let extra_fields = extra_fields.clone();
        let iter = std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            let key_idx: u64 = rng.gen_range(0..100_000);
            let mut fields = Map::new();
            fields.insert(key_field.clone(), Value::String(format!("k{key_idx:08}")));
            fields.insert(
                "event_time".into(),
                Value::Number((1_000_000_i64 + (100_000 - remaining as i64)).into()),
            );
            for (field, ty) in &extra_fields {
                let v = match ty.as_str().unwrap_or("f64") {
                    "f64" => serde_json::json!(rng.gen_range(0.0..1000.0)),
                    "i64" => serde_json::json!(rng.gen_range(0_i64..1_000_000)),
                    "str" => serde_json::json!(format!("s{}", rng.gen_range(0..1000))),
                    _ => serde_json::json!(0),
                };
                fields.insert(field.clone(), v);
            }
            Some(GeneratedEvent {
                event_name: event_name.clone(),
                fields,
            })
        });
        Box::new(iter) as Box<dyn Iterator<Item = GeneratedEvent> + Send>
    });

    Ok(Workload {
        name: workload_name.to_string(),
        register_payload: cfg.register,
        derivations,
        event_generator,
    })
}

/// Helper for adtech/fraud/ecommerce builders to derive [`DerivationInfo`]
/// from a register-payload `{"nodes": [...]}` object.
pub(crate) fn derivation_info_from_register(register: &Value) -> Vec<DerivationInfo> {
    let empty: Vec<Value> = vec![];
    let nodes = register["nodes"].as_array().unwrap_or(&empty);
    nodes
        .iter()
        .filter(|d| d["kind"] == "derivation")
        .map(|d| {
            let name = d["name"].as_str().unwrap_or_default().to_string();
            let mut ops: Vec<String> = vec![];
            if let Some(op_steps) = d["ops"].as_array() {
                for step in op_steps {
                    if let Some(agg) = step["agg"].as_object() {
                        for (_, v) in agg {
                            if let Some(op) = v["op"].as_str() {
                                ops.push(op.to_string());
                            }
                        }
                    }
                }
            }
            DerivationInfo {
                name,
                op_chain: ops,
            }
        })
        .collect()
}
