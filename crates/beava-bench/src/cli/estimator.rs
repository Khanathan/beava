//! Pre-run memory estimator — Phase 13.5 Plan 10.
//!
//! Uses Phase 12.9 cost model (`size_of::<AggOp>() = 80 bytes`, fraud-team
//! weighted-avg ~6 KB/entity) projected over the workload's derivation count
//! and the size-bucket's expected entity count.

use anyhow::{anyhow, Result};

use crate::workloads;

#[derive(Debug, Clone)]
pub struct MemoryEstimate {
    pub workload: String,
    pub size: String,
    pub entity_count_estimate: u64,
    pub bytes_per_entity: u64,
    pub expected_rss_bytes: u64,
    pub per_derivation_breakdown: Vec<DerivationCost>,
}

#[derive(Debug, Clone)]
pub struct DerivationCost {
    pub name: String,
    pub bytes_per_entity: u64,
}

const SIZE_ENTITY_COUNTS: &[(&str, u64)] = &[
    ("small", 10_000),
    ("medium", 100_000),
    ("large", 1_000_000),
];

const PER_AGG_OP_BYTES: u64 = 80; // Phase 12.9 size_of::<AggOp>.
const PER_ENTITY_OVERHEAD_BYTES: u64 = 256;

pub fn estimate_memory(workload_name: &str, size: &str) -> Result<MemoryEstimate> {
    let entity_count = SIZE_ENTITY_COUNTS
        .iter()
        .find(|(s, _)| *s == size)
        .map(|(_, n)| *n)
        .ok_or_else(|| anyhow!("unknown size {:?}; valid: small | medium | large", size))?;
    let workload = workloads::load_by_name(workload_name)?;
    let mut per_derivation: Vec<DerivationCost> = Vec::new();
    let mut total_per_entity_bytes = PER_ENTITY_OVERHEAD_BYTES;
    for d in &workload.derivations {
        let n_ops = d.op_chain.len() as u64;
        let mut cost = n_ops * PER_AGG_OP_BYTES;
        for op in &d.op_chain {
            cost += match op.as_str() {
                "histogram" | "hour_of_day_histogram" | "dow_hour_histogram"
                | "seasonal_deviation" | "event_type_mix" | "most_recent_n"
                | "reservoir_sample" => 2_048,
                "n_unique" => 12_288,
                "quantile" => 2_048,
                "top_k" => 1_024,
                "bloom_member" => 4_096,
                "entropy" => 512,
                "geo_velocity" | "geo_distance" | "geo_spread" | "distance_from_home" => 256,
                "ewma" | "ewvar" | "ew_zscore" | "decayed_sum" | "decayed_count" | "twa" => 32,
                "trend" | "trend_residual" | "outlier_count" | "value_change_count"
                | "burst_count" | "rate_of_change" | "inter_arrival_stats" => 256,
                "first_seen" | "last_seen" | "age" | "has_seen" | "time_since" | "streak"
                | "max_streak" | "negative_streak" | "first_seen_in_window" | "first" | "last"
                | "lag" | "delta_from_prev" => 32,
                "first_n" | "last_n" | "time_since_last_n" => 256,
                _ => 64,
            };
        }
        total_per_entity_bytes += cost;
        per_derivation.push(DerivationCost {
            name: d.name.clone(),
            bytes_per_entity: cost,
        });
    }
    let expected_rss_bytes = total_per_entity_bytes * entity_count;
    Ok(MemoryEstimate {
        workload: workload_name.into(),
        size: size.into(),
        entity_count_estimate: entity_count,
        bytes_per_entity: total_per_entity_bytes,
        expected_rss_bytes,
        per_derivation_breakdown: per_derivation,
    })
}

pub fn print_estimate_to_stderr(est: &MemoryEstimate) {
    eprintln!("=== Pre-run memory estimate ===");
    eprintln!("  workload:        {}", est.workload);
    eprintln!("  size:            {}", est.size);
    eprintln!("  entities (est):  {}", est.entity_count_estimate);
    eprintln!("  bytes/entity:    {}", est.bytes_per_entity);
    eprintln!(
        "  expected RSS:    {:.2} MB",
        est.expected_rss_bytes as f64 / 1_048_576.0
    );
    if est.expected_rss_bytes > 32 * 1024 * 1024 * 1024 {
        eprintln!("  WARNING: predicted > 32 GB — verify your box has enough memory.");
    }
}
