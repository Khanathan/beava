//! Fraud workload — backed by `configs/fraud-team.json`, the canonical
//! realistic fraud pipeline (5 event types, 5 group_by axes, 90 features)
//! and the primary tuning shape for the throughput regression-gate per
//! CLAUDE.md §"End-to-end throughput regression contract".

use anyhow::Result;

use super::Workload;

pub fn build_fraud_workload() -> Result<Workload> {
    super::load_workload_from_config("fraud", "fraud-team")
}
