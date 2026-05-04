//! Fraud workload — Phase 13.5 Plan 09.
//!
//! v0 binding: reuses `configs/fraud-team.json` — the canonical realistic
//! fraud pipeline (5 event types, 5 group_by axes, 90 features) per
//! `project_fraud_team_primary_bench`. This is the same config used by the
//! Phase 19 throughput regression-gate.

use anyhow::Result;

use super::Workload;

pub fn build_fraud_workload() -> Result<Workload> {
    super::load_workload_from_config("fraud", "fraud-team")
}
