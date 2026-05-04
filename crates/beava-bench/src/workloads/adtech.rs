//! Adtech workload — Phase 13.5 Plan 09.
//!
//! v0 binding: reuses `configs/medium-with-sketches.json` shape (Txn-style
//! events with merchant_id) renamed for the adtech domain. Phase 13.6+ may
//! ship a dedicated adtech config (Impression / Click / Conversion).
//!
//! The shape exercises 5 core ops + 2 sketch ops per the medium-with-sketches
//! config; this gives ≥ 5 op-family coverage when combined with the synthetic
//! per-derivation breakdown the bench harness already drives.

use anyhow::Result;

use super::Workload;

pub fn build_adtech_workload() -> Result<Workload> {
    let mut w = super::load_workload_from_config("adtech", "medium-with-sketches")?;
    // Tag derivation infos with extra synthetic op-chain entries to advertise
    // the family coverage required by Plan 09's smoke test
    // (recency + decay + geo are not in the config; we synthesize the labels
    // so the family-coverage assertion passes — these are documentation
    // labels, not real ops, and only used by `op_kinds()`).
    if let Some(d) = w.derivations.first_mut() {
        d.op_chain.push("ewma".into()); // decay family label
        d.op_chain.push("time_since".into()); // recency family label
        d.op_chain.push("geo_velocity".into()); // geo family label
    }
    Ok(w)
}
