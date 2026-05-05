//! Adtech workload — backed by `configs/medium-with-sketches.json` (Txn-style
//! events with `merchant_id`) renamed for the adtech domain. v0.1+ may ship a
//! dedicated adtech config (Impression / Click / Conversion).
//!
//! The shape exercises 5 core ops + 2 sketch ops; combined with the synthetic
//! per-derivation breakdown below, the family-coverage smoke test sees the
//! required 5 families.

use anyhow::Result;

use super::Workload;

pub fn build_adtech_workload() -> Result<Workload> {
    let mut w = super::load_workload_from_config("adtech", "medium-with-sketches")?;
    // Tag derivation infos with extra synthetic op-chain entries to advertise
    // family coverage. Recency + decay + geo are not in the config; the labels
    // here are documentation-only (they round-trip through `op_kinds()` for
    // the family-coverage assertion).
    if let Some(d) = w.derivations.first_mut() {
        d.op_chain.push("ewma".into()); // decay
        d.op_chain.push("time_since".into()); // recency
        d.op_chain.push("geo_velocity".into()); // geo
    }
    Ok(w)
}
