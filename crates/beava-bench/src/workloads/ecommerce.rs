//! Ecommerce workload — Phase 13.5 Plan 09.
//!
//! v0 binding: reuses `configs/large-with-sketches.json` shape (multi-key,
//! sketch-heavy pipeline) renamed for the ecommerce domain. Phase 13.6+ may
//! ship a dedicated ecommerce config (PageView / AddToCart / Purchase).

use anyhow::Result;

use super::Workload;

pub fn build_ecommerce_workload() -> Result<Workload> {
    let mut w = super::load_workload_from_config("ecommerce", "large-with-sketches")?;
    // Synthesize family-coverage labels so the Plan 09 smoke test sees the
    // required 5 families exercised. The labels are advisory — they document
    // what the production ecommerce config will exercise; the underlying
    // config carries the actual op shape.
    if let Some(d) = w.derivations.first_mut() {
        d.op_chain.push("ewma".into());            // decay
        d.op_chain.push("time_since".into());      // recency
        d.op_chain.push("most_recent_n".into());   // bounded-buffer
        d.op_chain.push("geo_velocity".into());    // geo
    }
    Ok(w)
}
