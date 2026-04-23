# Deferred Items — Phase 05.5

## Pre-existing clippy warning in phase5_agg.rs

**File:** `crates/beava-core/benches/phase5_agg.rs:259`
**Warning:** `clippy::redundant_closure` — `|| BTreeMap::<String, AggStateTable>::new()` should be `BTreeMap::<String, AggStateTable>::new`
**Found during:** Plan 05.5-03 (phase4_expr bench) — discovered while running workspace-wide clippy
**Status:** Pre-existing issue, not caused by 05.5-03 changes. Out of scope per deviation scope boundary rule.
**Suggested fix:** Replace the closure with the function reference directly.
**Owner:** Plan that next modifies `phase5_agg.rs` (plan 05.5-06 or similar).
