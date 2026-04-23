---
status: passed
phase: 04-stateless-ops-expression-evaluator
verified: 2026-04-23
must_haves_total: 5
must_haves_verified: 5
human_verification: []
gaps: []
---

# Phase 04 Verification — PASSED

**Phase Goal:** Server-side expression parser + evaluator for the `bv.col(...)` canonical form. Stateless per-event op chain (filter/select/drop/rename/with_columns/map/cast/fillna) executes before aggregations see events. SDK clients register chained ops in their DAG nodes.

## Success Criteria (5/5)

| SC | Verified |
|---|---|
| SC1: `Event.filter(bv.col("amount") > 100)` registered via SDK; server rejects failing events | ✅ `phase4_smoke.rs::sc1_*` + `test_phase4_smoke.py::test_sc1_*` |
| SC2: `Event.with_columns(is_big=...)` adds derived column | ✅ `phase4_smoke.rs::sc2_*` + `test_phase4_smoke.py::test_sc2_*` |
| SC3: Chained ops (filter → select → with_columns → cast) compose; schema propagates | ✅ `phase4_smoke.rs::sc3_chain` + Python smoke |
| SC4: Proptest client-server eval equivalence (≥256 cases, skip rate <50%) | ✅ `test_sc4_proptest_client_server_eval_equivalence` — skip rate ~0% after generator fix (commit e7a3946) |
| SC5: Malformed predicate at register → 400 with path on both HTTP and TCP | ✅ `phase4_smoke.rs::sc5_http` + `sc5_tcp` + Python smoke |

## Requirements Coverage (13/13)

SDK-OPS-01..10, SDK-COL-07, SRV-APPLY-06, SRV-APPLY-07 — all delivered by code and tests.

## Gates

- `cargo test --workspace`: 386+ tests pass (transient TCP port-contention flake observed on one run; second run clean)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean
- `cargo fmt --all --check`: clean
- `cd python && python -m pytest -q`: 156 pass
- `cd python && python -m ruff check .`: clean

## Fix Chain Summary

- Review findings (2 CR + 6 WR): all fixed in commits 2132d63..fab0c02
- Post-verify gap (SC4 schema mismatch + fmt drift): fixed in commits 2789098, 9cf63b3
- Post-reverify gap (SC4 comparison-in-arithmetic nesting): fixed in commit e7a3946

## Architectural Notes Carried Forward

Plan 04-05 introduces `RegistryInner.compiled_chains` as a parallel map keyed by derivation-node name. Phase 5's aggregation framework should follow the same pattern for aggregation state — parallel map, server-authoritative, no mutation of descriptors.

Per CONTEXT decisions (2026-04-23, captured in memory/project_stateful_architecture.md):
- Phase 5 ops keep apply-loop dispatch surface thin (apply_event + query; no serialize on operator trait)
- Serialization lives in a separate adapter layer per (operator × backend)
- Stream event IDs need to land in Phase 6 WAL to preserve future retraction capability
