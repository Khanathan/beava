---
phase: 17-enriched-event-propagation
verified: 2026-04-12T23:15:00Z
status: human_needed
score: 2/3 must-haves verified (SC-2 benchmark requires manual run)
overrides_applied: 0
human_verification:
  - test: "Run benchmark matrix: python3 benchmark/tally-throughput/bench.py --matrix --clients 8"
    expected: "Throughput within -5% of 1.1M eps baseline (pitfall C-1 gate)"
    why_human: "Benchmark requires a running Tally server and Python client; cannot run headlessly. Script exists at benchmark/tally-throughput/bench.py. This is success criterion 2 from ROADMAP.md."
---

# Phase 17: Enriched Event Propagation Verification Report

**Phase Goal:** Downstream datasets can reference upstream computed fields (derives, aggregations) during cascade execution, enabling multi-stage computed features like map -> group_by -> downstream sum("amount_usd")
**Verified:** 2026-04-12T23:15:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | User can define a multi-stage pipeline where an upstream dataset computes a derived field and a downstream dataset aggregates that derived field, and PUSH returns the correct downstream result in a single request-response cycle | VERIFIED | `test_enriched_derive_to_downstream_sum` asserts `total_usd_1h == 120.0` (amount=100, exchange_rate=1.2); `test_enriched_multi_hop_cascade` verifies 4-hop cascade; all 7 enrichment tests pass |
| 2 | Enriched fields propagate via a side-channel accumulator (not event clone) and full benchmark matrix passes within -5% of 1.1M eps baseline (pitfall C-1 gate) | PARTIAL — automated portion verified, benchmark needs human | Stack-local `enrichment_json` / `enrichment_fv` accumulators in `push_with_cascade_internal` (pipeline.rs:874-875); no DashMap insertion confirmed; benchmark script exists at `benchmark/tally-throughput/bench.py` but requires running server |
| 3 | Enrichment works correctly under multi-threaded tokio runtime with 8 concurrent clients (pitfall C-5 — enrichment values never re-enter DashMap during downstream push) | VERIFIED | `test_enriched_concurrent_clients` passes: 8 tokio tasks, 100 events each, per-user assert `total_usd_1h == 1500.0`; no cross-contamination |

**Score:** 2/3 fully verified (SC-2 automated aspects pass; benchmark gate needs manual run)

### Deferred Items

None.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/engine/operators.rs` | Operator trait with enrichment param, resolve_field helper, 14 operator impls updated | VERIFIED | `resolve_field` exists (1 definition); 16 enrichment params (trait + 15 impls including HLL); 13 `resolve_field(` call sites |
| `src/engine/expression.rs` | EvalContext with enrichment field, resolution order features -> enrichment -> event | VERIFIED | `pub enrichment: Option<&'a ahash::AHashMap<String, FeatureValue>>` found; resolution order implemented |
| `src/state/snapshot.rs` | OperatorState::push dispatches enrichment to all 15 variants | VERIFIED | `pub fn push` signature includes enrichment param (line 56); 16 enrichment references in file |
| `src/engine/pipeline.rs` | push_internal with enrichment params; push_with_cascade_internal with accumulator | VERIFIED | `enrichment_json: Option<&...>` at line 510; accumulator at lines 874-875; `Some(&enrichment_json)` passed in 2 downstream call sites |
| `tests/test_pipeline.rs` | 7 enrichment integration tests | VERIFIED | All 7 `fn test_enriched_*` tests exist and pass: derive-to-downstream, multi-hop, async mode, where-clause, qualified/unqualified resolution, no-cascade regression |
| `tests/test_concurrent.rs` | Concurrent enrichment correctness test with 8 clients | VERIFIED | `test_enriched_concurrent_clients` exists and passes (0.43s) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/engine/operators.rs` | `src/engine/operators.rs` | `resolve_field` used by all field-reading operators | WIRED | 13 call sites in operators.rs |
| `src/state/snapshot.rs` | `src/engine/operators.rs` | `OperatorState::push` delegates to `Operator::push` with enrichment | WIRED | `op.push(event, enrichment, now)` dispatched to all 15 variants |
| `src/engine/pipeline.rs::push_with_cascade_internal` | `src/engine/pipeline.rs::push_internal` | enrichment accumulator threaded through cascade loop | WIRED | `Some(&enrichment_json)` passed to downstream `push_internal` calls (2 sites: keyed and keyless downstream paths) |
| `src/engine/pipeline.rs::push_internal` | `src/engine/operators.rs::resolve_field` | `op.push` passes enrichment_json to operators | WIRED | `op.push(event, enrichment_json, now)` at line 617 |
| `tests/test_pipeline.rs` | `src/engine/pipeline.rs` | Tests exercise `push_with_cascade` with enrichment | WIRED | All 7 enrichment tests call `engine.push_with_cascade(...)` |
| `tests/test_concurrent.rs` | `src/engine/pipeline.rs` | Concurrent test exercises enrichment under multi-thread | WIRED | `test_enriched_concurrent_clients` uses TCP wire protocol (OP_PUSH_ASYNC + OP_FLUSH) |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `push_with_cascade_internal` | `enrichment_json` / `enrichment_fv` | Primary stream `push_internal` results | Yes — populated from `primary_features` map and intermediate downstream results | FLOWING |
| Downstream `push_internal` | `enrichment_fv` → `EvalContext` | Passed from cascade accumulator | Yes — EvalContext checks enrichment before event fallback | FLOWING |
| `test_enriched_derive_to_downstream_sum` | `total_usd_1h` | 3-stage cascade (RawTxns -> CurrencyNorm -> UserStats) | Yes — asserts `FeatureValue::Float(120.0)` | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| 7 enrichment integration tests pass | `~/.cargo/bin/cargo test test_enriched` | 7 passed, 0 failed | PASS |
| Concurrent enrichment test passes | `~/.cargo/bin/cargo test test_enriched_concurrent -- --nocapture` | 1 passed, 0 failed (0.43s) | PASS |
| No-cascade fast path skips enrichment | `grep "has_downstream" src/engine/pipeline.rs` | `if !has_downstream { return self.push_internal(..., None, None, ...)` } | PASS |
| Benchmark script exists for manual run | `ls benchmark/tally-throughput/bench.py` | EXISTS | PASS (manual gate pending) |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| ENG-01 | 17-01-PLAN, 17-02-PLAN, 17-03-PLAN | Enriched event propagation — upstream derive results visible to downstream datasets via side-channel accumulator, enabling multi-stage computed features | SATISFIED | resolve_field + EvalContext enrichment (Plan 01), stack-local cascade accumulator (Plan 02), 7 integration tests + concurrent test (Plan 03). All 6 commits verified (b71bbe9, 50d5094, 56f2a9e, 0158052, 9c91bba, 47f7e8b). |

### Anti-Patterns Found

No TODOs, FIXMEs, HACKs, or placeholder patterns found in any modified files.

### Human Verification Required

#### 1. Benchmark Regression Gate (SC-2)

**Test:** Start the Tally server, then run `python3 benchmark/tally-throughput/bench.py --matrix --clients 8`
**Expected:** Throughput within -5% of 1.1M eps baseline (must be >= ~1.045M eps with 8 concurrent clients)
**Why human:** Benchmark requires a running server instance + Python client; cannot execute headlessly during code verification. This is the C-1 gate — proving that side-channel enrichment adds no meaningful overhead to the hot path.

### Gaps Summary

No blocking gaps. The only open item is the manual benchmark gate for SC-2 (C-1 performance regression check), which requires a live server. All implementation artifacts are present, substantive, correctly wired, and data flows through them. The concurrent safety proof (C-5) passes automated testing. The single-request-cycle delivery of downstream computed features (SC-1) is asserted at the correct value in integration tests.

---

_Verified: 2026-04-12T23:15:00Z_
_Verifier: Claude (gsd-verifier)_
