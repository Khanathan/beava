---
phase: 01-core-engine
verified: 2026-04-09T14:00:00Z
status: passed
score: 5/5 must-haves verified
overrides_applied: 0
gaps: []
deferred: []
---

# Phase 1: Core Engine Verification Report

**Phase Goal:** The foundational engine — in-memory state store, windowed aggregation ring buffer, core operators, and expression evaluator — is fully functional and unit-tested without any networking
**Verified:** 2026-04-09T14:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A Rust test can create an EntityState, push timestamped events, and read count/sum/avg values reflecting only events within the configured window | VERIFIED | `tests/test_pipeline.rs`: 9 integration tests pass, including `test_push_multiple_events_aggregates_correctly` (count=2, sum=80, avg=40 after two pushes) and `test_window_expiration_end_to_end` |
| 2 | The bucketed ring buffer correctly expires old buckets as time advances, and reads across multiple buckets produce accurate aggregates | VERIFIED | `src/engine/window.rs`: 14 unit tests pass. `test_advance_beyond_full_window_zeros_all_buckets` verifies pitfall-3 handling; `test_bucket_wraps_around_ring` verifies ring wrap with correct sum |
| 3 | A derive expression string is parsed at registration time into an AST and evaluated at event time to produce a numeric result | VERIFIED | `src/engine/expression.rs` (1120 lines): `parse_expr` returns `Result<Expr, TallyError>`, `eval` walks AST with `EvalContext`. `test_derive_with_event_field_access` integration test passes |
| 4 | The expression evaluator returns FeatureValue::Missing (not panic, not NaN) for division-by-zero and for missing fields | VERIFIED | `expression.rs` line 416: `if r == 0.0 { return FeatureValue::Missing }`. `guard_float()` at line 367 catches NaN/infinity. `test_eval_div_by_zero_float_returns_missing` and `test_eval_div_by_zero_int_returns_missing` tests pass. `test_derive_division_by_zero_returns_missing` integration test passes |
| 5 | All operator state uses AHashMap and SystemTime-based window buckets so client-supplied Unix timestamps are handled correctly | VERIFIED | `src/types.rs`: `pub type FeatureMap = ahash::AHashMap<String, FeatureValue>`. `src/state/store.rs`: `entities: AHashMap<EntityKey, EntityState>`. `RingBuffer` uses `SystemTime` throughout. `advance_to` uses `unwrap_or(Duration::ZERO)` for out-of-order timestamps |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `Cargo.toml` | Project manifest with all Phase 1 dependencies | VERIFIED | ahash, winnow, thiserror, serde, serde_json, postcard all present |
| `src/types.rs` | FeatureValue, Timestamp, EntityKey type definitions | VERIFIED | 42 lines; Float/Int/String/Missing variants with serde derives; AHashMap FeatureMap |
| `src/error.rs` | TallyError enum with thiserror | VERIFIED | 23 lines; Parse/Type/Window/Expression/Protocol variants |
| `src/engine/window.rs` | RingBuffer with time-bucketed sliding window | VERIFIED | 339 lines (> 100 min); struct RingBuffer, advance_to, add_to_current, sum_all; 14 tests |
| `src/engine/operators.rs` | Operator trait + CountOp/SumOp/AvgOp | VERIFIED | 450 lines (> 150 min); all three operators implemented; 24 unit tests |
| `src/engine/expression.rs` | Expression parser (winnow Pratt) and evaluator | VERIFIED | 1120 lines (> 250 min); parse_expr, eval, EvalContext, all AST types; 56 tests |
| `src/state/store.rs` | StateStore with AHashMap<EntityKey, EntityState> | VERIFIED | 243 lines (> 80 min); EntityState/StateStore/StaticFeature exported; 8 unit tests |
| `src/engine/pipeline.rs` | StreamDefinition, OperatorDef, PipelineEngine | VERIFIED | 368 lines (> 120 min); PipelineEngine, StreamDefinition, FeatureDef; push-through wired |
| `tests/test_pipeline.rs` | Integration tests | VERIFIED | 224 lines (> 60 min); 9 integration tests all passing |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/engine/window.rs` | `src/types.rs` | `use crate::types::Timestamp` | WIRED | Uses `SystemTime` (Timestamp alias) throughout |
| `src/engine/operators.rs` | `src/engine/window.rs` | CountOp/SumOp/AvgOp contain RingBuffer fields | WIRED | `buffer: RingBuffer<u64>`, `buffer: RingBuffer<f64>`, paired buffers in AvgOp |
| `src/engine/operators.rs` | `src/types.rs` | push/read use FeatureValue | WIRED | `FeatureValue::Int`, `FeatureValue::Float`, `FeatureValue::Missing` returned from read |
| `src/engine/expression.rs` | `src/types.rs` | eval returns FeatureValue | WIRED | `use crate::types::FeatureValue` and `EvalContext` uses `AHashMap<String, FeatureValue>` |
| `src/engine/pipeline.rs` | `src/state/store.rs` | PipelineEngine holds &mut StateStore | WIRED | `store: &mut StateStore` parameter in push/get_features; `use crate::state::store::StateStore` |
| `src/engine/pipeline.rs` | `src/engine/operators.rs` | StreamDefinition uses Operator trait | WIRED | `use super::operators::{Operator, CountOp, SumOp, AvgOp}`; operators pushed per FeatureDef |
| `src/engine/pipeline.rs` | `src/engine/expression.rs` | Derive features evaluated via eval() | WIRED | `use super::expression::{Expr, EvalContext, parse_expr, eval}`; `eval(expr, &ctx)` called at line 182 and 222 |
| `tests/test_pipeline.rs` | `src/engine/pipeline.rs` | Integration tests use PipelineEngine | WIRED | 9 integration tests exercise full push-through path |

### Data-Flow Trace (Level 4)

No dynamic data rendering artifacts (no UI components, no API routes). All data flows are in-process and exercised by unit and integration tests. Skipped — not applicable to this phase.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full test suite passes | `cargo test` | 110 passed; 0 failed | PASS |
| Integration tests: push events and read features | `cargo test --test test_pipeline` | 9 passed; 0 failed | PASS |
| Window unit tests: ring buffer expiration | `cargo test engine::window` | 14 tests pass | PASS |
| Operator unit tests: count/sum/avg | `cargo test engine::operators` | 24 tests pass | PASS |
| Expression unit tests: parse and eval | `cargo test engine::expression` | 56 tests pass (23 parser + 33 evaluator) | PASS |
| State store unit tests | `cargo test state::store` | 8 tests pass | PASS |
| Compilation | `cargo build` | Compiles with 0 errors, 0 warnings | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| ENG-01 | Plan 04 | In-memory state store (HashMap<EntityKey, EntityState>) with live and static features | SATISFIED | `StateStore` in `src/state/store.rs`; AHashMap used; live + static features merged in `get_all_features` |
| ENG-02 | Plan 01 | Sliding windows use bucketed ring buffer with configurable bucket granularity | SATISFIED | `RingBuffer<T>` in `src/engine/window.rs`; `new(window_duration, bucket_duration)` configurable; lazy expiration via `advance_to` |
| ENG-03 | Plan 02 | count operator tracks event count within a time window | SATISFIED | `CountOp` in `src/engine/operators.rs`; wraps `RingBuffer<u64>`; returns `Int(N)` or `Missing` |
| ENG-04 | Plan 02 | sum operator accumulates a numeric field within a time window | SATISFIED | `SumOp` in `src/engine/operators.rs`; wraps `RingBuffer<f64>`; Redis-strict type checking |
| ENG-05 | Plan 02 | avg operator computes running average of a numeric field within a time window | SATISFIED | `AvgOp` in `src/engine/operators.rs`; paired count+sum buffers; divides on read |
| ENG-06 | Plan 03 | Expression evaluator parses derive/where expressions at registration time into AST | SATISFIED | `parse_expr(input: &str) -> Result<Expr, TallyError>` in `expression.rs`; used in `PipelineEngine` via `FeatureDef::Derive { expr: Expr }` |
| ENG-07 | Plan 03 | Expression evaluator supports arithmetic, comparison, boolean, field access, builtins | SATISFIED | `BinOp` has Add/Sub/Mul/Div/Gt/Lt/Gte/Lte/Eq/Neq/And/Or; `UnOp` has Not/Neg; `FieldRef` has Local/Qualified/Event; builtins abs/min/max/now implemented |
| ENG-08 | Plan 03 | Expression evaluator returns Missing on division-by-zero or missing inputs | SATISFIED | `guard_float()` catches NaN/infinity; `r == 0.0` check in Div arm; Missing propagation before all binary ops; 2 dedicated div-by-zero tests pass |

All 8 ENG requirements satisfied. No orphaned requirements.

### Anti-Patterns Found

No blockers found. Reviewed all key files from phase summaries.

| File | Pattern | Severity | Notes |
|------|---------|----------|-------|
| `src/state/store.rs` | `Box<dyn Operator>` not serializable | Info | Intentional — acknowledged in SUMMARY as Phase 4 concern; operator enum wrapper deferred |

### Human Verification Required

None. All phase-1 goals are verifiable programmatically. No UI, no networking, no external services.

### Gaps Summary

No gaps. All 5 roadmap success criteria are verified. All 8 ENG requirements (ENG-01 through ENG-08) are satisfied. The full test suite passes with 110 tests, 0 failures. The phase goal — a functional, unit-tested core engine with no networking — is fully achieved.

---
_Verified: 2026-04-09T14:00:00Z_
_Verifier: Claude (gsd-verifier)_
