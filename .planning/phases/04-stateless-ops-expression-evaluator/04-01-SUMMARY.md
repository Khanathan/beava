---
phase: 04-stateless-ops-expression-evaluator
plan: "01"
subsystem: core-data-model
tags: [row, value, null-semantics, beava-core, btreemap, three-valued-logic, rust]

requires:
  - phase: 02-sources-registry-version-bumps
    provides: FieldType enum in schema.rs — Value mirrors it one-to-one via type_of()
  - phase: 02.5-tcp-wire
    provides: beava-core crate structure and module conventions

provides:
  - "Row struct: BTreeMap<String, Value> with owning mutation API (with_field/without_field/renamed)"
  - "Value enum: 7 variants (Null/Str/I64/F64/Bool/Bytes/Datetime) with NaN-safe PartialEq"
  - "SQL three-valued null logic helpers: and_three_valued / or_three_valued / not_three_valued"
  - "Value::type_of() -> Option<FieldType> mapping (None for Null)"

affects:
  - 04-02-parser (AST literal types are Value variants)
  - 04-03-evaluator (evaluator output is Value; boolean operators call and/or/not helpers)
  - 04-04-op-executor (op chain works against Row; with_field/without_field/renamed are the mutation primitives)
  - 05-aggregation (BTreeMap iteration order is load-bearing for aggregation-key determinism)

tech-stack:
  added: []
  patterns:
    - "Owning Row API: with_field/without_field/renamed consume self and return updated Row (SDK-OPS-09)"
    - "NaN-safe PartialEq: F64(NaN) != F64(NaN); cross-variant comparisons always false"
    - "Runtime-tolerant three-valued logic: non-bool/non-null operands propagate Null rather than panic"

key-files:
  created:
    - crates/beava-core/src/row.rs
  modified:
    - crates/beava-core/src/lib.rs

key-decisions:
  - "Owning API for Row mutation (with_field takes self, returns Self) enforces SDK-OPS-09 at the type level — no shared mutable Row references are possible"
  - "Value::PartialEq is manual (not derived) to give NaN the correct IEEE-754 semantics: two NaNs are never equal"
  - "Non-bool/non-null operands to and/or/not return Null rather than Err — matches CONTEXT.md D-04 runtime-tolerant design: register-time type checking (Plan 04-05) is the error gate; runtime silently propagates Null"
  - "Row::new() backed by BTreeMap::new() — deterministic alphabetical iteration order is load-bearing for Phase 5 aggregation-key stability"

patterns-established:
  - "TDD red-green commit chain: test(04-01): lands first with todo!() stubs all panicking; feat(04-01): replaces stubs with real logic"
  - "Value three-valued helpers use match arms ordered: short-circuit first, normal cases, fallthrough Null — mirrors SQL operator evaluation order"

requirements-completed:
  - SDK-OPS-09

duration: 15min
completed: 2026-04-23
---

# Phase 4 Plan 01: Row + Value + SQL Three-Valued Null Logic Summary

**`BTreeMap<String, Value>` Row with NaN-safe PartialEq, `FieldType`-mirroring 7-variant Value enum, and full SQL three-valued null truth table in `beava-core` — the stable substrate for Plans 04-02 through 04-04**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-04-23
- **Completed:** 2026-04-23
- **Tasks:** 2 (1.a red, 1.b green)
- **Files modified:** 2

## Accomplishments

- `Value` enum with 7 variants (Null/Str/I64/F64/Bool/Bytes/Datetime), custom NaN-safe `PartialEq`, and `type_of() -> Option<FieldType>` mapping covering all non-Null variants
- SQL three-valued `and_three_valued` / `or_three_valued` / `not_three_valued` helpers with correct short-circuit semantics (`false AND null = false`, `true OR null = true`) and runtime-tolerant non-bool coercion to Null
- `Row(BTreeMap<String, Value>)` with owning `with_field` / `without_field` / `renamed` API — SDK-OPS-09 enforced at the type system level (consuming `self` makes shared mutation structurally impossible)
- TDD red-green commit chain established: `test(04-01)` stub commit (12 FAILED) → `feat(04-01)` green commit (12 PASSED, 268 workspace tests zero regressions)
- beava-core remains syscall-free (no `std::fs`, `std::net`, `tokio` imports in `row.rs`)

## Task Commits

1. **Task 1.a (red): Write failing tests** - `1c866ee` (test)
2. **Task 1.b (green): Implement Row + Value + helpers** - `b1db22e` (feat)

## Files Created/Modified

- `/Users/petrpan26/work/tally/crates/beava-core/src/row.rs` — New module: Value enum, Row struct, PartialEq, type_of, three-valued logic helpers, 12 unit tests (372 LoC)
- `/Users/petrpan26/work/tally/crates/beava-core/src/lib.rs` — Added `pub mod row;` (alphabetical between `registry_diff` and `schema`)

## Decisions Made

- **Owning Row API** (`with_field(self, ...) -> Self` not `&mut self`): satisfies SDK-OPS-09 by making shared mutable state structurally impossible — derivation op steps consume and produce new Rows, they cannot alias upstream state.
- **Manual `PartialEq` for `Value`**: derived `PartialEq` would give `F64(NaN) == F64(NaN)` as `true` (Rust's default float comparison), which violates IEEE-754. Manual impl guards with `is_nan()`.
- **Runtime-tolerant non-bool operands → Null** (not Err): CONTEXT.md §D-04 says type errors are caught at register time by the expression type-checker (Plan 04-05); the runtime evaluator propagates Null on type mismatch rather than panicking or returning Err. This matches pandas/polars/SQL behavior.
- **`Value::Null` is structurally equal to itself** (`Value::Null == Value::Null` is `true` in Rust `PartialEq`) while SQL `null == null` semantics (returns `Value::Null`, not true) are expressed through the `and_three_valued`/`or_three_valued` helpers and the evaluator's `==` operator implementation in Plan 04-03.

## Deviations from Plan

None — plan executed exactly as written. The `todo!()` stub approach for the red commit matched the plan's specified structure; all 12 tests were written per the plan's behavior block.

## Issues Encountered

None. One minor lint cleanup: stub parameter names used `_other` / `_new` prefixes to suppress `unused_variable` warnings in the red commit, which is the idiomatic Rust pattern.

## Known Stubs

None. All public methods are fully implemented. The three-valued logic helpers handle the complete truth table including all non-bool/non-null fallthrough cases.

## Threat Flags

None. `row.rs` is pure in-memory data manipulation with no network, file system, or trust-boundary surface.

## Self-Check

### Files exist
- `crates/beava-core/src/row.rs` — FOUND (372 lines)
- `crates/beava-core/src/lib.rs` — FOUND (modified, contains `pub mod row;`)

### Commits exist
- `1c866ee` — FOUND (`test(04-01): add failing Row + Value + three-valued-logic tests (red)`)
- `b1db22e` — FOUND (`feat(04-01): implement Row + Value + SQL three-valued null logic (green)`)

### Gate results
- `cargo test -p beava-core row::tests` — 12/12 PASSED
- `cargo test --workspace` — 268 tests PASSED, 0 failed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — CLEAN
- `cargo fmt --all --check` — CLEAN

## Self-Check: PASSED

## Next Phase Readiness

- Plans 04-02 (expression parser), 04-03 (evaluator), and 04-04 (op executor) can import `Row` and `Value` from `beava_core::row`
- The evaluator (Plan 04-03) calls `and_three_valued` / `or_three_valued` / `not_three_valued` directly for `and`/`or`/`not` AST nodes — no re-implementation needed
- Phase 5 aggregation-key stability is guaranteed: `Row::iter()` delegates to `BTreeMap::iter()` which is alphabetically sorted and deterministic
- No blockers for downstream plans

---
*Phase: 04-stateless-ops-expression-evaluator*
*Completed: 2026-04-23*
