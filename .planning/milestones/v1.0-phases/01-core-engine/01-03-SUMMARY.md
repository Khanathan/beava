---
phase: 01-core-engine
plan: 03
subsystem: engine
tags: [winnow, pratt-parser, expression-evaluator, ast, missing-propagation]

# Dependency graph
requires:
  - phase: 01-01
    provides: types.rs (FeatureValue enum, FeatureMap), error.rs (TallyError)
provides:
  - Expression parser (parse_expr): string -> AST at registration time
  - Expression evaluator (eval): AST + EvalContext -> FeatureValue at event time
  - EvalContext with field resolution (local, qualified, event)
  - Missing propagation semantics (SQL NULL, div-by-zero, NaN/infinity guards)
affects: [02-server, 03-python-sdk, 05-remaining-operators]

# Tech tracking
tech-stack:
  added: [winnow expression() Pratt combinator]
  patterns: [Pratt parsing with binding powers, fn-pointer fold functions, keyword whole-word matching]

key-files:
  created: [src/engine/expression.rs]
  modified: [src/engine/mod.rs]

key-decisions:
  - "winnow expression() Pratt combinator with nested alt() for >9 operator alternatives (winnow Alt tuple limit)"
  - "Keywords (and/or/not) rejected in parse_field_ref to let Pratt prefix/infix handle them"
  - "guard_float() defense-in-depth: all f64 arithmetic results checked for NaN/infinity -> Missing"
  - "String equality handled before Missing propagation check in eval_binary"

patterns-established:
  - "TDD RED/GREEN: write failing tests first, then implement to pass"
  - "Keyword safety: after keyword match, verify next char is not alphanumeric/underscore"
  - "Missing propagation: check both operands before any operation"

requirements-completed: [ENG-06, ENG-07, ENG-08]

# Metrics
duration: 8min
completed: 2026-04-09
---

# Phase 01 Plan 03: Expression Parser and Evaluator Summary

**winnow Pratt expression parser with full Missing propagation evaluator for derive/where expressions**

## Performance

- **Duration:** 8 min
- **Started:** 2026-04-09T13:41:49Z
- **Completed:** 2026-04-09T13:49:46Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Expression parser converts all CLAUDE.md expression patterns into correct ASTs using winnow Pratt combinator
- Expression evaluator with SQL NULL Missing propagation, div-by-zero -> Missing, NaN/infinity guards
- 56 unit tests for expression module (23 parser + 33 evaluator), 94 total tests passing
- Keyword safety: and_count, not_fraud, or_else parsed as field names, not keyword + remainder

## Task Commits

Each task was committed atomically:

1. **Task 1: Expression AST types and winnow Pratt parser (ENG-06)**
   - `76e1ea1` (test: add failing tests for expression parser - RED)
   - `1ef29d6` (feat: implement winnow Pratt expression parser - GREEN)
2. **Task 2: Expression evaluator with Missing propagation (ENG-07, ENG-08)**
   - `50d1539` (test: add failing tests for expression evaluator - RED)
   - `b3dd359` (feat: implement expression evaluator with Missing propagation - GREEN)

## Files Created/Modified
- `src/engine/expression.rs` - Expression parser (winnow Pratt) and evaluator (1120 lines)
- `src/engine/mod.rs` - Added `pub mod expression;` declaration

## Decisions Made
- Used nested `alt()` to work around winnow's Alt trait tuple limit (max ~9 alternatives per tuple)
- Keywords (and/or/not) checked as whole-word by verifying next char is not alphanumeric/underscore in `keyword()` function
- Keywords also rejected in `parse_field_ref` so Pratt prefix/infix parsers handle them instead of consuming as field names
- All f64 arithmetic results pass through `guard_float()` checking NaN/infinity -- defense-in-depth per STRIDE threat register
- String equality (==, !=) handled as special case before Missing propagation check, allowing String == String comparison

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] winnow Alt tuple limit required nested alt()**
- **Found during:** Task 1 (parser implementation)
- **Issue:** winnow's `Alt` trait only implements for tuples up to ~9 elements; 12 infix operators exceeded this
- **Fix:** Split infix operators into two nested `alt()` groups: comparison+boolean (8) and arithmetic (4)
- **Files modified:** src/engine/expression.rs
- **Verification:** All 23 parser tests pass
- **Committed in:** 1ef29d6

**2. [Rule 1 - Bug] Keywords consumed as field names by parse_field_ref**
- **Found during:** Task 1 (parser implementation)
- **Issue:** `not x` parsed as field `not` with leftover `x` because parse_field_ref matched `not` as identifier before Pratt prefix parser
- **Fix:** Added KEYWORDS array check in parse_field_ref; rejects bare keywords so Pratt parser handles them
- **Files modified:** src/engine/expression.rs
- **Verification:** `test_parse_unary_not` passes; `test_parse_keyword_prefix_field_not_fraud` still passes
- **Committed in:** 1ef29d6

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Both fixes necessary for correct parsing. No scope creep.

## Issues Encountered
- winnow `PResult` type alias is `Result<T, ContextError>` (not ErrMode-wrapped) -- required adjusting error construction in keyword() and parse_field_ref to use plain `ContextError::new()` instead of `ErrMode::Backtrack(...)`.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Expression parser and evaluator ready for pipeline engine integration (Phase 2)
- derive expressions can be parsed at pipeline registration and evaluated per-event
- where-clause filter expressions use the same parse_expr/eval infrastructure
- EvalContext.resolve_field supports all three field reference types needed by views

---
*Phase: 01-core-engine*
*Completed: 2026-04-09*
