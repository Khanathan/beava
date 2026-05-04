# Phase 13.7.5: Pre-OSS Repo Polish — comment audit + test-coverage audit

**Status:** Captured 2026-05-03 mid-execution Phase 13.0 per user directive — "we need a plan to review our repo to prepare for OSS, we need to plan out so that our repo doesnt look like AI Slop by doing component to component review of comments, right now code are overloaded with comments. Also we need to check if our test coverage for every features and ops are good enough."

**Slot in v0 critical path:** Insert between Phase 13.7 (docs site) and Phase 13.8 (packaging + GA tag). Naming: `13.7.5` keeps the v0 launch numbering coherent without renumbering 13.8.

**Why slot here (not earlier):** Phase 13.4 (engine renames + verb-style routes + new opcodes), 13.5 (Python SDK rewrite — DELETEs ~2000 LOC, NEW core client), 13.6 (TS+Go SDKs from scratch), and 13.7 (docs site rendering) all materially change the codebase. Doing comment + test polish before they land would clean code that's about to be rewritten — wasted work. Doing it after means each crate is in its final form.

**Why slot before 13.8 (not after):** v0 ship is the audience checkpoint. First-impression matters; comment density and test rigor are exactly what early reviewers grep for. Cleaning post-ship is reactive ("PR-#1 says: too many comments") instead of proactive.

---

## Two workstreams

### Workstream A — Comment audit (AI-slop removal)

**Goal:** Apply the CLAUDE.md heuristic ("default to no comments; only add when the WHY is non-obvious") retroactively, component by component. Outcome: code reads as engineered, not generated.

**Heuristic codification (the KEEP/DELETE checklist):**

❌ **DELETE patterns:**
- Paraphrasing what the code does (`// loop over users`, `// initialize state`)
- Doc comments on every private function (Rust `///` is valuable on PUBLIC API — not on every helper)
- Section markers (`// === Setup ===`, `// === Cleanup ===`, `// ───────────────`)
- Self-narration sequences (`// First, we ... // Then, we ... // Finally, we ...`)
- Restating the type signature in prose (`/// fn add(a: i32, b: i32) -> i32 — adds two integers`)
- Phase/plan/task references in code (`// added in Phase 13.5 for ...`)
- AI-tell phrases: "Note that ...", "Importantly, ...", "Here we ...", "We then ...", "It should be noted that ..."
- Multi-paragraph docstrings on a 5-line function
- Closing braces with comments echoing the opening (`} // end of for`, `} // matches `if x` above`)

✓ **KEEP patterns:**
- Explains WHY a non-obvious constraint exists ("// must be u32 — Python `struct` lib only handles u32 here")
- Workaround for a specific bug (link the issue / commit / phase)
- Invariant a reader can't infer from the code ("// guaranteed to be > 0 by `validate_input` in caller")
- SAFETY contracts on `unsafe` blocks (Rust idiom)
- Doc comments on PUBLIC lib/SDK exports (cargo doc / Sphinx render targets)
- Hidden-state warnings ("// mutates `self.state` while iterating — careful")
- Single-line "why this isn't obvious" notes

**Per-component plan structure (parallelizable via worktrees):**

| Plan | Component | Notes |
|------|-----------|-------|
| 13.7.5-01 | Conventions doc + heuristic checklist | Codify the rules above; one-page reference for executor agents |
| 13.7.5-02 | `crates/beava-core/` | Most stable; biggest surface; biggest payoff. ops + sketches + agg primitives |
| 13.7.5-03 | `crates/beava-server/` | mio data plane (recent code, likely heavier comments) |
| 13.7.5-04 | `crates/beava-runtime-core/` | Router / wire_request / http_listener |
| 13.7.5-05 | `crates/beava-persistence/` | WAL + snapshot |
| 13.7.5-06 | `crates/beava-bench/` + `beava-bench-v2/` | Bench harnesses (notoriously heavy comments — bench-v2 is 890 LOC uncommitted draft) |
| 13.7.5-07 | `python/beava/` | Post-Phase-13.5 rewrite scrub |
| 13.7.5-08 | `examples/{python,typescript,go}/` | Demo files — should be lean already (Plan 14 just landed); verify |

**Process per plan:**
1. Read every `.rs` / `.py` / `.ts` / `.go` in scope
2. For each comment, apply the heuristic (KEEP / DELETE)
3. Edit in place — keep commits small (one PR-sized commit per crate)
4. Run `cargo test --workspace` (or pytest equivalent) after — must stay green
5. NO logic changes — pure comment hygiene
6. Verify `cargo doc` still renders for Rust crates (public API doc comments preserved)

**Estimated LOC removed:** ~3000-8000 across the codebase (rough — depends on actual density). Each plan likely deletes 200-1500 LOC of comments.

---

### Workstream B — Test coverage audit + gap fill

**Goal:** Verify every feature, every operator, every wire endpoint, every architectural invariant has at least one test exercising it. Identify gaps; fill them.

**Existing test inventory (post-Phase-13.0):**

| Layer | Tests | Source |
|-------|-------|--------|
| Wire-fixture validity | 17 fixtures × Draft 2020-12 schemas | `examples/wire/_validate_examples.py` (Plan 13.0-02) |
| 9 vertical demos (Python/TS/Go) | 9 happy-path E2E | `examples/test_examples.sh` (Plan 13.0-14) |
| 53-op Python integration tests | 54 high-volume integration | `python/tests/v0/test_<family>.py` (Plan 13.0-16) |
| Global-agg + bv.lit Python | 6+3 = 9 | `python/tests/v0/test_global.py` + `test_lit.py` (Plan 13.0-16) |
| Architectural invariants | mio-only / events-only / lifetime-bounds / cold-eviction / metrics | `crates/beava-server/tests/phase12_*.rs` |
| Per-op Rust unit tests | per-AggKind variant | `crates/beava-core/tests/*.rs` |
| Recovery / WAL replay | snapshot round-trip + WAL replay | `crates/beava-server/tests/recovery_*.rs` |
| Bench harnesses | beava-bench / beava-bench-v2 | `crates/beava-bench/src/*` |

**Gap-detection process (Plan 13.7.5-09):**

Build a coverage matrix CSV:

```
| Feature/Op/Endpoint | Wire spec | Doc page | JSON example | Rust unit test | Architectural test | Python integration | Demo | Cross-SDK conformance |
|---------------------|-----------|----------|--------------|----------------|--------------------|--------------------|------|------------------------|
| bv.count (op)       | ✓         | ✓        | ✓            | ?              | ✓                  | ✓                  | ✓    | ✗                      |
| bv.sum (op)         | ✓         | ✓        | ✓            | ?              | ✓                  | ✓                  | ✓    | ✗                      |
| ... (53 ops)        | ...       | ...      | ...          | ...            | ...                | ...                | ...  | ...                    |
| OP_REGISTER         | ✓         | ✓        | ✓            | ?              | -                  | indirect           | ✓    | ✗                      |
| OP_PUSH             | ✓         | ✓        | ✓            | ?              | ✓ (architectural)  | ✓                  | ✓    | ✗                      |
| ... (6 endpoints)   | ...       | ...      | ...          | ...            | ...                | ...                | ...  | ...                    |
| force=True          | ✓         | ✓        | ✓            | ?              | -                  | ✗ (gap?)           | ✗    | ✗                      |
| dry_run=True        | ✓         | ✓        | ?            | ?              | -                  | ✗ (gap?)           | ✗    | ✗                      |
| Schema add (additive) | ✓       | ✓        | ?            | ?              | -                  | ✗ (gap?)           | ✗    | ✗                      |
| Schema diff (destructive 409) | ✓ | ✓     | ?            | ?              | -                  | ✗ (gap?)           | ✗    | ✗                      |
| Cold-entity TTL eviction | ✓    | ✓        | -            | -              | ✓ (12.8)           | ?                  | -    | -                      |
| Lifetime op cap      | ✓        | ✓        | -            | -              | ✓ (12.8)           | ?                  | -    | -                      |
| WAL recovery         | -        | ✓        | -            | ?              | ✓                  | ✗ (Python doesn't replay) | - | -                |
| Snapshot round-trip  | -        | ✓        | -            | ?              | ✓                  | -                  | -    | -                      |
| `bv.fork` / playground | -      | -        | -            | -              | -                  | -                  | -    | -    (REJECTED v0)     |
```

For each `✗` gap: classify as MUST-FIX (blocks v0 launch) vs DEFER (v0.1+).

**Likely gaps to fill (preliminary):**

- `force=True` register flow — Python integration test
- `dry_run=True` register flow — Python integration test
- Schema evolution diff matrix (additive vs destructive) — Python integration test
- App.reset() — Python integration test
- Connection drop + auto-reconnect — Python integration test
- max_retries=0 default behavior — Python integration test
- URL scheme dispatch (`http://` vs `tcp://` vs no-URL embed) — Python integration test
- Cross-SDK conformance (same scenario across Python + TS + Go produces same per-entity values) — NEW test harness
- Per-op Rust unit tests — verify ALL 53 AggKinds have at least one direct unit test (some may rely only on integration)
- WAL replay correctness for every RecordType variant
- Snapshot upgrade path (FORMAT_VERSION mismatch handling)

**Plan structure for Workstream B:**

| Plan | Scope | Notes |
|------|-------|-------|
| 13.7.5-09 | Coverage matrix + gap analysis | Produces `COVERAGE-MATRIX.md`; classifies each gap MUST-FIX vs DEFER |
| 13.7.5-10 | Fill Rust gaps | Per-crate; targeted unit tests for any uncovered AggKind / RecordType / register flow |
| 13.7.5-11 | Fill Python gaps | Extend `python/tests/v0/` — force=True, dry_run=True, App.reset, schema evolution, URL scheme dispatch, retry/reconnect |
| 13.7.5-12 | Cross-SDK conformance harness | NEW — single Python script driving the same scenario across all 3 SDKs (Python + TS + Go); diff their outputs; assert agreement |

---

## Closure plan

| Plan | Scope |
|------|-------|
| 13.7.5-13 | SUMMARY + VERIFICATION + STATE/ROADMAP advance to Phase 13.8 packaging+GA |

---

## Estimated scope

- **Total plans:** 13 (8 comment-audit + 4 coverage + 1 closure)
- **Wall-clock:** ~1-2 weeks with parallelism (comment-audit plans are independent; gap-fill plans depend on the matrix from 13.7.5-09)
- **LOC delta:** likely net-negative (-3000-8000 from comment removal, +500-1500 from gap-fill tests)
- **Test count delta:** +20-50 new tests (rough — depends on actual gaps)

---

## Dependencies / blockers

**Blocked on:** Phase 13.4 + 13.5 + 13.6 + 13.7 must all be CLOSED before 13.7.5 starts. Otherwise we're polishing code that's about to be replaced.

**Not blocking:** Phase 13.8 (packaging + GA tag) can sequentially follow 13.7.5.

---

## Cross-references

- Codifies CLAUDE.md system-prompt heuristic ("default to no comments")
- Companion to ADR-003 (Phase 13.0-15 closure) — adds `bv.lit` + global-agg test coverage to the matrix
- Plan 13.0-16 already shipped 68 Python integration tests — 13.7.5-09 audits whether they cover everything
- Mentions `feedback_logistics_autonomy` — gap classification (MUST-FIX vs DEFER) is the user-facing decision point; everything else is logistics-autonomous
- Tracking item for `.planning/ideas/v0.1-deferrals.md` — any DEFER classification rolls up there

---

## Out of scope for 13.7.5

- Performance benchmarking — `crates/beava-bench` already does this; not a coverage concern
- Security audit — separate concern; consider `/cso` skill or dedicated phase if desired
- Doc style audit (e.g., voice consistency across docs/) — Phase 13.7 owns docs site quality
- Refactoring beyond comment removal — pure hygiene; no logic / structure changes
- Adding new operators / new wire endpoints — scope creep; v0.1+ territory

---

*Capture file. To convert to a real phase: after Phase 13.0 closes, insert ROADMAP §13.7.5 entry referencing this doc and run `/gsd-discuss-phase 13.7.5` when ready (recommended: after Phase 13.7 docs site lands).*
