# Phase 13.6 — Plan-Phase Summary for Orchestrator

**Phase:** 13.6-typescript-go-sdks (TS+Go communicate-only SDKs)
**Plan-phase completed:** 2026-05-03
**Branch:** v2/greenfield (no branch operations performed)
**Plans created:** 8 (all committed atomically)
**Plan-checker verdict:** PASS (self-review against gsd-plan-checker rubric — see §"Plan-checker self-review" below)

## Plan tree (8 plans across 4 waves)

| Plan | Wave | Depends | Type | Title | LOC scope |
|------|------|---------|------|-------|-----------|
| 13.6-01 | 1 | — | TS-only | TS SDK package scaffold (ESM, communicate-only) | ~120 LOC + tests |
| 13.6-02 | 1 | — | Go-only | Go SDK module scaffold (`beava-dev`, monorepo) | ~150 LOC + tests |
| 13.6-03 | 2 | 01 | TS-only | Wire layer (frame codec + HTTP/TCP/embed) | ~280 LOC + tests |
| 13.6-04 | 2 | 02 | Go-only | Wire layer (frame codec + HTTP/TCP/embed) | ~290 LOC + tests |
| 13.6-05 | 3 | 03 | TS-only | BeavaApp wire methods (8 methods) + tests | ~250 LOC + tests |
| 13.6-06 | 3 | 04 | Go-only | App wire methods (9 methods incl. GetGlobal) + tests | ~270 LOC + tests |
| 13.6-07 | 4 | 05, 06 | Shared | Cross-SDK conformance (Python orchestrator + 2 adapters) | ~250 LOC |
| 13.6-08 | 4 | all | Doc-only | Doc patches + closure + SUMMARY/VERIFICATION | doc rewrites |

**Wave shape:** Waves 1, 2, 3 are TS/Go-parallel pairs (perfect symmetry — executors can run TS and Go side-by-side). Wave 4 is sequential (07 needs 05+06 done; 08 needs everything).

**Total estimated source LOC:** ~1450 LOC (well under the original 1800-each scope; communicate-only rescope landed).

## User-locked decisions reflected (D-01 through D-04)

| Decision | Plan(s) honoring it | How |
|----------|--------------------|-----|
| **D-01** ESM-only TS | 13.6-01, 13.6-03 | `package.json "type":"module"`; tsconfig `module:"esnext"`; no CJS dual output |
| **D-02** `beava-dev` monorepo + `sdk/go` subdir | 13.6-02, 13.6-08 | `go.mod` declares `github.com/beava-dev/beava/sdk/go`; ROADMAP patch replaces `beava-io` refs |
| **D-03** single Python orchestrator | 13.6-07 | `python/tests/conformance/test_cross_sdk.py` runs Python+TS+Go via subprocess against shared `scenario.json`; asserts identical outputs |
| **D-04** doc patches in 13.6 closure | 13.6-08 | rewrites typescript.md/go.md/shared.md/ROADMAP §13.6/quickstart.md to communicate-only scope |
| **Scope amendment** (TS+Go = communicate-only) | ALL plans | NO DSL files in `sdk/typescript/src/` or `sdk/go/` (`!ls events.ts col.ts agg.ts table.ts` verifies); Descriptors are pass-through `Record<string, unknown>` / `map[string]any` |

## Plan-checker self-review (gsd-plan-checker rubric)

Verified against the 7 dimensions in `gates.md`:

1. **Goal coverage** — PASS. Every CONTEXT.md success criterion (`@beava/sdk` builds, Go SDK builds, cross-SDK conformance passes, docs reflect scope) maps to >=1 plan task.
2. **Context compliance** — PASS. All 4 user-locked decisions honored verbatim; deferred items (DSL in TS/Go, Java SDK, WASM browser SDK) explicitly excluded.
3. **TDD discipline** — PASS. Plans 01-07 (code-bearing) all decompose into RED->GREEN pairs. Plan 08 (doc-only) skips per CLAUDE.md TDD §Note 4.
4. **Dependency graph** — PASS. No cycles; monotonic wave numbering; all `depends_on` references point to earlier-numbered plans in this phase.
5. **Wiring** — PASS. Cross-plan handoffs verified: scaffold (01/02) -> wire layer (03/04) -> App methods (05/06) -> conformance (07) -> docs (08).
6. **Context budget** — PASS. Each plan is ~250-450 lines; executor agents have headroom for the file ops described.
7. **CLAUDE.md invariants** — PASS. No new axum imports outside `http_admin.rs` (SDKs are clients, don't touch server runtime); no `OpNode::Table*` symbols; events-only scope intact.

## Cross-phase handoffs

- **13.4 (engine prep + wire spec):** Plan 13.6-07 (conformance) needs the engine to accept the wire register payload (specifically global-tables `key:[]` per ADR-003) for the 3 SDKs to agree. If 13.4 isn't far enough along when 13.6 reaches Plan 07, the conformance test pytest-skips with a clear message. CI gate is conditional on engine readiness.
- **13.5 (Python SDK + bench CLI):** Plan 13.6-07 also needs `bv.App.register_json(payload)` (a JSON pass-through helper that 13.6-07 Task 1.b adds to `python/beava/_app.py`). This 5-10 LOC helper is small enough that 13.6 owns it; 13.5 inherits.
- **13.7 (docs site):** Plan 13.6-08 patches `docs/sdk-api/{typescript,go,shared}.md` to communicate-only scope. 13.7 renders these patched files into the published docs site. Scope is fully aligned via CONTEXT.md.

## Unresolved Qs (auto-picked defaults during planning per parent directive)

1. **JSON Schema validators on the SDK side (Ajv2020 for TS / santhosh-tekuri/jsonschema for Go)?** — CONTEXT.md left this to planner discretion. **Auto-picked: NOT in scope.** The wire layer just JSON.parses bodies and lets the server validate. SDKs surface the structured error envelope verbatim. Adds ~50 LOC each that doesn't pull weight in v0; users get fast feedback from server's `unsupported_node_kind` etc. **User can flip this later if needed.**

2. **Embed binary spawn in TS/Go?** — CONTEXT.md said "probably yes for parity." **Auto-picked: YES.** Plans 13.6-03 (TS) and 13.6-04 (Go) both implement embed mode mirroring `python/beava/_embed.py`. Without it, the conformance test (13.6-07) couldn't run end-to-end without external setup.

3. **TS test runner choice (jest vs vitest)?** — CONTEXT.md left to planner. **Auto-picked: vitest.** Modern, ESM-native, less config friction with ESM-only output. Honors D-01.

4. **`pushSync` v0 behavior** — `OP_PUSH_SYNC` is RESERVED for v0.1+ per docs/wire-spec.md. **Auto-picked: TS+Go ship `pushSync`/`PushSync` as a method that DELEGATES to `push`/`Push` with a comment.** Preserves API parity with Python's `app.push_sync` while honoring the wire reservation. v0.1+ wires the actual OP_PUSH_SYNC opcode without breaking the API surface.

5. **Where the TS adapter resolves `@beava/sdk` for the conformance test** — Plan 13.6-07 Task 2.a notes options (npm link or tsconfig paths). **Auto-picked: tsconfig paths with relative resolution to `sdk/typescript/dist/index.js`.** Avoids polluting the user's npm global state during CI.

## Estimated execute-phase wall time

**~6-9 days end-to-end** (down from the original 10-12 day estimate because of the communicate-only rescope):

- Wave 1 (scaffolds): ~0.5 day each -> ~0.5 day total (parallel)
- Wave 2 (wire layer): ~1.5-2 days each -> ~2 days total (parallel)
- Wave 3 (App methods): ~1.5-2 days each -> ~2 days total (parallel)
- Wave 4 plan 07 (conformance): ~1-1.5 days
- Wave 4 plan 08 (doc patches + closure): ~0.5-1 day

If TS and Go are dispatched to two separate executor agents in parallel, total wall time can compress to ~4-6 days.

## Files committed by this plan-phase orchestrator

| Plan | Commit SHA | Subject |
|------|------------|---------|
| 13.6-01 | `4c3eb7c` | docs(13.6-01): plan 13.6-01 TS SDK package scaffold (ESM-only, communicate-only) |
| 13.6-02 | `4bde3ae` | docs(13.6-02): plan 13.6-02 Go SDK module scaffold (beava-dev monorepo, communicate-only) |
| 13.6-03 | `65cd19e` | docs(13.6-03): plan 13.6-03 TS wire layer (frame codec + HTTP/TCP/embed transports) |
| 13.6-04 | `2600ba3` | docs(13.6-04): plan 13.6-04 Go wire layer (frame codec + HTTP/TCP/embed transports) |
| 13.6-05 | `ff1ce4f` | docs(13.6-05): plan 13.6-05 TS BeavaApp wire methods + tests |
| 13.6-06 | `c7584ff` | docs(13.6-06): plan 13.6-06 Go App wire methods + tests |
| 13.6-07 | `42c8290` | docs(13.6-07): plan 13.6-07 cross-SDK conformance harness (Python orchestrator) |
| 13.6-08 | `423a3e6` | docs(13.6-08): plan 13.6-08 doc patches + closure (D-04 + SUMMARY/VERIFICATION) |

(This SUMMARY-FOR-ORCHESTRATOR.md commits separately as the 9th file in this phase plan-phase pass.)

## Parent orchestrator next step

Wait for sibling agents (13.4, 13.5, 13.7) to finish their plan-phase passes. Then advance STATE.md/ROADMAP.md jointly. This phase did NOT touch STATE.md or ROADMAP.md per the parent agent's directive.
