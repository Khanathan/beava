# Phase 19: Test Migration and Old API Removal - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

Port ALL existing tests (>= 744) to the new @tl.source/@tl.dataset/EventSet/FeatureSet API. Delete @st.stream, @st.view, legacy operator aliases, and _dataframe.py public API from the SDK. Verify no performance regression. Clean break before launch.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — infrastructure phase. Key constraints from STATE.md critical pitfalls:
- C-2: Old API removal breaks 744 tests — port ALL tests first, verify count >= 744, THEN delete.
- C-4: Two APIs being replaced — @st.stream AND _dataframe.py. Test migration covers both.
- Old API removed, not deprecated alongside (clean break before launch — per PROJECT.md decision).
- Order: migrate tests first → verify count → delete old API → verify again → benchmark.

</decisions>

<code_context>
## Existing Code Insights

Codebase context will be gathered during plan-phase research.

</code_context>

<specifics>
## Specific Ideas

No specific requirements — infrastructure phase. Refer to ROADMAP phase description and success criteria.

</specifics>

<deferred>
## Deferred Ideas

None — infrastructure phase.

</deferred>
