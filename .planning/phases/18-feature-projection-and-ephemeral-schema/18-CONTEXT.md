# Phase 18: Feature Projection and Ephemeral Schema - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

Users can control which features appear in PUSH/GET responses via select()/drop() on datasets. RegisterRequest schema extended with ephemeral pipeline fields (projection, ephemeral, ttl, max_keys) using #[serde(default)] for backward compat. Snapshot round-trip must preserve new fields.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — infrastructure phase. Key constraints from STATE.md:
- C-3: All new RegisterRequest fields use #[serde(default)] for backward compat. A v1.3-format RegisterRequest must load on v2.0 server.
- Projection is response-layer filtering — operators still compute all features, projection only filters what's returned.
- Ephemeral fields are schema-only in v2.0 — lifecycle enforcement deferred to post-launch.

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
