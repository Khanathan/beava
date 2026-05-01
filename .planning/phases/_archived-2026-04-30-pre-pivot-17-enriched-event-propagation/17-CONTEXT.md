# Phase 17: Enriched Event Propagation - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

Downstream datasets can reference upstream computed fields (derives, aggregations) during cascade execution, enabling multi-stage computed features like map -> group_by -> downstream sum("amount_usd"). Enriched fields propagate via a side-channel accumulator (not event clone). Must pass benchmark gate within -5% of 1.1M eps baseline and work correctly under multi-threaded tokio runtime with 8 concurrent clients.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase. Use ROADMAP phase goal, success criteria, and codebase conventions to guide decisions. Key constraints from STATE.md critical pitfalls:
- C-1: Side-channel AHashMap accumulator, never clone serde_json::Value per hop. Gate: <5% regression from 1.1M eps.
- C-5: Enrichment values never re-enter DashMap during downstream push.

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
