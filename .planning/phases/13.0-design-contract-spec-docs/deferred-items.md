# Phase 13.0 Deferred Items

Surfaced during Plan 13.0-15 cross-link verification (Task 4):

## Pre-existing broken cross-links to non-existent `13.0-PLAN.md`

6 doc files contain dead links to `.planning/phases/13.0-design-contract-spec-docs/13.0-PLAN.md` (an aggregate meta-plan file that does not exist — Phase 13.0 has 16 individual `13.0-NN-PLAN.md` files, no umbrella plan):

- docs/wire-spec.md:420 (authored by Plan 13.0-02)
- docs/http-api.md:693 (authored by Plan 13.0-03)
- docs/sdk-api/python.md:632 (authored by Plan 13.0-04)
- docs/sdk-api/typescript.md:598 (authored by Plan 13.0-04)
- docs/sdk-api/go.md:609 (authored by Plan 13.0-04)
- docs/sdk-api/shared.md:293 (authored by Plan 13.0-04)

**Disposition:** OUT OF SCOPE for Plan 13.0-15. Per CLAUDE.md section-ownership convention, the closure plan does NOT touch spec docs; these would need a follow-up `docs(13.0-stamp): fix dead 13.0-PLAN.md cross-links` mini-commit (or absorbed into the next phase's first plan).

**Suggested fix:** Replace the 6 occurrences with `[../.planning/phases/13.0-design-contract-spec-docs/](.planning/phases/13.0-design-contract-spec-docs/)` (link to the directory) OR `[13.0 plan tree](.planning/phases/13.0-design-contract-spec-docs/13.0-CONTEXT.md)` (link to the CONTEXT — first stable doc in the directory).
