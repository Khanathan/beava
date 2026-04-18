# Phase 47 TODO / FIXME / XXX Audit (INFRA-06, D-09)

**Scan date:** 2026-04-17
**Baseline count:** 3 hits (raw, excluding vendor JS)
**Post-audit count:** 0 naked TODOs/FIXMEs/XXXs (all annotated per D-09)

> Note: `src/server/ui/vendor/dagre-d3.min.js` contains ~20 TODO comments in
> minified third-party JavaScript. These are NOT in scope — vendor/generated
> files are excluded from D-09 per Phase 47 scope discipline.

## Disposition ledger

| File:line | Text | Disposition | Rationale / Issue |
|-----------|------|-------------|-------------------|
| `src/client/wire.rs:28` | `// v0.2 TODO: once Phase 29/31 re-plans the protocol module, collapse this file and ...` | keep | Phase 47 audit: keep — design note explaining intentional duplication of Scope/codec between client and server modules; will be addressed when Phase 29/31 protocol re-plan occurs. Rewritten to `// NOTE:`. |
| `src/server/tcp.rs:202` | `/// TODO(Phase 47): remove unlabeled beava_events_total and these fields if dashboards have migrated` | keep | Phase 47 audit: keep — backward-compat preservation note for the unlabeled Prometheus series until dashboards migrate. Annotated as `// TODO(gh-TBD)` tracking item. |
| `src/server/http.rs:356` | `// TODO(Phase 47): remove unlabeled beava_events_total emission` | keep | Phase 47 audit: keep — companion to tcp.rs item above; both must be removed together when dashboards migrate to labeled series. Annotated as `// TODO(gh-TBD)` tracking item. |

## Rules (recap of D-09)

- **fixed** — TODO replaced by real implementation in this plan.
- **keep** — TODO comment rewritten to `// NOTE:` or `// Phase 47 audit: keep — <reason>`; no naked TODO allowed at launch.
- **issue** — GitHub issue to file at launch; TODO annotated with `// TODO(gh-TBD): ...`; issue title + body drafted inline below.

## Issues to file at launch

### GH-TBD-1: Remove deprecated unlabeled beava_events_total Prometheus series

**Files:** `src/server/tcp.rs:202`, `src/server/http.rs:356`

Remove the unlabeled `beava_events_total` Prometheus counter and its backing
fields `events_total` in `Metrics`. Dashboards that currently use the unlabeled
series must migrate to `beava_events_total{proto="http"}` and
`beava_events_total{proto="tcp"}` (labeled series introduced in Phase 45-04).
Hold until operator dashboards have been updated and a migration grace period
has elapsed (suggested: 2 minor versions after 0.1.0).

---

## Clippy follow-up (Phase 47 budget cap)

No budget cap was hit. All 13 clippy errors were within the 30-fix ceiling and
were addressed in Task 3.
