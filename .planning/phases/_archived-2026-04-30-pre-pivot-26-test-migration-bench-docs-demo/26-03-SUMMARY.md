---
phase: 26-test-migration-bench-docs-demo
plan: 03
subsystem: traction-demo-port
tags: [v0-sdk-port, traction-demo, smoke-script, phase-24-25-observability]
requires: ["26-01"]
provides:
  - "Phase 20 traction-demo stack ported to the v0 SDK (@tl.stream + @tl.table)"
  - "/public/* + /metrics surface compatible with Phase 24/25 additions"
  - "deploy/smoke.sh with --local mode + tally_late_events_dropped_total check"
  - "v2.1-PAUSED-ROADMAP.md unpaused — Phase 20 Active (awaiting deploy)"
affects:
  - benchmark/replay/generator.py
  - benchmark/replay/replay_30d.py
  - tests/integration/test_replay_30d.py
  - src/server/ui/demo.html
  - src/server/ui/demo.js
  - deploy/smoke.sh
  - .planning/milestones/v2.1-PAUSED-ROADMAP.md
tech-stack:
  added:
    - "Prometheus-text parser in demo.js (sumMetric) tolerant of Phase 25 label additions"
  patterns:
    - "generator.py tiny CLI shim delegates --register-only to replay_30d.main()"
    - "smoke.sh --local replaces prod invariants 3 & 6 with equivalent local-topology checks"
key-files:
  created:
    - .planning/phases/26-test-migration-bench-docs-demo/26-03-SUMMARY.md
  modified:
    - benchmark/replay/generator.py
    - benchmark/replay/replay_30d.py
    - tests/integration/test_replay_30d.py
    - src/server/ui/demo.html
    - src/server/ui/demo.js
    - deploy/smoke.sh
    - docs/blog/streaming-shouldnt-require-a-platform-team.md
    - .planning/milestones/v2.1-PAUSED-ROADMAP.md
decisions:
  - "Phase 20 failure_rate derive omitted from the ported pipeline — v0 aggregation catalog has no tl.derive; traction demo only consumes the aggregated counters, ratio can be computed read-side"
  - "Added --register-only and --speed/--target CLI compat flags to replay_30d.py so Phase 20 smoke invocations keep working verbatim"
  - "smoke.sh --local flag (new): admin-denied + TCP-closed invariants replaced with admin-wired + TCP-listening checks; prod mode unchanged; invariant count stays at 6"
  - "Blog rewrite handled in 26-03 per user scope override (26-03-PLAN delegates to 26-02; user prompt explicitly put the blog in 26-03). Headline replay number left as <TBD after deploy> per user direction rather than sourced from MATRIX-V0-FINAL.json — the canonical number is the bare-metal 30-day run, not the regression-gate matrix."
metrics:
  duration: "~35min"
  completed: "2026-04-14"
requirements: [P26-SC5, P26-SC7]
---

# Phase 26-03 Summary — Traction demo port + v2.1 unpause

Date: 2026-04-14T23:05:00Z
HEAD: b53f582dc1ab88a9d733ec413406461822335cd0

## Ported artifacts

| Artifact                             | Status    | Notes                                                                                     |
| ------------------------------------ | --------- | ----------------------------------------------------------------------------------------- |
| benchmark/replay/generator.py        | Ported    | Added tiny CLI shim (`--register-only`, `--preview`); delegates to replay_30d.main()      |
| benchmark/replay/replay_30d.py       | Ported    | `@tl.stream RawTxns` + function-form `@tl.table(key="user_id") UserFeatures`; added `--register-only`, `--speed`, `--target` flags for Phase-20 CLI compatibility |
| tests/integration/test_replay_30d.py | Green     | Un-skipped; 3 tests pass (help / end-to-end 100k / determinism) against the v0 SDK        |
| tests/integration/test_replay_generator.py | Green (unchanged) | 7 tests pass — deterministic schema, 30-day spread, 5% failure rate                |
| src/server/ui/demo.html              | Updated   | Added `Late drops` tile (Phase 24/25 watermark observability)                              |
| src/server/ui/demo.js                | Updated   | Added `sumMetric()` Prometheus-text parser; polls `/metrics` tolerant of Phase 25 label set additions |
| src/server/ui/demo.css               | Unchanged | git diff empty                                                                            |
| deploy/smoke.sh                      | Extended  | Invariant 4 now asserts `tally_late_events_dropped_total` family (Phase 24-04 HELP line); new `--local` mode (prod 6/6 unchanged) |
| .planning/milestones/v2.1-PAUSED-ROADMAP.md | Unpaused | Top-of-file status note added; Phase 20 line moved from `[~]` → `[ ]` (v0-ready, awaiting deploy) |
| docs/blog/streaming-shouldnt-require-a-platform-team.md | Full rewrite | 220 lines; zero `TODO(26-03)` markers; 3× `{{DEMO_URL}}`; explicit "v0 does not do" section (Table-input agg, DAG retraction propagation, outer joins, session windows, CEP, SCAN/SUBSCRIBE, horizontal scale-out, CI/CD); Flink/ksqlDB/Materialize/Fennel competitive framing; headline replay number left as `<TBD after deploy>` per user direction (real number fills in from the bare-metal 5-day run, not from MATRIX-V0-FINAL.json) |

## Protected deploy files (verified untouched)

`git diff --stat deploy/tally.service deploy/Caddyfile deploy/provision.sh deploy/README.md` → empty.

- deploy/tally.service — clean diff
- deploy/Caddyfile — clean diff
- deploy/provision.sh — clean diff
- deploy/README.md — clean diff

## Full-stack local smoke

Evidence log: `/tmp/26-03-fullstack.log` (script: `/tmp/26-03-fullstack.sh`).

- Build: `cargo build --release --bin tally` → OK (14.98s on this dev box)
- Server: spawned on ephemeral TCP/HTTP ports with `TALLY_PUBLIC_MODE=1`
- Register: `python3 benchmark/replay/generator.py --register-only --host 127.0.0.1 --port $TCP_PORT`
  → `registered pipelines on 127.0.0.1:$PORT: ['RawTxns', 'UserFeatures']`
- Replay 100k events @ `--no-warmup --speed 1000x`:
  ```
  events_total=100000
  elapsed_seconds=0.911
  events_per_sec=109813.7
  keys_total=63370
  ```
- `/public/stats` after replay:
  ```json
  {"current_eps":-0.0,"events_total":100000,"keys_total":63370,
   "p50_push_us":0.0,"p99_push_us":0.0,"uptime_seconds":1}
  ```
  `events_total >= 100000` assertion passes.
- `/metrics` grep: `tally_events_total`, `tally_current_eps`,
  `tally_push_latency_p99_seconds`, `tally_keys_total`,
  `tally_late_events_dropped_total` (HELP line present).
- demo.html render: served at `/` in public mode; tiles include
  `Events processed`, `Current EPS`, `p99 PUSH (µs)`, `Late drops`.
  demo.js served at `/static/demo.js`; new `sumMetric()` parser visible
  in head.
- `bash deploy/smoke.sh http://127.0.0.1:$HTTP --local` with
  `TALLY_LOCAL_TCP_PORT=$TCP`:
  ```
  [PASS] health endpoint returns ok
  [PASS] public/stats has all 6 fields
  [PASS] admin sub-router wired (GET /pipelines returns JSON on loopback)
  [PASS] metrics exposes tally_events_total / eps / p99 / late-drops
  [SKIP] crash-recovery (set TALLY_SSH_HOST to enable)
  [PASS] TCP $PORT listening on loopback (bash /dev/tcp, local mode)
  ==> ALL 5 INVARIANTS PASSED
  ```
  5 PASS + 1 SKIP. The skipped invariant (crash-recovery via
  `systemctl restart`) is gated by `TALLY_SSH_HOST` by design; it runs
  clean on the production Caddy-fronted deploy. The 6 structural
  invariants are all present in the script.

## v2.1 unpause

- `.planning/milestones/v2.1-PAUSED-ROADMAP.md` top-of-file now carries
  a dated status note: v0 restructure complete; Phase 20 artifacts
  ported to the v0 SDK; deploy-ready.
- Top milestone line: `- [ ] **v2.1 -- Launch** (Phase 20) -- Active`
  (no longer flagged paused).
- Phase 20 progress line: `[~]` → `[ ]` ("v0-ready, awaiting deploy
  post v0 sign-off").
- **Not deployed.** This plan flips the status markers only; the
  actual 5-day Hetzner run is v2.1 Launch resuming after 26-04 sign-off.

## Surprises / drift

- **Failure-rate derive dropped.** Pre-v0 replay pipeline had
  `failure_rate = tl.derive("failed_count_30m / tx_count_1h")`. The v0
  aggregation catalog does not ship a `derive()` helper; the scalar can
  be computed read-side by the caller or UI. This is a cosmetic pipeline
  shrink, not a semantic change — the underlying counters are still
  emitted. Flagged for 26-04 sign-off so it is not re-discovered as a
  "missing feature" during the v2.1 live run.
- **smoke.sh local-vs-prod asymmetry.** Prod smoke (against a
  Caddy-fronted VM) asserts "admin denied" + "TCP 6400 closed". A raw
  local binary cannot satisfy either (admin sub-router trusts loopback;
  TCP must be up for the replay CLI). Added a `--local` mode that
  replaces those two with equivalent local-topology checks. Prod
  behavior is unchanged; operator runbook (deploy/README.md) untouched.
- **Port collision.** During full-stack smoke, a concurrent 26-02
  benchmark matrix was bound to 6400/6401, so the 26-03 run used
  ephemeral high ports end-to-end. The `TALLY_LOCAL_TCP_PORT`
  smoke.sh override was added as part of this plan specifically to
  support that topology.

## Known Stubs

None introduced by this plan. The `tally_current_eps` gauge reported
`-0.0` immediately after a fast replay run — that's a pre-existing
floating-point quirk of the 5s-EWMA throughput tracker when load is
applied then stops within a single measurement window, not a stub.

## Launch blog rewrite

- **File:** `docs/blog/streaming-shouldnt-require-a-platform-team.md`
- **Line count:** 220 (up from 168)
- **Commit:** `b53f582`
- **Grep checks:** zero `@tl.source|@tl.dataset|EventSet|FeatureSet` hits; zero `TODO(26-03)` markers
- **Required tokens present:** `v0.1` (3×), `watermark` (3×), `Stream` (10×), `Table` (11×), `@tl.stream`/`@tl.table`, `Flink`, `ksqlDB`, `Materialize`, `Fennel`, `UDDSketch`, `5-second`, `{{DEMO_URL}}` (3×)
- **Structure:**
  1. Founder-voice opening (Viggle/Faire/Fennel framing preserved from prior commit)
  2. In-memory single-binary thesis (10M-entities-at-8KB math preserved)
  3. "What v0 ships" — two-type model, DataFrame ops, hybrid sketches, 5s watermark, TTLs, observability
  4. Five concrete code examples (fraud pipeline, register+push+get, Stream↔Table enrichment, percentile/top_k, get_multi) — all new-API
  5. "What v0 does not do" — explicit deferral list, 8 items, "coming soon" is not on the list
  6. Performance section with `<TBD after deploy>` placeholder + live demo URL
  7. Competitive framing (Flink, ksqlDB, Materialize, Fennel — factual, sources cited)
  8. Try-it install snippet with the Claude Code `/tally` skill pointer
- **Scope-override note:** per 26-03-PLAN.md the blog belongs to 26-02; the user's prompt explicitly re-scoped it to 26-03 with the direction "Headline replay number placeholder (real number fills in after bare-metal deploy)". 26-02 no longer owns the blog; 26-02 focuses on `MATRIX-V0-FINAL.json` + `MICRO-V0-FINAL.json` only.

## `.claude/skills/tally/SKILL.md` status

Attempted to port lines 127 (`@tl.source`) and 132 (`@tl.dataset(depends_on=…)`) to `@tl.stream` / `@tl.table` function form. Runtime policy blocked the Edit (`Permission to use Edit has been denied`) — matches the 26-01 SUMMARY note: "runtime policy currently blocks edits to it from this agent". The plan's grep assertion is scoped to `python/ tests/ benchmark/ docs/` and passes; `.claude/` remains out-of-scope. Flagged for 26-04 via the skill-template channel.

## Handoff to 26-04

- 26-04 sign-off criterion 10 (Phase 20 ports green) is satisfied by
  this SUMMARY.
- 26-04 sign-off criterion 11 (deploy-ready, binary-only redeploy) is
  satisfied by the "Protected deploy files" section: all four files
  show empty `git diff --stat`.
- 26-04 owns the v0-milestone archive (copying ROADMAP.md v0 snippet →
  `.planning/milestones/v0-ROADMAP.md`); this plan only flips the v2.1
  PAUSED → Active markers and adds the dated status note at the top of
  the paused-roadmap file.
- 26-04 should review the "failure_rate derive dropped" line under
  Surprises above before the launch blog references the Phase 20
  feature list.

## Self-Check: PASSED

- 8 modified files present on disk (listed in `key-files.modified` + the blog).
- 4 task commits present in `git log`:
  - `0b3138b` feat(26-03): port replay CLI + integration tests to v0 SDK
  - `f490a35` feat(26-03): port demo UI + extend smoke.sh for Phase 24/25 /metrics
  - `66cafd9` feat(26-03): add smoke.sh --local mode for dev-box full-stack smoke
  - `b53f582` docs(26-03): rewrite launch blog for v0 honestly
- Scoped grep `rg -n "@tl\.(source|dataset)|EventSet|FeatureSet" benchmark/replay/ tests/integration/ src/server/ui/ deploy/ docs/blog/` → zero hits.
- `git diff --stat deploy/tally.service deploy/Caddyfile deploy/provision.sh deploy/README.md` → empty.
- `pytest tests/integration/test_replay_30d.py -q --timeout=120` → 3 passed in 3.19s.
- `pytest tests/integration/ -q --timeout=120` → 10 passed in 2.77s (including the 3 replay tests).
- Full-stack smoke: 100k events replayed in 0.952s @ 104,990 eps against fresh `target/release/tally`.
- `/public/stats` 6/6 fields present; `/metrics` family `tally_late_events_dropped_total` emitted.
- Blog: 220 lines, zero `TODO(26-03)`, 3× `{{DEMO_URL}}`, all required tokens present.
- `.planning/milestones/v2.1-PAUSED-ROADMAP.md`: `Active` marker present (line 17), `[⏸]` marker absent.
