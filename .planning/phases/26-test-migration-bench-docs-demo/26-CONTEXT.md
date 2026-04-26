# Phase 26: Test migration, bench, docs, demo rebuild - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 restructure conversation; final closeout phase

<domain>
## Phase Boundary

Close out the v0 restructure milestone by:

1. **Porting the existing test suite** (≥744 tests per Phase 19 baseline) to the new `@tl.stream`/`@tl.table` SDK surface. Delete any remaining references to old API (`@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet`, `_dataframe.py` public API).
2. **Benchmark regression gate** — run the full 9-cell matrix (`bench_v0.py` from Phase 22-04) on the now-complete v0 engine, verify within −5% of `BASELINE.json`. This is the definitive pre-launch perf check.
3. **Rewrite the launch blog post** (`docs/blog/streaming-shouldnt-require-a-platform-team.md`) to describe the Stream+Table+watermarks+retraction story honestly — no mentions of deferred features as if they shipped.
4. **Rebuild the Phase 20 traction demo** against the new v0 SDK (replay CLI, demo.html, 6-invariant smoke script all need porting). Leave the deploy artifacts (systemd + Caddyfile + provision.sh) as-is — those are port-independent.
5. **Sign-off**: all tests green, bench gate passes, Phase 20 ready to deploy (which unpauses v2.1 Launch).

This phase is closeout; no new user-facing features.

**Out of scope:**
- New operators / new join shapes
- New protocol opcodes
- External CI integration (deferred)
- The actual 5-day live Hetzner deploy (that's v2.1 Launch resuming post-v0)

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Test migration

- Target: ≥744 tests green on the v0 API (Phase 19 migrated to 744; v0 has added ~200 along the way, so ending count ~950+)
- Accept any test skipped earlier in this milestone as `v0-migrated` IF the underlying feature is implemented; **un-skip** them
- Delete any `_dataframe.py` / `EventSet` / `FeatureSet` / `@tl.source` / `@tl.dataset` references (grep assertion: zero results outside archived files)
- Keep v2.0-compat tests only if they test shared infrastructure (snapshot format, protocol framing) — otherwise delete

### Benchmark regression gate

- Run `bench_v0.py` full 9-cell matrix (small/medium/large × 1c/4c/8c)
- Compare to `.planning/phases/22-stream-aggregation-engine/BASELINE.json`
- **Gate**: no cell > 5% regressed. 7-run medians per 22-04 protocol.
- If regression: profile with `cargo flamegraph`; optimize in place; do NOT paper over a regression
- Result captured as `MATRIX-V0-FINAL.json` in the phase dir
- Both Rust criterion benches (sketch micro) AND end-to-end matrix must pass

### Blog rewrite

- File: `/data/home/tally/docs/blog/streaming-shouldnt-require-a-platform-team.md`
- Currently contains placeholder + dev-box numbers
- Replace with the honest v0 story:
  - Two-type model: Stream + Table
  - DataFrame-parity operators
  - UDDSketch/CMS+heap/HLL hybrid sketches with retract semantics
  - 5s watermark event-time handling
  - Single-binary deployment
- Headline number: the full-engine 30-day replay result from the new `bench_v0.py` (recapture on bare metal during Phase 20 traction demo deploy)
- Honest callouts for deferred features (Table aggregation, outer joins, retraction propagation, session windows, CEP) — do NOT overclaim
- Competitive framing: "what Tally does / doesn't do vs Flink / ksqlDB / Materialize / Fennel" — reuse language from `.planning/research/flink-kafka-gap-analysis.md` and `.planning/research/retraction-literature-survey.md`

### Phase 20 traction demo rebuild

All Phase 20 artifacts live under `/data/home/tally/.planning/phases/20-traction-demo/` and the actual code paths are:

- **Replay CLI** — `benchmark/replay/generator.py` + `benchmark/replay/replay_30d.py`. Port from old `@tl.source`/`@tl.dataset` API to `@tl.stream`/`@tl.table`. Tests in `tests/integration/test_replay_*.py` also need porting.
- **Demo frontend** — `src/server/ui/demo.html/css/js`. HTML/CSS unchanged; any JS that references old API schemas needs updating. Minimal surface; mostly just feature-query + metrics polling.
- **Smoke script** — `deploy/smoke.sh` — may reference old API REGISTER payload if any; update. 6 invariants unchanged otherwise.
- **HTTP surface** — `/public/*` endpoints in `src/server/http.rs` — ensure they still work after Phase 25's additions.

Leave alone:
- `deploy/tally.service`, `deploy/Caddyfile`, `deploy/provision.sh` — API-agnostic, no port needed
- `deploy/README.md` — operator runbook, still valid

### Sign-off criteria (all must be TRUE)

- [ ] `cargo test` green (≥1000 tests expected at this point)
- [ ] `pytest python/tests/` green (≥200 tests expected)
- [ ] `pytest tests/integration/` (traction demo replay) green
- [ ] No references to old API in non-archived files (`grep -r "@tl.source\|@tl.dataset\|EventSet\|FeatureSet" --include=*.py python/ tests/ benchmark/ | grep -v "\.planning/"` returns zero)
- [ ] `bench_v0.py` 9-cell matrix within −5% of BASELINE.json (7-run medians for 1c cells)
- [ ] `MATRIX-V0-FINAL.json` committed
- [ ] Criterion sketch benches within targets from Phase 22-03 (UDDSketch insert ≤ 500ns, CMS insert ≤ 200ns, HLL insert ≤ 200ns) on a consistent box
- [ ] `docs/blog/streaming-shouldnt-require-a-platform-team.md` rewritten honestly (no placeholder content, no overclaim)
- [ ] `docs/` site (if exists) updated with v0 SDK reference + migration note
- [ ] Phase 20 traction demo ports: `benchmark/replay/*.py` on new API, `src/server/ui/demo.*` compatible, `deploy/smoke.sh` passes locally
- [ ] Phase 20 deploy-ready (no re-provision needed — just recompile binary, push, deploy)
- [ ] `.planning/ROADMAP.md` v0 milestone marked Complete
- [ ] `.planning/STATE.md` v0 Restructure → Complete, v2.1 Launch → Active (resumable)

### Plan split recommendation

- **26-01**: Test migration (delete old API refs, port any remaining test references, verify test count floor)
- **26-02**: Benchmark matrix gate + criterion sketch bench gate + `MATRIX-V0-FINAL.json` + regression fix if any
- **26-03**: Blog rewrite + Phase 20 traction demo port (replay CLI, demo.html, smoke.sh)
- **26-04**: Sign-off — milestone close, STATE.md update, ROADMAP.md mark complete, prepare v2.1 Launch to resume

</decisions>

<code_context>
## Existing Code Insights

- `/data/home/tally/python/tests/` — Python test suite (check for skipped tests marked `v2_compat` or `v0-migrated`)
- `/data/home/tally/tests/` — Rust integration tests
- `/data/home/tally/benchmark/tally-throughput/bench_v0.py` — v0 benchmark harness (from 22-04)
- `/data/home/tally/.planning/phases/22-stream-aggregation-engine/BASELINE.json` — v2.0 baseline for comparison
- `/data/home/tally/benches/` — Rust criterion benches (uddsketch_ops / cms_ops / hll_ops)
- `/data/home/tally/benchmark/replay/` — Phase 20 replay CLI (pre-v0)
- `/data/home/tally/tests/integration/test_replay_*.py` — Phase 20 integration tests (pre-v0)
- `/data/home/tally/src/server/ui/demo.*` — Phase 20 demo frontend
- `/data/home/tally/deploy/smoke.sh` — Phase 20 smoke script (6 invariants)
- `/data/home/tally/docs/blog/streaming-shouldnt-require-a-platform-team.md` — launch blog, placeholder content

</code_context>

<specifics>
## Specific Ideas

- **Before starting**, run: `grep -rn "@tl.source\|@tl.dataset\|EventSet\|FeatureSet" --include="*.py" python/ tests/ benchmark/ docs/` to get a concrete gap list
- **Bench regression gate** — if any cell misses, use `cargo flamegraph` to find the hot path; common causes from this milestone: watermark tracking overhead (Phase 24), signal registry polling (Phase 25), TTL eviction scan (Phase 25)
- **Blog honesty** — avoid overclaiming; say "v0 ships X, v0.1 adds Y"; the deferred list from v0 spec is long, don't hide it
- **Traction demo port** — should be minimal since 20-01/02/03 code is clean; main work is schema/decorator imports + maybe `tl.col` expression syntax

</specifics>

<deferred>
## Deferred Ideas

- CI/CD integration (GitHub Actions wiring for the regression gate) — v0.1
- Multi-platform testing (macOS + Linux + Windows) — v0.1
- Documentation site generation (MkDocs/Docusaurus) — v0.1 if not already configured
- Actual 5-day Hetzner deploy — this is v2.1 Launch resuming, not Phase 26

</deferred>

---

*Phase: 26-test-migration-bench-docs-demo*
*Sources: `.planning/research/v0-restructure-spec.md`, Phase 20 artifacts, Phase 22 BASELINE.json, Phase 19's test migration pattern*
