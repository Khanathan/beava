# v0 OSS Launch Plan — All Remaining Polish + Fixes

**Generated:** 2026-05-05 (post-Phase-13.7.5 closure, pre-13.7.6 execution)
**Source signals:** `.planning/phases/13.7.6-pre-oss-repo-polish/13.7.6-CONTEXT.md` (23 plans authored), `WEBSITE-GAPS.md` (46 documented gaps), Phase 13.7.5 verifier findings, throughput-methodology bug surfaced 2026-05-05.
**Status:** **Decisions locked 2026-05-05**; Wave 1 of 13.7.6 ready to execute.

## Locked decisions (5)

| # | Decision | Locked answer |
|---|----------|--------------|
| 1 | Phase 13.7.7 insertion | **D — fold throughput methodology + Gap 32 + Gap 25 into 13.7.6**; defer Gap 38 / 28 / 31 to v0.0.1 |
| 2 | Decorator kwargs (Gaps 28 + 31) | **(b) — Raise `NotImplementedError`** when these eventually land (v0.0.1+, NOT v0) |
| 3 | README throughput claim (Gap 43) | **A — Cite Apple-M4 honestly + hardware footnote** |
| 4 | beava-website Tier 0/1 docs drift | **A — Drive in parallel** with main-repo 13.7.6 (separate work stream, not main-repo phase scope) |
| 5 | Launch-blocker fix order vs history rewrite | **Before — fixes on unrewritten history**, then `filter-repo` runs once |

## Sustained-mode bench bug surfaced 2026-05-05

While confirming the throughput-methodology hypothesis, the `beava-bench-v18 --duration-secs 60` invocation **without** `--total-events` cap **hung at 0.1% CPU for 14+ minutes** before being killed. This is bigger than a measurement-methodology bug — the sustained-mode path has a deadlock or stall. Folded into the throughput methodology fix (now 13.7.6-27 scope).

## Executive summary

Phase 13.7.5 closed PASS. Three workstreams remain to v0 GA:

1. **13.7.6 (existing 23 plans + 3 new bench fixes)** — pre-OSS security + commit-path + public-facing files. ~3-5 days.
2. **13.7.7 (NEW — propose insertion)** — pre-OSS launch-blocker fixes + throughput-methodology fix + honest rebaseline. ~2-3 days.
3. **13.8 (existing 12 plans)** — packaging + GA tag. ~5-7 days. Adjust README throughput claims after 13.7.7 honest rebaseline.

Plus **separate beava-website repo work** (Tier 1 docs drift, Gaps 1-23) — not in main-repo scope.

Total to v0 GA: **~10-15 days wall-clock, solo.** Faster with parallel execution where safe.

---

## Phase 13.7.6 — Pre-OSS repo polish (23 + 3 new = 26 plans)

### Wave 1 — Workstream C (security audit + lint sweep) — 8 plans

All audit-style; produce findings; mostly autonomous.

| Plan | Scope | Tool | Status |
|------|-------|------|--------|
| 13.7.6-01 | `cargo clippy --workspace -D warnings` + audit every `#[allow(...)]` for justification comment | clippy | clippy GREEN; **64 allow-attrs to classify KEEP/REMOVE/ADD-COMMENT** |
| 13.7.6-02 | `cargo audit` (RustSec) + `cargo deny check` (license/advisory policy) | cargo-audit + cargo-deny | tools installed ✓ |
| 13.7.6-03 | `ruff check beava/` + `mypy --strict python/beava` | ruff + mypy | mypy GREEN; **3 ruff lints (1 auto-fixable I001 + 2 B905)** |
| 13.7.6-04 | `tsc --noEmit` (TypeScript SDK) + `eslint` if configured | tsc | needs verify in `sdk/typescript/` |
| 13.7.6-05 | `go vet ./...` (mandatory) + `staticcheck ./...` (optional) | go | go vet GREEN; staticcheck not installed |
| 13.7.6-06 | OWASP Top-10 (2021) + LLM Top-10 review of v0 attack surface | analytical (no tool); /cso skill optional | spawn-as-agent |
| 13.7.6-07 | ASVS L1 threat-model narrative (single page) | analytical | spawn-as-agent |
| 13.7.6-08 | gitleaks + trufflehog + git-secrets across full git history (~2443 commits) | 3 tools | tools installed ✓ |

### Wave 1 — Workstream E (public-facing files) — 9 plans

Doc-only rewrites/audits; spawn as parallel doc-only agents (per memory `feedback_parallel_cargo_agents_stall`, doc-only worktrees are safe).

| Plan | Scope | Notes |
|------|-------|-------|
| 13.7.6-14 | LICENSE audit (existing 199 LOC; verify Apache-2.0 canonical) | mostly verify |
| 13.7.6-15 | **README.md rewrite** | **AMENDED:** must address Gap 43 (throughput claim). Drop "315K EPS TCP / 100K+ EPS HTTP" until 13.7.7 produces honest measured numbers; add hardware disclosure footnote |
| 13.7.6-16 | CONTRIBUTING.md rewrite (D-06 USER OVERRIDE: skip public architectural-invariant tripwire docs) | per locked decisions |
| 13.7.6-17 | SECURITY.md audit + refresh (existing 43 LOC) | per D-08 Workstream-E |
| 13.7.6-18 | CODE_OF_CONDUCT.md audit + refresh (existing 19 LOC) | per D-08 Workstream-E |
| 13.7.6-19 | CHANGELOG.md create (Keep-a-Changelog 1.x format) | new file |
| 13.7.6-20 | .gitignore audit (existing 71 LOC) — IDE configs / OS junk / common gaps | additive |
| 13.7.6-21 | .github/ templates — issue templates + PR template | 4 template files |
| 13.7.6-22 | .github/workflows audit — public-launch readiness (5 workflows) | verify CI surface |

### Wave 2 — NEW PLANS for Gaps 43-45 (bench tooling) — 3 plans

| Plan | Scope | Justification |
|------|-------|---------------|
| **13.7.6-24 (NEW)** | **Strip the lying `--parallel` flag from `beava-bench throughput` CLI.** Option B per WEBSITE-GAPS.md §44: rename to `--smoke-test-only` or remove from `--help`. Add comment in CLI help pointing users to `beava-bench-v18` for production benchmarking. Update the `let _ = parallel;` site at `crates/beava-bench/src/harness/mod.rs:44-46` to either gate behind `--smoke` or hard-error if `--parallel > 1`. Affects: `crates/beava-bench/src/cli/throughput.rs` + `crates/beava-bench/src/harness/mod.rs` + tests. | Gap 44: 100× discrepancy between `beava-bench throughput --parallel 32` (1,084 EPS smoke) and `beava-bench-v18 --parallel 32` (124K EPS real). Public-CLI lies are launch-blockers. |
| **13.7.6-25 (NEW)** | **Fix Reproduce section in `.planning/throughput-baselines.md`.** Rewrite the at-top "Reproduce" block to use the actual working invocation (`cd crates/beava-bench && ../../target/release/beava-bench-v18 --pipeline X --transport tcp --wire-format msgpack --duration-secs 60 --parallel 32`). Optionally add `default-run = "beava-bench-v18"` to `crates/beava-bench/Cargo.toml` so plain `cargo run -p beava-bench` works. Document "polished CLI is smoke-test; v18 is production" sidebar. | Gap 45: committed baselines doc can't be reproduced as written. Disqualifies the committed numbers' credibility. |
| **13.7.6-26 (NEW)** | **`crates/beava-bench/README.md` create.** Per-binary guidance: which of the 4 binaries (`beava-bench`, `-legacy`, `-v18`, `-v2`) to use for which workflow. Decision tree from WEBSITE-GAPS.md §44. | Gap 44 secondary: README references this dir for "reproducible numbers" but no per-binary guidance exists. |

### Wave 2 — Workstream D rehearsal (1 plan, non-destructive) — 1 plan

| Plan | Scope |
|------|-------|
| 13.7.6-09 | filter-repo dry-run rehearsal on bare clone at `/tmp/beava-rehearsal-bare.git/`. Produces 3 artifacts (`mailmap.txt`, `trailer-strip.py`, `rehearsal-report.md`) for Plan 11 to consume verbatim. No effect on dev tree. |

### Wave 1 — Workstream Misc

| Plan | Scope |
|------|-------|
| 13.7.6-10 | Branch cleanup — strip stale `worktree-agent-*` branches accumulated under `.claude/worktrees/`; ~30+ stale entries from past sessions. Pure git plumbing. |

### Wave 3-5 — Workstream D (DESTRUCTIVE — STOP for explicit user auth)

| Plan | Scope | Auth |
|------|-------|------|
| 13.7.6-11 | `git filter-repo` execution on bare clone: strip `.planning/` + `CLAUDE.md` + `.claude/` from history; trailer-only commit-message scrub; mailmap rewrite (author email → `hoang@beava.dev`) | **STOP — checkpoint, autonomous: false** |
| 13.7.6-12 | Branch rename `v2/greenfield → main` on rewritten bare clone | depends on 11 |
| 13.7.6-13 | Force-push rewritten bare clone to `git@github.com:beava-dev/beava.git` (canonical public repo); enable repo at GitHub level; rename `tally → beava` under `beava-dev` org | **STOP — checkpoint, autonomous: false** |

### Wave 6 — Closure

| Plan | Scope |
|------|-------|
| 13.7.6-23 | Phase closure: SUMMARY + VERIFICATION + STATE/ROADMAP advance to 13.8 (or 13.7.7 if user approves insertion). Single doc-only commit per CLAUDE.md TDD §item #7. |

---

## Phase 13.7.6 — Workstream G+H FOLDED IN (was proposed Phase 13.7.7)

Per Decision 1 (D), the throughput-methodology fix + Gap 32 + Gap 25 fold into 13.7.6 as Wave 1.5 plans 27-31. Per Decision 5 (Before), they land BEFORE the destructive Plans 11/12/13.

### Wave 1.5 — Folded launch-blocker fixes (5 plans)

| Plan | Scope | Justification |
|------|-------|---------------|
| **13.7.6-27 (NEW)** | **Fix `beava-bench-v18` sustained-mode hang + measurement methodology.** Two parts: (a) **debug the deadlock** in the `--duration-secs 60` (no `--total-events` cap) path — process hangs at 0.1% CPU; root-cause likely a worker-shutdown signal that only fires on event-cap exhaustion. (b) **separate burst-vs-sustained semantics**: rename `sustained_eps` to `burst_eps` when `elapsed < duration_secs * 0.95`; emit `sustained_eps` only when the full duration ran. | Phase 13.7.5 measurement bug confirmed 2026-05-05; bench-v18 hang reproduced same day. Without fix, all committed throughput numbers since Phase 12 are 1.5-second bursts. |
| **13.7.6-28 (NEW)** | **Honest 8-cell throughput rebaseline** (post-27 fix). Run `--duration-secs 60 --parallel 32` for 4 shapes × 2 transports. Append "Phase 13.7.6 — methodology-correct sustained rebaseline" section to `.planning/throughput-baselines.md`. Both burst-rate (legacy comparison) and sustained-rate per cell. | Establishes honest baselines for 13.8 README + ship-pitch. |
| **13.7.6-29 (NEW)** | **Update README.md throughput claim per Decision 3-A.** Replace "315K EPS TCP / 100K+ HTTP" with measured-on-Apple-M4 sustained numbers from 13.7.6-28 + hardware disclosure footnote. Coordinated with Plan 13.7.6-15 (which becomes "structural rewrite"; Plan 29 is "throughput-paragraph rewrite"). Plan 15 must run AFTER Plan 28 lands, OR Plan 15 ships with `<TODO: post-28 numbers>` placeholder for Plan 29 to fill. | Gap 43 cleanup. |
| **13.7.6-30 (NEW)** | **Fix Gap 32 — TCP `send_push` swallows engine error frames as success.** `python/beava/_transport.py::TcpTransport.send_push` doesn't check the response opcode; user gets `{"error":{...}}` returned as success. Add opcode check + raise on `OP_ERR_RESPONSE`. Add regression test in `python/tests/v0/test_tcp_error_path.py` (NEW). | **HIGH — silent data loss** on TCP transport. User pushes invalid event, gets no error. |
| **13.7.6-31 (NEW)** | **Fix Gap 25 — TS+Go SDKs send `/batch-get` (hyphen); engine accepts `/batch_get` (underscore).** Update `sdk/typescript/src/transport.ts` + `sdk/go/transport.go` to use `_` form. Add cross-SDK URL test (against real engine, not mock). | **HIGH — silent failure for non-Python users**. Non-Python SDKs 404 against real engine. |

**Deferred to v0.0.1** (per Decision 1-D + Decision 2-b):

- Gap 38 — `app.ping()` over HTTP raises NotImplementedError → v0.0.1 hotfix
- Gap 28 — `@bv.event(dedupe_key=, dedupe_window=)` accepted but inert → v0.0.1 raise NotImplementedError per Decision 2-b
- Gap 31 — `@bv.event(cold_after=)` accepted but inert → v0.0.1 raise NotImplementedError per Decision 2-b

---

## Phase 13.8 — Packaging + GA tag (existing 12 plans, no changes)

Per `.planning/phases/13.8-packaging-and-ga-tag/`. Ships unchanged; runs after 13.7.6 + 13.7.7 close.

12 plans:

1. PyPI multi-arch wheels (4 platforms: Linux x86_64 / Linux ARM64 / macOS ARM64 / macOS Intel; defer Windows + musl per D-8-C)
2. npm `@beava/sdk` publish (TS SDK)
3. Go module verify (`github.com/beava-dev/beava/sdk/go`)
4. Docker Hub + ghcr.io multi-arch manifest
5-7. Skipped (renumbered) or sub-plans
- 04a (NEW from earlier session): curl|sh installer at github.com/beava-dev/beava/releases/latest/download/install.sh
- 04b (NEW from earlier session): Homebrew tap auto-bump on tag push
8. release.yml multi-channel orchestrator
9. Cargo.toml version bump 0.1.0 → 0.0.0
10. README.md hero rewrite (post-13.7.7 honest numbers)
11. Marketing drafts (HN + Twitter + dev.to) — D-8-F
12. SOAK-LOG.md (post-tag soak window)
13. Closure — tag v0.0.0 on main; force-push if needed

**Manual prereqs (from earlier HANDOFF.json — non-blocking 13.7.6/13.7.7):**

- Claim Docker Hub `beava` namespace
- Generate Docker Hub access token + set `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` GitHub secrets
- Claim npm org `beava` at https://www.npmjs.com/org/create
- Generate npm token OR set up Trusted Publishers (preferred per Plan 13.8-02 D-05)
- (Optional) DNS for beava.dev — only if vanity URLs desired

---

## Out-of-scope: beava-website docs drift (Gaps 1-23)

The website at `beava.dev` (separate repo `beava-website/`) has 23 docs-vs-code drift gaps. These are **not** in main-repo phase scope but **must land** before launch announcement.

**Tier 0** (block any user from getting started):

- Gap 0 — `pip install beava` 404 (handled by Phase 13.8 PyPI publish)
- Gap 2 — quickstart `e.agg()` no `group_by` — headline 60-second example doesn't run as written
- Gap 3 + 4 — chained `bv.App(...).register(...).serve()` server-bind line in quickstart; `App` has no `.serve()` method

**Tier 1** (broken docs surfaces):

- Gaps 7, 12, 13, 14, 15, 16+17, 18, 21+22+24 — see WEBSITE-GAPS.md "Recommended fix order"

**Tier 2** (small docs surface):

- Gaps 5, 6, 9 — single-line doc fixes

**Recommendation:** create a `beava-website/.planning/` phase parallel to 13.7.6/13.7.7/13.8 to drive these docs fixes. Soft-launch (D-8-B: publish day-0, announce ~7-14d later) gives a window to land website fixes between PyPI publish and HN announcement.

---

## Out-of-scope: v0.0.x hotfix backlog

Tracked in `.planning/ideas/v0.1-deferrals.md`:

- Gap 1 — `bv.__version__` add to `__init__.py` (1 line)
- Gap 10 — `bv.Optional` re-export (1 line)
- Gaps 34-36 — `register` semantics (full-state replacement vs incremental; can't add field without `force=True`; `optional_fields` can't evolve) — design questions
- Gap 11 — composite-key `app.get(..., [k1, k2])` engine work
- Gap 8 — `bv.test.fixture` clock-controllable (`f.advance_time`) — feature work
- Phase 13.7.5 carry-forward: `docs/operators/index.md` heading off-by-one (53 → 54)
- Phase 13.7.5 carry-forward: fraud-team/tcp throughput WARN — quiescent re-measurement (now superseded by 13.7.7-01 methodology fix)
- Phase 13.6 carry-forward: cross-SDK conformance harness payload bug (sends `kind:"table"` instead of derivation `output_kind=table`)
- Phase 13.4 carry-forward: HTTP-transport throughput regression −24% to −32%

---

## Execution timeline (locked per Decisions 1-5)

```
day 0      Wave 1 of Phase 13.7.6 (parallel-safe, no destructive ops)
           → Workstream C audits (Plans 01-08)
           → Workstream E doc rewrites (Plans 14-22; Plan 15 deferred to post-28)
           → Bench fixes (Plans 24-26 NEW)
           → Workstream Misc (Plan 10 branch cleanup)
           → Plan 09 filter-repo dry-run rehearsal (non-destructive)

day 1-2    Wave 1.5 of Phase 13.7.6 (folded-in launch-blocker fixes per Decision 1-D)
           → Plan 27 NEW — bench-v18 sustained-mode hang debug + methodology fix
           → Plan 28 NEW — honest 8-cell rebaseline
           → Plan 29 NEW — README throughput-paragraph update (per Decision 3-A)
           → Plan 30 NEW — Gap 32 TCP swallow fix
           → Plan 31 NEW — Gap 25 TS+Go batch-get URL fix
           → Plan 15 README structural rewrite (now AFTER Plan 28/29 land)

day 2      STOP — explicit user auth required for Plans 11/12/13
           (destructive: filter-repo history rewrite + branch rename + force-push)

day 2-3    Plans 11/12/13 with per-step auth
           → Plan 23 closure of Phase 13.7.6 (now ~32 plans total)

day 3-8    Phase 13.8 — packaging + GA tag (12 plans, unchanged)
           → multi-arch wheels, npm/Docker/GH Releases/Homebrew
           → README hero finalised with honest numbers
           → v0.0.0 tag

day 8-22   Soft-launch window (per D-8-B)
           → Public repo live; PyPI/npm/Docker/Homebrew installable
           → Announcement (HN + Twitter + dev.to) at T+7-14d

PARALLEL   beava-website Tier 0 + Tier 1 docs drift fixes (per Decision 4-A)
day 0-2    → Separate `beava-website/` repo
           → Tier 0 (Gaps 0/2/3/4): headline 60-second quickstart fix
           → Tier 1 (Gaps 7-22): per-page rewrites
           → Lands before announcement T+7-14d
```

**Critical-path dependencies:**

- 13.7.7 throughput rebaseline (Plans 01-02) **MUST** land before 13.7.6-15 README rewrite finalizes; otherwise the README ships with placeholder claims OR the inflated 315K number.
  - Option A: reorder 13.7.6-15 to AFTER 13.7.7-02 (defer to 13.7.6-23 closure or 13.7.7-09)
  - Option B: 13.7.6-15 ships with `<TODO: honest measured numbers from 13.7.7-02>` placeholder
  - Recommended: Option A.

- 13.7.7-04 (TCP swallow fix) updates `python/beava/_transport.py` — coordinate with anyone doing Python work concurrently to avoid merge conflicts.

- 13.7.6-13 force-push to `beava-dev/beava` is the **point-of-no-return** for the public history. Triple-check Plan 09 dry-run artifacts before authorizing 13.7.6-11.

---

## User decision points

Before execution starts, please confirm or amend:

### Decision 1 — Phase 13.7.7 insertion

- **A. Approve as proposed** (insert between 13.7.6 and 13.8; ~2-3 days)
- **B. Fold 13.7.7 plans into 13.7.6** (extends 13.7.6 to ~5-7 days; one closure)
- **C. Defer all 13.7.7 SDK fixes to v0.0.1 hotfix** (ship v0 with the silent-failure bugs intact)
- **D. Fix only throughput methodology + Gap 32 + Gap 25 in 13.7.6; defer Gap 38 + 28 + 31** (compromise; honest numbers + critical SDK bugs)

**Recommendation: D.** Keeps v0 launch surface honest while not blocking on decorator-design questions.

### Decision 2 — Decorator-kwargs Gap 28 + 31 (`dedupe_key`, `cold_after`)

- **(a)** Implement data-plane dedupe + cold-after (real engine work, ~3-5 days each) — DEFER to v0.1
- **(b)** Raise `NotImplementedError` at decorator time so users get a clear signal in v0 — fix in v0 (~10 LOC each)
- **(c)** Document as "v0 accepts but no-ops; v0.1 implements" — current state, half-fix

**Recommendation: (b).** v0-clean; ~20 LOC total; users get the signal.

### Decision 3 — README hardware disclosure (Gap 43 fix)

- **A. Cite Apple-M4 numbers honestly** (~125K EPS sustained TCP / ~92K HTTP); add hardware footnote
- **B. Re-measure on launch hardware** (specify the launch box; cite that hw)
- **C. Drop the throughput claim entirely from README hero** (move to docs); pivot hero to a different value-prop

**Recommendation: A.** Honest, concrete, reproducible. Users on faster hw measure higher; users on equivalent hw match the number.

### Decision 4 — beava-website Tier 0/1 docs drift parallel work

- **A. Drive in parallel during Phase 13.7.6/13.7.7 execution** (separate workstream; doesn't block main-repo critical path)
- **B. Sequence after 13.7.6 lands** (clean main-repo first; docs later)
- **C. Leave to soft-launch window** (publish broken docs day-0; fix during the 7-14d soak)

**Recommendation: A.** Tier 0 gaps (0/2/3/4) are the headline quickstart; users hit them in the first 60 seconds. Fix before announcement.

### Decision 5 — Phase 13.7.7 trigger order

- **A. 13.7.7 starts BEFORE 13.7.6 destructive ops (Plans 11-13)** so launch-blocker fixes land on the unrewritten history (cleaner provenance)
- **B. 13.7.7 starts AFTER 13.7.6 history rewrite** (fixes land on the rewritten public-only history; smaller diff to push)

**Recommendation: A.** Land all v0-blocking fixes before history rewrite; only the destructive ops land on the cleaned history.

---

## Artifacts produced by this plan

When complete, the v0 OSS launch will have:

- `github.com/beava-dev/beava` (renamed from `tally`) — public, history-rewritten, branch `main`
- PyPI `beava` package, multi-arch wheels (4 platforms)
- npm `@beava/sdk`, ESM-only TS SDK
- `github.com/beava-dev/beava/sdk/go` Go module
- Docker Hub `beava/beava` + ghcr.io mirror, multi-arch manifest
- `github.com/beava-dev/homebrew-beava` tap (already created previous session)
- GitHub Releases × 3 platforms + curl-installable `install.sh`
- `beava.dev` website with Tier 0/1 docs drift fixed
- `v0.0.0` tag everywhere
- README with honest measured throughput numbers + hardware disclosure
- Public CHANGELOG, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT
- Marketing drafts ready for HN + Twitter + dev.to (release at announcement T+7-14d)
