# Phase 13.7.6: Pre-OSS Security Audit + Commit-Path Sanitization + Public-Facing Files

**Status:** Captured 2026-05-04 mid-execution per user directive — "we need to also run clippy on our repo. But yeah looks good. For commit please remove claude code from all commit and only place me as commit. Dont show AI in all commits."

**Slot in v0 critical path:** Insert between Phase 13.7.5 (comment-audit + test-coverage audit, captured 2026-05-03) and Phase 13.8 (packaging + GA tag). Companion to 13.7.5: 13.7.5 cleans the **code surface**; 13.7.6 cleans the **repo surface** (history, public files, dependencies).

**Naming:** `13.7.6` keeps v0 launch numbering coherent. Sequential after 13.7.5 (depends on 13.7.5's clippy-of-record state — running clippy after comment audit avoids re-running on code about to change).

---

## Four workstreams

### Workstream C — Security audit + lint sweep

**Goal:** Establish a clean security + quality baseline before public publication. No CVEs in deps; no AI-detected dead-code or anti-patterns from clippy; threat model documented and mitigations verified.

**Plans:**

| Plan | Scope | Notes |
|------|-------|-------|
| 13.7.6-01 | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Already part of CI gates per CLAUDE.md but needs a dedicated sweep pass — there are likely accumulated `#[allow(...)]` from rapid-iteration phases that should be re-evaluated. Inventory all `allow` attributes; justify each one. |
| 13.7.6-02 | `cargo audit` + `cargo deny` (license / advisory check) | Verify every dependency is Apache-2.0 / MIT / BSD-compatible; no GPL / AGPL deps; no known CVEs; pin transitive deps via `Cargo.lock` review. |
| 13.7.6-03 | Python lint sweep — `ruff check` + `mypy --strict` | Already GREEN from Phase 13.5 Plan 11; this is a re-verify pass. Pin tool versions in `pyproject.toml`. |
| 13.7.6-04 | TypeScript lint — `tsc --noEmit` + `eslint` (if configured) | Phase 13.6 shipped vitest + tsc; verify no TS warnings; consider adding `eslint` if not present. |
| 13.7.6-05 | Go vet — `go vet ./...` + `staticcheck` (optional) | Phase 13.6 shipped Go module; standard vet pass. |
| 13.7.6-06 | OWASP Top-10 + LLM-prompt review (`/cso` skill or `/security-review`) | Run dedicated security-review skill on the codebase. Focus areas: predicate-string injection (T-03-02-01 — `_col.py` escape function — verify tests exist), force=True destructive register, OP_RESET test_mode gate (D-03), HTTP/TCP listener input handling. |
| 13.7.6-07 | Threat model — ASVS L1 narrative | Document the trust boundary (server is single-tenant; no auth in v0 — operator is responsible for network isolation), the high-severity threats, and the v0 mitigations. Single page in `docs/security.md`. |
| 13.7.6-08 | Secrets sweep — `git secrets`, `trufflehog`, manual audit | Scan full history (~2400 commits) for leaked API keys, tokens, internal URLs, employee emails, etc. If anything found, surfaces D-01 force-squash decision. |

---

### Workstream D — Commit-path sanitization

**Goal:** Ship a public-facing git history that's professional, free of AI-tooling artifacts, and free of any secrets / internal references found in Workstream C-08.

**Plans:**

| Plan | Scope | Notes |
|------|-------|-------|
| 13.7.6-09 | **D-01 decision lock** — what ships as public-repo history? | User-locked decision needed. Four options on the table (see decision matrix below). Recommendation defaults: keep history + strip `.planning/` via `git filter-repo` (preserves engineering narrative; hides private business reasoning). |
| 13.7.6-10 | **D-04 — strip AI attribution from commit history** | Run `git filter-repo --message-callback` (or `git rebase --interactive` if scope is tractable) to remove every `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` trailer (~30+ instances per audit) and every `🤖 Generated with [Claude Code](https://claude.com/claude-code)` line. Preserve commit subjects + bodies otherwise. Set author/committer = Hoang Phan on every commit. (Per `feedback_no_ai_attribution_in_commits` memory.) |
| 13.7.6-11 | **Strip `.planning/` from public history** (if D-01 = "keep history + strip planning") | `git filter-repo --invert-paths --path .planning/` to remove the entire planning tree from every commit in history. `.planning/` stays in private dev branch but never appears in the public repo. CONTEXT/SCRATCH/SUMMARY files contain private business reasoning + AI-generation patterns. |
| 13.7.6-12 | Strip executor-agent worktree branches | `git branch -D worktree-agent-*` (currently ~30 stale worktree branches from this session); `git worktree prune`; verify only `main` (renamed from `v2/greenfield`) remains. |
| 13.7.6-13 | Branch rename `v2/greenfield → main` | Single public branch. |
| 13.7.6-14 | (Optional) Repo rename `tally → beava` | Per `project_beava_product` memory — public name is `beava.dev`, repo codename was `tally`. GitHub one-click rename + redirects auto-handled. |

**D-01 Decision matrix:**

| Option | Pros | Cons | Recommended? |
|--------|------|------|--------------|
| Squash to fresh `main` (single "Initial commit") | Clean first impression; no AI tells; `.planning/` never appears; smallest repo | Loses engineering narrative; can't `git blame` to design decisions; first-time contributors see a wall | OK if you want minimum public surface |
| Squash to ~20-30 curated commits | Curated narrative; clean attribution | Manual curation work; no `git blame` granularity | Middle ground |
| **Keep full history, strip `.planning/` via filter-repo** | Real history preserved; engineering narrative intact; `.planning/` (private business reasoning) never published | All AI-generated commit messages preserved (some contain phase numbers, deviation references) | **DEFAULT recommendation** |
| Keep full history including `.planning/` | Maximum transparency about how it was built | Public-facing AI-generation patterns; private reasoning leaked; ~50% larger repo | NOT recommended (privacy + image) |

**Locks needed from user:**
1. D-01 — which history option above?
2. D-04 — does "strip AI attribution" mean strip the trailer ONLY (keep commit subjects intact), or also rewrite subjects that mention "Claude" (e.g., `chore: add gstack skill routing rules to CLAUDE.md` — the file IS legitimately named CLAUDE.md per Claude Code convention; stays as-is)?
3. Repo rename `tally → beava` — yes/no/defer?
4. Author/committer on rewritten history — `Hoang Phan <hoang@beava.dev>` or `<hoang.phan@viggle.ai>` (current local config) or both? (Most repos use the `noreply` GitHub-style email but you may prefer a real one.)

---

### Workstream E — Public-facing files

**Goal:** Standard OSS scaffolding so the GitHub repo presents professionally and contributors know the workflow.

**Plans:**

| Plan | Scope | Notes |
|------|-------|-------|
| 13.7.6-15 | `LICENSE` | Apache-2.0 (per CLAUDE.md OSS commitment). Single file at repo root. |
| 13.7.6-16 | `README.md` (repo-root, distinct from beava.dev home) | Hero / install / 60-second quickstart / link tree. Scope-narrow vs the docs site. Likely 100-200 lines. |
| 13.7.6-17 | `CONTRIBUTING.md` | PR workflow, test gates (TDD red→green for code), `cargo test --workspace --features testing` + `cargo clippy -D warnings` + `cargo fmt --check` are the merge bars. Issue templates, scope of v0 vs v0.1+. |
| 13.7.6-18 | `SECURITY.md` | Vuln-disclosure email/process. Standard format. |
| 13.7.6-19 | `CODE_OF_CONDUCT.md` | Contributor Covenant 2.1 (standard, copy-pasteable). |
| 13.7.6-20 | `CHANGELOG.md` | Curated v0.0.0 entry. Synthesize from Phase 13.x SUMMARY files. |
| 13.7.6-21 | `.gitignore` audit | Verify nothing internal leaks (e.g., `.cursor/`, IDE configs, tmp/, *.swp, etc.). |
| 13.7.6-22 | `.github/ISSUE_TEMPLATE/{bug_report,feature_request}.md` + `.github/PULL_REQUEST_TEMPLATE.md` | Standard GitHub UI scaffolding. |
| 13.7.6-23 | (Optional) `.github/workflows/ci.yml` | Public-facing CI: `cargo test --workspace`, `cargo clippy`, `cargo fmt`, `pytest`, `mypy`, `npm test`, `go test`. Pin runner version. |

**Decision needed: What about `CLAUDE.md`?** This file is named per Claude Code convention but contains real project conventions (TDD discipline, perf gates, mio-only invariant, events-only invariant). Three options:
1. Keep as `CLAUDE.md` — most modern OSS projects have one and it's reasonably understood
2. Rename to `AGENTS.md` (gaining traction as the cross-tool standard)
3. Rename to `CONVENTIONS.md` (project-neutral)

Default recommendation: **keep `CLAUDE.md` as-is** — real cross-tool standard hasn't emerged, file content is project conventions not Claude-specific, renaming creates a cross-reference debt.

---

### Workstream F — Closure

| Plan | Scope |
|------|-------|
| 13.7.6-24 | SUMMARY + VERIFICATION + STATE/ROADMAP advance to Phase 13.8 packaging+GA |

---

## Estimated scope

- **Total plans:** 24 (8 security + lint + 6 commit-path + 9 public-facing files + 1 closure)
- **Wall-clock:** ~3-5 days
- **Decision points needing user lock:** D-01 (history shape), D-04 detail (subject rewrite scope), repo rename, author email

---

## Dependencies / blockers

**Blocked on:** Phase 13.7.5 (comment audit + test coverage). Otherwise we'd run clippy / lint on code about to be edited.

**Blocks:** Phase 13.8 (packaging + GA). Public files + cleaned history are required before the GA tag goes out.

---

## Cross-references

- Companion to Phase 13.7.5 (`.planning/ideas/phase-13.7.5-pre-oss-polish.md`) — 13.7.5 cleans **code**, 13.7.6 cleans **repo state**
- Implements `feedback_no_ai_attribution_in_commits` memory — historical scrub of ~30+ Co-Authored-By trailers
- Out of scope: project skill / `.claude/` directory cleanup (those stay private to the dev clone; only the published-tree content matters)

---

## Out of scope for 13.7.6

- v0.0.x bug fixes (HTTP-transport regression, conformance harness payload, etc.) — those defer post-GA
- Repo rebrand beyond rename (logo, brand colors, marketing site) — Phase 13.7 already shipped beava-website
- Translating CLAUDE.md / docs into other languages
- Setting up GitHub Actions / CI infrastructure beyond the basic public CI workflow
- Sponsorship / FUNDING.yml (premature; can add later)

---

*Capture file. To convert to a real phase: run `/gsd-discuss-phase 13.7.6` after Phase 13.7.5 closes (or in parallel — comment-audit and security-audit are independent).*
