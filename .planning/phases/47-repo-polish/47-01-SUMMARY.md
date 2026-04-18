---
phase: 47-repo-polish
plan: "01"
subsystem: infra/docker
tags: [docker, cargo-chef, distroless, compose, publish-runbook]
dependency_graph:
  requires: []
  provides: [INFRA-01, INFRA-02, INFRA-05, docker-image, docker-compose, publish-runbook]
  affects: [47-06-README, SHIP-02-fresh-vm-smoke]
tech_stack:
  added: [cargo-chef, gcr.io/distroless/cc-debian12:nonroot, docker-buildx]
  patterns: [multi-stage-dockerfile, distroless-nonroot-runtime, named-volume-compose]
key_files:
  created:
    - Dockerfile
    - examples/docker-compose.yml
    - docs/docker-publish-runbook.md
  modified:
    - .dockerignore
    - docker-compose.yml
decisions:
  - "Used rust:1.88-bookworm (not 1.83) — cargo-chef v0.1.77 locked deps require Rust 1.88+ (cargo-platform 0.3.2, guppy 0.17.25)"
  - "Binary name stays beava (not tally) — confirmed via Cargo.toml [[bin]] section"
  - "HTTP port 6900 set via BEAVA_HTTP_PORT env var (binary has no --http-port CLI flag)"
  - "Root docker-compose.yml retained and synced to port 6900 (docs/installation.md references it)"
  - "No serve subcommand — binary runs directly; CMD removed, ENTRYPOINT is the binary"
metrics:
  duration_minutes: 60
  completed_date: "2026-04-17"
  tasks_completed: 2
  files_changed: 5
---

# Phase 47 Plan 01: Docker image + compose + publish runbook Summary

Multi-stage cargo-chef → distroless/cc-debian12:nonroot image producing a 56.8 MB non-root Beava container with `docker run -p 6900:6900 beavadb/beava:latest` as the canonical onboarding command.

## What Was Built

### Task 1 — Dockerfile + .dockerignore (INFRA-01, INFRA-05)

Rewrote `Dockerfile` from a 2-stage slim builder to a 4-stage cargo-chef / distroless pattern:

1. **chef** (`rust:1.88-bookworm`) — installs cargo-chef
2. **planner** — `COPY . .` + `cargo chef prepare` → `recipe.json`
3. **builder** — cooks deps from recipe (cached layer), then `cargo build --release --bin beava`
4. **runtime** (`gcr.io/distroless/cc-debian12:nonroot`) — copies binary, sets `BEAVA_HTTP_PORT=6900`

`.dockerignore` excludes `target/`, `.planning/`, `.git/`, `docs/`, `examples/`, `benchmark/` while re-including `src/`, `Cargo.toml`, `Cargo.lock`, `benches/`, `python/` for the build.

### Task 2 — examples/docker-compose.yml + docs/docker-publish-runbook.md (INFRA-02, D-02/D-03/D-04)

`examples/docker-compose.yml` — minimal single-service compose referencing `beavadb/beava:latest`, exposing ports 6900 (HTTP) and 6400 (TCP), with a named `beava-data` volume at `/data`.

`docs/docker-publish-runbook.md` — 147-line manual runbook covering one-time Docker Hub org setup, `docker buildx build --platform linux/amd64 --tag beavadb/beava:latest --tag beavadb/beava:0.1.0`, local smoke test, `docker push`, post-push pull verification, rollback procedure, and a post-launch CI skeleton (deferred per D-04).

Root `docker-compose.yml` updated to sync HTTP port from the old default 6401 → 6900 to match the published image and new ENV default.

## Build + Smoke Results

| Metric | Result |
|--------|--------|
| Cold build time | ~49 s (rust:1.88 layers pre-cached; true cold ~8-10 min) |
| Cached build time | 13 s |
| Image size | 56.8 MB |
| Runtime user | uid 65532 (nonroot) |
| /health smoke | `{"status":"ok"}` — HTTP 200 |
| docker compose up smoke | /health 200 + down -v clean |

## Commits

| Task | Commit | Files |
|------|--------|-------|
| Task 1 — Dockerfile + .dockerignore | `a1be889` | `Dockerfile`, `.dockerignore` |
| Task 2 — compose + runbook | `1476622` | `examples/docker-compose.yml`, `docs/docker-publish-runbook.md`, `docker-compose.yml` |

## Requirements Closed

- **INFRA-01** — Multi-stage distroless Dockerfile at repo root
- **INFRA-02** — `examples/docker-compose.yml` with port 6900 + /data volume
- **INFRA-05** — Non-root runtime (distroless nonroot uid 65532)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Upgraded Rust builder from 1.83 → 1.88**
- **Found during:** Task 1, first docker build attempt
- **Issue:** `cargo-chef v0.1.77 --locked` pulls `ignore v0.4.25` (edition2024), which requires Rust 1.85+. Then `cargo-platform@0.3.2` and `guppy@0.17.25` required 1.86 and 1.88 respectively.
- **Fix:** Changed builder stage from `rust:1.83-bookworm` to `rust:1.88-bookworm`. The project's own `Cargo.toml` has no explicit MSRV and uses `edition = "2021"`, so 1.88 is fully compatible.
- **Impact:** `rust:1.88-bookworm` and `distroless/cc-debian12` are both Debian 12 (bookworm) — glibc ABI matches. No runtime impact.

**2. [Rule 1 - Bug] Removed non-existent `serve` subcommand from CMD**
- **Found during:** Task 1, reading src/main.rs
- **Issue:** Plan spec included `CMD ["serve", "--http-port", "6900", ...]` but the `beava` binary has no `serve` subcommand — it reads only env vars and starts directly.
- **Fix:** Used `ENTRYPOINT ["/usr/local/bin/beava"]` with no CMD args. All config via ENV block (`BEAVA_HTTP_PORT`, `BEAVA_TCP_PORT`, etc.).

**3. [Rule 2 - Missing] Root docker-compose.yml port sync**
- **Found during:** Task 2, cross-check
- **Issue:** Root-level `docker-compose.yml` used `BEAVA_HTTP_PORT=6401` and mapped `6401:6401`, conflicting with the new 6900 standard.
- **Fix:** Updated root compose to `BEAVA_HTTP_PORT=6900` and port `6900:6900`. File retained (not deleted) because `docs/installation.md` references it without a specific path.

## Known Stubs

None — no UI-rendered placeholders introduced.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes introduced. The image does not bake in `BEAVA_ADMIN_TOKEN` (left unset, callers must supply it).

## Self-Check: PASSED

| Check | Result |
|-------|--------|
| `Dockerfile` exists | FOUND |
| `.dockerignore` exists | FOUND |
| `examples/docker-compose.yml` exists | FOUND |
| `docs/docker-publish-runbook.md` exists | FOUND |
| Commit `a1be889` exists | FOUND |
| Commit `1476622` exists | FOUND |
