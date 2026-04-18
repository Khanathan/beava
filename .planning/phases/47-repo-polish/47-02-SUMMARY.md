---
phase: 47-repo-polish
plan: "02"
subsystem: ci
tags: [github-actions, ci, rust, python, nextest, swatinem-cache]
dependency_graph:
  requires: []
  provides: [".github/workflows/ci.yml"]
  affects: ["README.md (Plan 47-06 badge consumption)"]
tech_stack:
  added:
    - "dtolnay/rust-toolchain@stable (fmt + clippy + stable toolchain)"
    - "Swatinem/rust-cache@v2 (cargo artifact cache)"
    - "taiki-e/install-action@v2 (cargo-nextest prebuilt install)"
    - "actions/setup-python@v5 (Python matrix)"
  patterns:
    - "Separate fmt / clippy / test jobs for fast failure isolation"
    - "Python matrix via strategy.matrix over python-version"
    - "concurrency cancel-in-progress for branch-push debounce"
key_files:
  created:
    - ".github/workflows/ci.yml"
  modified: []
decisions:
  - "Split Rust monolithic job into three jobs (fmt/clippy/test) — fmt fails fast (~30s) without waiting for compilation"
  - "Python matrix builds the Beava binary per matrix cell — integration tests in conftest.py require it; Swatinem cache keyed per python-version to avoid cross-cell stomp"
  - "cargo nextest run --all-features --no-fail-fast (not --release) — nextest default is debug; --release would increase cold build time by ~60s for no test-correctness benefit"
  - "No needs: dependency between Rust and Python jobs — they fan out in parallel"
metrics:
  duration_seconds: 41
  completed_date: "2026-04-18"
  tasks_completed: 1
  tasks_total: 1
  files_created: 1
  files_modified: 0
requirements_closed: [INFRA-03]
requirements_pending: [INFRA-04]
---

# Phase 47 Plan 02: GitHub Actions CI Summary

**One-liner:** Four-job CI workflow (fmt, clippy, nextest, Python SDK matrix 3.10–3.12) using canonical D-05/D-06 action stack with Swatinem cache and cargo-nextest.

## What Was Built

`.github/workflows/ci.yml` — replaces the prior monolithic `rust` + single-Python-version `python` jobs with:

| Job | Command | Timeout | Cache |
|---|---|---|---|
| `fmt` | `cargo fmt --all --check` | 5 min | none needed |
| `clippy` | `cargo clippy --all-targets --all-features -- -D warnings` | 10 min | Swatinem/rust-cache@v2 |
| `test` | `cargo nextest run --all-features --no-fail-fast` | 15 min | Swatinem/rust-cache@v2 |
| `python` (x3) | `pytest tests/ -q --timeout=60` | 15 min | Swatinem + pip cache |

Triggers: push to `main`, PRs to `main`. Concurrency group cancels in-flight runs on new push.

## Python SDK Layout

The Python SDK lives at `python/` (not `python/beava/`), with `pyproject.toml` at `python/pyproject.toml` and tests under `python/tests/`. The `conftest.py` provides a session-scoped `beava_server` fixture that runs `cargo build` and starts the binary for integration tests. The workflow builds the binary before pytest runs and installs the SDK with `pip install -e . pytest pytest-timeout` (no `[dev]` extra declared in pyproject.toml).

## Expected CI Runtime

| Scenario | fmt | clippy | test | python (per cell, parallel) | Total wall-clock |
|---|---|---|---|---|---|
| **Warm cache** | ~25 s | ~90 s | ~2 min | ~3 min (build cached) | ~3–4 min |
| **Cold cache** | ~25 s | ~4 min | ~5 min | ~8 min (full cargo build) | ~8–9 min |

Wall-clock is dominated by the Python matrix cells on cold cache (cargo build × 3). Swatinem's per-key caching (`key: python-${{ matrix.python-version }}`) lets cache warm after first run; subsequent runs drop to ~3 min.

## Badge URL (for Plan 47-06)

```
https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg
```

Markdown badge:
```markdown
[![CI](https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg)](https://github.com/petrpan26/beava/actions/workflows/ci.yml)
```

Swap `petrpan26/beava` for `beavadb/beava` (or whatever org/repo is used at push time) per D-07.

## Commit

- `91079d9` — `ci(47-02): add GitHub Actions workflow — fmt, clippy, nextest, python SDK matrix (INFRA-03/04, D-05/06)`

## Requirements Closed

- **INFRA-03** — CI workflow with fmt/clippy/nextest/python matrix: CLOSED
- **INFRA-04** — CI badge URL: URL confirmed (`ci.yml/badge.svg`); badge insertion pending Plan 47-06 README rewrite

## Deviations from Plan

None — plan executed exactly as written.

The prior `ci.yml` used `actions/cache@v4` (raw Cargo cache, stale-key prone), a monolithic `rust` job, `cargo test` (not nextest), and a single Python 3.11 version. The new file is a full rewrite aligned with D-05/D-06 spec.

## Known Stubs

None.

## Threat Flags

None — this file adds no network endpoints, auth paths, or schema changes.

## Self-Check: PASSED

- `.github/workflows/ci.yml` exists: CONFIRMED
- Commit `91079d9` exists: CONFIRMED
- All 18 acceptance criteria: PASS
- YAML valid (ruby -ryaml): PASS
