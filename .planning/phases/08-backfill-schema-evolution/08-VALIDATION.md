---
phase: 8
slug: backfill-schema-evolution
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-09
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust) + pytest (Python SDK) |
| **Config file** | Cargo.toml / python/pyproject.toml |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test && cd python && python -m pytest` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test && cd python && python -m pytest`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 08-01-01 | 01 | 1 | SCHM-01 | — | N/A | unit | `cargo test schema_diff` | ❌ W0 | ⬜ pending |
| 08-01-02 | 01 | 1 | SCHM-02 | — | N/A | unit | `cargo test schema_remove` | ❌ W0 | ⬜ pending |
| 08-02-01 | 02 | 1 | SCHM-03 | — | N/A | integration | `cargo test backfill` | ❌ W0 | ⬜ pending |
| 08-02-02 | 02 | 1 | SCHM-04 | — | N/A | integration | `cargo test backfill_cooperative` | ❌ W0 | ⬜ pending |
| 08-01-03 | 01 | 1 | SCHM-05 | — | N/A | unit | `cd python && python -m pytest -k backfill` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_schema_evolution.rs` — stubs for SCHM-01, SCHM-02
- [ ] `tests/test_backfill.rs` — stubs for SCHM-03, SCHM-04
- [ ] `python/tests/test_backfill.py` — stubs for SCHM-05

*Existing test infrastructure (cargo test + pytest) covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live latency during backfill | SCHM-04 | Requires sustained load testing | Push events while backfill runs; measure p99 latency |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
