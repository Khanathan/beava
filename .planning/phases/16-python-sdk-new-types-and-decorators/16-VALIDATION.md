---
phase: 16
slug: python-sdk-new-types-and-decorators
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-12
---

# Phase 16 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | python/pyproject.toml |
| **Quick run command** | `cd python && python -m pytest tests/test_new_api.py -x -q` |
| **Full suite command** | `cd python && python -m pytest tests/ -x -q` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd python && python -m pytest tests/test_new_api.py -x -q`
- **After every plan wave:** Run `cd python && python -m pytest tests/ -x -q`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | Status |
|---------|------|------|-------------|-----------|-------------------|--------|
| 16-01-01 | 01 | 1 | API-03 | unit | `pytest tests/test_new_api.py::test_eventset_field_descriptors -x` | ⬜ pending |
| 16-01-02 | 01 | 1 | API-03 | unit | `pytest tests/test_new_api.py::test_featureset_field_descriptors -x` | ⬜ pending |
| 16-02-01 | 02 | 1 | API-01 | unit | `pytest tests/test_new_api.py::test_source_compile -x` | ⬜ pending |
| 16-02-02 | 02 | 1 | API-02, API-04 | unit | `pytest tests/test_new_api.py::test_dataset_compile -x` | ⬜ pending |
| 16-02-03 | 02 | 1 | API-05 | unit | `pytest tests/test_new_api.py::test_union_compile -x` | ⬜ pending |
| 16-03-01 | 03 | 2 | API-06 | unit | `pytest tests/test_new_api.py::test_validate_cycles -x` | ⬜ pending |
| 16-03-02 | 03 | 2 | API-06 | unit | `pytest tests/test_new_api.py::test_validate_missing_deps -x` | ⬜ pending |
| 16-04-01 | 04 | 2 | API-07 | integration | `pytest tests/test_new_api.py::test_register_and_push -x` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Existing infrastructure covers all phase requirements. pytest is already installed and configured.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| IDE autocomplete works for EventSet/FeatureSet | API-03 | Requires IDE (pyright/mypy type inference) | Open a .py file with EventSet subclass, verify autocomplete shows Field attributes |

---

## Validation Sign-Off

- [ ] All tasks have automated verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
