---
phase: 9
slug: incremental-snapshots
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-09
---

# Phase 9 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust built-in) |
| **Config file** | Cargo.toml |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 9-01-01 | 01 | 1 | OPS-03 | — | N/A | unit | `cargo test dirty` | ❌ W0 | ⬜ pending |
| 9-01-02 | 01 | 1 | OPS-03 | — | N/A | unit | `cargo test delta_snapshot` | ❌ W0 | ⬜ pending |
| 9-02-01 | 02 | 1 | OPS-04 | — | N/A | unit | `cargo test base_plus_delta` | ❌ W0 | ⬜ pending |
| 9-02-02 | 02 | 1 | OPS-04 | — | N/A | integration | `cargo test snapshot_recovery` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] Test stubs for dirty-key tracking (OPS-03)
- [ ] Test stubs for base+delta recovery (OPS-04)

*Existing test infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Delta file size proportional to changes | OPS-03 | Requires measuring file sizes with large datasets | Push 1M keys, modify 1K, verify delta << base |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
