# Phase 59 — Deferred Items

Issues discovered during execution that are out-of-scope per the Rule-4
scope boundary (only auto-fix issues DIRECTLY caused by the current
task's changes).

## 2026-04-21 — Wave 1 (59-01) — `tests/test_concurrent.rs` pre-existing failures

**Discovered during:** Task 2 broad regression sweep (`cargo test --release --tests`).

**Status:** Pre-existing on `arch/tpc-full-shard` HEAD (verified via `git stash`
round-trip — same 6 failures reproduce without any Phase 59 Wave 1 changes
applied). This is Phase-58-era fallout, not caused by Wave 1.

**Failures (all in `tests/test_concurrent.rs`):**

| Test                              | Symptom                                          |
|-----------------------------------|--------------------------------------------------|
| `concurrent_push_and_get`         | PUSH returns STATUS_ERROR (1) instead of OK (0). |
| `set_mset_concurrent_with_push`   | Same symptom.                                    |
| `fan_out_under_concurrency`       | Same symptom.                                    |
| `multi_stream_parallel_push`      | Same symptom.                                    |
| `same_stream_different_keys_concurrent` | Same symptom.                              |
| `test_enriched_concurrent_clients` | Same symptom.                                   |

**Likely root cause (not investigated here):** the test's `start_server()`
helper calls `make_concurrent_state(..)` but does NOT call
`spawn_shard_threads` — at Phase 58+ the shard-thread dispatch path is the
only write path, so `state.shard_handles.read().len() == 0` leads
`handle_push_core_ex` to return "shard 0 not registered" → STATUS_ERROR.
The test was written against the pre-Phase-54 DashMap path.

**Action:** Filed for Phase 60+ harness cleanup. Out of Wave 1 scope
(does not touch any file or behavior Phase 59 modifies).

**Verification it's pre-existing, not Wave-1-caused:**

- `git stash push --include-untracked` → `cargo test --release --test test_concurrent` → still fails (6/6).
- `git stash pop` → `cargo test --release --test binary_push_bytes_passthrough --test json_over_tcp_still_accepted` → both GREEN with Wave 1.
- `cargo test --release --lib` → 822/0/35 (baseline 812 + 10 new wire:: tests).
