# Phase 25 — Deferred Items

## From Plan 25-02 (v0 TTL defaults + suggestion engine)

### test_cli_happy_path
- **Type:** Integration test
- **Deferred because:** Plan proposed spawning the full server via
  `std::process::Command`, registering data, invoking the CLI, and
  asserting on stdout. Slow warm-up + ordering nondeterminism moves
  this to a follow-up.
- **Mitigation in place:** CLI output format covered by
  `test_config_recommendations::recommendation_schema_shape` (validates
  the JSON fields the CLI prints).

### test_startup_advisory_log
- **Type:** Integration test
- **Deferred because:** Requires a tracing/eprintln subscriber hook to
  capture process-level log output from a spawned binary. The logic is
  a thin wrapper over `recommend_config()` which has 8 covered tests.
- **Mitigation in place:** The advisory is a ≤10-line block in main.rs
  calling the already-tested `recommend_config`. Behavior is obvious.
