# Phase 19.1 — Deferred items

Items discovered during plan execution that are out of scope for the current
plan but worth tracking. Each entry includes: discovered-by, scope, and
suggested follow-up.

---

## 1. HTTP parser MAX_HEADER_BYTES check applies to entire buffer (headers + body)

**Discovered by:** Plan 19.1-02 smoke run with fraud-team.json (15 KB register
body)

**File:** `crates/beava-runtime-core/src/http_listener.rs:69-74`

**Bug:**
```rust
let probe = if buf.len() > MAX_HEADER_BYTES {
    return Err(ParseError::TooLarge);
} else {
    buf.as_ref()
};
```

The early-return at `buf.len() > MAX_HEADER_BYTES` (8 KiB) fires when ANY
HTTP request — headers + body combined — exceeds 8 KiB. This intentionally
bounds header parsing cost (httparse stack allocation), but it currently also
rejects any request whose body is larger than 8 KiB AT ANY POINT during
incremental TCP read. fraud-team.json's register body is ~15 KiB and trips
this immediately on the second TCP packet, before the parser has even tried
to read the `Content-Length` header.

`MAX_BODY_BYTES = 4 MiB` is set but never reached because the buffer-size
check fires earlier.

**Symptom:** `cargo run --release -p beava-bench -- --pipeline fraud-team.json`
fails with `register request: connection closed before message completed`.
Bench server's apply thread parses HTTP request, hits `ParseError::TooLarge`,
sends `RingItem::ParseError` (which probably triggers connection close).

**Repro:**
```bash
./target/release/beava-bench-v18 \
  --pipeline crates/beava-bench/configs/fraud-team.json \
  --transport tcp --wire-format msgpack \
  --parallel 2 --pipeline-depth 16 --total-events 50 \
  --blast-shape zipfian --no-ledger
# → Error: register request: connection closed before message completed
```

Bisect shows ANY 2-derivation subset of fraud-team.json that pushes the
combined register body over 8 KiB triggers the same error; events-only or
single-derivation subsets work.

**Out-of-scope reason:**
Plan 19.1-02 is "validate fraud-team.json against AggOpDescriptor schemas;
fix in-place". The validator is exercised via `register_validate::validate_payload`
in a unit test (`tests/fraud_team_validates_against_agg_op_descriptor.rs`)
and that test passes. The HTTP parser body-size limit is a wire-stack issue
in beava-runtime-core, NOT a fraud-team.json schema or fraud-team-validation
issue. Pre-existing bug; predates this plan.

**Suggested fix:**
Move the buffer-size cap from `parse_http_request`'s early gate to two
separate caps:
1. Cap header bytes only (track `header_end` from httparse, accept up to
   `MAX_HEADER_BYTES` of header bytes; reject if no `\r\n\r\n` in first
   8 KiB).
2. Cap body bytes via `Content-Length` header check before consuming
   (already done at line 143, but unreachable due to the early-return).

Acceptance: a 15 KiB register POST succeeds against the bench v18 server.

**Suggested phase:** 19.1-03 (WAL config) is a sibling concurrent plan; this
fix has nothing to do with WAL and could land independently. Could fold into
19.1-03 OR open a small standalone hotfix. Plans 19.1-04 (lazy buckets) and
19.1-05 (re-baseline) BOTH need this fix to actually run fraud-team.json
through the bench, so it is **on the critical path** for those plans.

**Severity:** BLOCKER for Plans 19.1-04, 19.1-05 if fraud-team.json is the
canonical bench cell (per memory `project_fraud_team_primary_bench`). NOT
a blocker for 19.1-02 — validation is the deliverable here, not bench
execution.

---

## 2. `phase18_04_6_integration_test` flakes when run in parallel

**Discovered by:** Plan 19.1-03 GREEN verification (`cargo test -p beava-server
--features testing`)

**File:** `crates/beava-server/tests/phase18_04_6_integration_test.rs`

**Symptom:** When the three `#[tokio::test]` server-boot tests in this file
run with the default test-thread parallelism, exactly one of them fails with
`server at 127.0.0.1:NNNN did not become ready within 10 seconds`. The
particular test that fails varies between runs (e.g. `test_runtime_kind_metric_mio`,
`test_serve_loop_uses_mio_not_tokio`).

`cargo test ... -- --test-threads=1` always passes all three (~0.22 s total).

**Reproduced at:** parent commit `fcea536` (no Plan 19.1-03 changes
applied) — confirmed PRE-EXISTING. Out of scope for this plan.

The file already declares a `SERVER_SERIALIZER: std::sync::Mutex<()>` for
exactly this purpose, but at least one of the three boot-tests is missing a
`let _g = SERVER_SERIALIZER.lock().unwrap();` guard, allowing two tests to
boot a full ServerV18 stack in parallel. Each stack spawns: a mio loop
(std::thread), an admin tokio server, a WalWriter thread, a WalSink worker,
and (after Plan 19.1-03) allocates 4 × 32 MiB = 128 MiB of WAL ring memory.
On a 10-core M4 the OS thread-pool/tokio-task-queue saturation causes one
test's `/health` poll loop to time out at the 10 s deadline.

**Suggested fix (out of scope here):** Audit each `#[tokio::test]` that calls
`ServerV18::serve()` in the file; add `let _serial = SERVER_SERIALIZER.lock().unwrap();`
at the top of each. Or rewrite the deadline poller to use a longer timeout
under heavy CI load.

**Suggested phase:** standalone hotfix or Phase 19.2 polish. Not blocking any
Phase 19.1 plan because `--test-threads=1` is the documented recipe in the
existing throughput-bench/test-server flow, and Plan 19.1-03's own
`wal_env_var_tunables.rs` test file passes deterministically.

**Severity:** non-blocker. Test-only flakiness; production paths unaffected.

---

## 3. `phase18_04_7_iopool_test` deterministically fails 3 of N tests

**Discovered by:** Plan 19.1-03 GREEN verification (`cargo test --workspace`)

**File:** `crates/beava-server/tests/phase18_04_7_iopool_test.rs`

**Symptom:** Three tests fail deterministically at HEAD AND at the
pre-GREEN parent commit `1fdd97c` (so this is PRE-EXISTING and unrelated
to Plan 19.1-03's WAL-config changes):

```
test test_apply_thread_does_no_parse_or_encode ... FAILED
  panicked at .../phase18_04_7_iopool_test.rs:321:5:
    expected off-apply parse > 0, got 0
test test_mixed_http_tcp_through_iopool ... FAILED
  panicked at .../phase18_04_7_iopool_test.rs:484:5:
    off-apply parse should be > 0 (mixed traffic)
test test_serve_with_dirs_uses_iopool_for_read_write ... FAILED
  panicked at .../phase18_04_7_iopool_test.rs:207:5:
    expected off-apply parse count > 0, got 0
```

All three assert that an "off-apply parse" counter is incremented during
operation; counter stays at 0. Most likely cause: an instrumentation
counter / metric got renamed or removed in a recent refactor, the test
still references the old metric name, and the test was never re-run to
catch the rename.

**Reproduced at:** parent commit `fcea536` (RED-only, no GREEN changes
applied) — PRE-EXISTING. Plan 19.1-03 only touches `wal_config.rs` and the
WAL-init block in `server.rs`; neither path overlaps with the iopool
metric-counting code paths these tests exercise.

**Suggested fix (out of scope here):** find the off-apply parse counter
that the tests query; trace it through the IoPool worker code; either
re-wire the counter, or update the tests to query the current metric
name.

**Suggested phase:** standalone hotfix or Phase 19.2 polish. Not blocking
Plan 19.1-03 (WAL-config does not touch IoPool); not blocking Plan
19.1-04 (lazy buckets, agg-thread only); marginally relevant to Plan
19.1-05 (re-baseline runs the bench harness, not these tests). Could land
alongside item #2 as a single "test hygiene" hotfix branch.

**Severity:** non-blocker for Phase 19.1 plans. Worth fixing before Phase
20 because hidden test failures erode the gate.
