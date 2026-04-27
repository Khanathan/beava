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
