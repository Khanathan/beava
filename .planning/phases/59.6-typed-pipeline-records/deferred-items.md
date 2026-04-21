# Phase 59.6 Deferred Items

## Out-of-scope issues discovered during Wave 0 execution

### Pre-existing test compile failures on HEAD

`cargo test --release --tests --no-run` and `cargo test --release --lib --no-run` both
fail on HEAD (prior to any 59.6 Wave 0 changes) with 33-34 `E0063: missing field 'salt'
in initializer of StreamDefinition` errors.

**Verified pre-existing:** reproduced after `git stash` of all local changes including
the unstaged Phase 60 WIP in `src/shard/thread.rs`, `src/shard/metrics.rs`, and
`src/server/protocol.rs`. The `StreamDefinition.salt` field was already committed to
`src/engine/pipeline.rs` (main branch), but the test-side callers in `src/shard/`,
`src/engine/`, and `tests/` still construct `StreamDefinition { .. }` literal without
the new field.

**Why deferred:** This is leakage from the in-progress Phase 60 (hotkey-mitigation-
via-application-salting) work. Wave 0 of Phase 59.6 is a RED-scaffolding wave — it
adds new files and two counter fields, but does NOT touch `StreamDefinition` or any
struct-literal call site. The scope boundary rule (deviation Rule 3 + SCOPE BOUNDARY
note) explicitly says "do NOT fix pre-existing warnings, linting errors, or failures
in unrelated files."

**Observable impact on Wave 0 acceptance:**
- `cargo build --release` — still passes (no regression from 59.6 Wave 0).
- `cargo test --release --lib` / `--tests` — fails on HEAD with the `salt` errors,
  so the plan's lib-baseline numbers (`825/0/35 fjall` / `817/0/35 state-inmem`)
  cannot be re-measured this wave. The baseline figures in the plan were recorded
  before Phase 60 landed its partial `StreamDefinition.salt` change.

**Owner / resolution:** Phase 60 (hotkey-salting) closes when its `StreamDefinition
{ ..., salt: None }` sweep updates every remaining literal site. 59.6 waves ≥ 1
can proceed without waiting (they add new code, not modify `StreamDefinition`
initializers). The `cargo bench --no-run` gate and `cargo build --release` gate
(which 59.6 Wave 0 *does* hit) both pass.

**Recommendation:** Phase 60 executor should add the `salt: None` field to every
`StreamDefinition {...}` literal in tests and shard-internal call sites before Phase
60 close. This is a mechanical sweep, no design decisions needed.

---

### Wave 1 follow-up: pre-existing E2E join / enrich flakes visible now that the server compiles

Once Phase 59.6 Wave 1 restored `cargo build --release` as a working gate
(the server itself compiles cleanly; the pre-existing `salt` blocker only
prevents `cargo test --lib` from linking), two Python E2E tests start
failing — but they were NOT running at all on HEAD prior to 59.6, because
the `conftest.py::beava_server` fixture calls `cargo build` and errored
out with the salt compile failure:

- `python/tests/test_v0_joins_e2e.py::test_stream_stream_join_tcp`
  — expects `matched >= 1` after a stream-stream join; gets `{}`.
- `python/tests/test_v0_joins_e2e.py::test_stream_table_enrich_tcp`
  — same `row.get("n") == 2` → `None`.

**Verified pre-existing:** reproduced by stashing `python/beava/_serialize.py`
(my Wave 1 `schema:` emission) — the test still fails. Since pre-Wave-1
HEAD could not build the server, the tests were erroring on fixture
setup, not fail-on-assertion. Wave 1 surfaces the genuine join-E2E
regression that landed somewhere between the last green server build
(pre-Phase-60 WIP) and now.

**Why deferred:** out of scope for 59.6 Wave 1 (which lands schema runtime
+ registry, not Stream-Stream-Join operator fixes). These are likely the
responsibility of whoever lands the Phase 60 salt-sweep finish or whoever
last touched join state — a parity regression in Stream-Stream-Join or
stream-table enrich between that commit and arch/tpc-full-shard tip.

**Owner / resolution:** next phase to touch joins, or a dedicated fix
once the Phase 60 salt-sweep closes.

---

### Wave 3 follow-up: cross-shard enrich assertion regresses after salt-sweep unblock

Wave 3 added `salt: None` to `tests/cross_shard_enrich_from_table.rs` +
`tests/common/cascade_harness.rs` to unblock the Wave-3 acceptance gate
(same pattern as Wave 2's targeted sweep). With those additions:

- `enrich_from_table_same_shard_fast_path` — **PASS** (no regression).
- `enrich_from_table_crosses_shard_boundary` — **FAIL** — asserts
  `EnrichedSnap.last_gdp_usd == Int(800_000)` but reads `Missing`
  after a cross-shard `EnrichFromTable` fan-out.

**Verified pre-existing:** my Wave-3 changes only ADD `push_typed_on_shard`
+ the `ShardOp::PushTypedRow` variant; the existing `ShardOp::Push` arm
+ `push_with_cascade_on_shard` (which this test exercises) are
untouched. The regression was visible in Wave-1's deferred-items
addendum as "Stream-Stream-Join or stream-table enrich between that
commit and arch/tpc-full-shard tip". Salt-sweep unblocks surface it; do
not fix.

**Owner / resolution:** next phase to touch Phase 56 cross-shard
EnrichFromTable or whoever fixes the E2E join flake. The typed-path
Wave 3 tests use the same-shard path and are unaffected.

---

### Wave 6 follow-up: pre-existing Python E2E TCP flakes (7 tests) unchanged by Wave 6

Python pytest full suite (excluding integration dir) on Wave 6 HEAD shows
7 pre-existing failures, all in TCP e2e tests that spin up a live
`beava_server`:

- `test_v0_joins_e2e.py::test_stream_stream_join_tcp` (same as Wave 1)
- `test_v0_joins_e2e.py::test_stream_table_enrich_tcp` (same as Wave 1)
- `test_v0_register_roundtrip.py::test_full_tcp_roundtrip_register_push_get`
- `test_v0_stream_table_join.py::test_stream_table_enrich_tcp_roundtrip`
- `test_v0_stream_table_join.py::test_stream_table_enrich_composite_key_tcp`
- `test_watermark_e2e.py::test_event_time_populated_by_user_lands_in_correct_bucket`
- `test_watermark_e2e.py::test_event_time_absent_uses_wall_clock`

**Verified pre-existing:** reproduced with `git stash` of all Wave 6
changes (including python/ edits + src/server/tcp.rs REGISTER ack change
+ wire/typed.rs schema_id tightening); same 7 failures, identical error
shapes (`KeyError: 'n'`, `row == {}`). Unrelated to Wave 6.

**Why deferred:** Wave 6's scope is advanced typed aggs + SDK v0.3.0
handshake. The failing e2e tests exercise the push → feature-read path,
which has been flaky since Phase 60's in-progress salt sweep landed on
the branch (per Wave 1 + Wave 3 notes above). Wave 6 changes make no
modification to the push or feature-read code paths.

**Owner / resolution:** Phase 60 salt-sweep closure, or a dedicated
debugging session on the e2e harness. 520/527 unit tests pass; the 7 are
pre-existing and non-blocking for Wave 6 acceptance (SC-4 + SC-6 flip
GREEN on Rust-side tests that don't depend on the e2e harness).
