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
