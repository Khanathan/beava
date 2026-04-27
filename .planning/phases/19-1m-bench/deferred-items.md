# Phase 19 — Deferred Items

Items discovered during plan execution that are out of scope for the plan in
question and have been logged here for follow-up. Do NOT fix in the current
plan; they're flagged for future agents.

## Plan 19-03

### server panic on HTTP push under simple-fraud (small) pipeline

**Discovered during:** Plan 19-03 manual end-to-end smoke (Test 2 / Test 4 in
the post-commit verification scripts).

**Symptom:** A single `POST /push/Txn` against a freshly-registered `small`
pipeline panics the server with:

```
thread 'main' (40862272) panicked at crates/beava-core/src/agg_apply.rs:107:38:
index out of bounds: the len is 0 but the index is 0
```

**Reproducer:** Spawn `target/debug/beava --config /dev/null` with isolated WAL
+ snapshot dirs, register the `crates/beava-bench/configs/small.json`
pipeline, then `POST /push/Txn` with body
`{"event_time":1000000,"user_id":"k0","amount":42.0}`. The connection is
closed without a response (httpcore.RemoteProtocolError).

**Why deferred:** Plan 19-03's own smoke test runs `--transport tcp
--wire-format msgpack` — the TCP push path executes correctly. The HTTP push
path is a SEPARATE bug that pre-dates Phase 19 and is unrelated to the
harness plan's deliverables. SCOPE BOUNDARY rule: only auto-fix issues
DIRECTLY caused by the current task's changes.

**Suggested follow-up:** Open a separate task (debug or repair plan) to bisect
`crates/beava-core/src/agg_apply.rs:107` when the apply loop processes a
freshly-registered pipeline's first event with no prior state.

### pre-existing mypy errors in `python/beava/_app.py`

**Discovered during:** Plan 19-03 GREEN verification.

**Symptom:** `python -m mypy benches/` reports:

```
beava/_app.py:231: error: Returning Any from function declared to return "dict[str, Any]"  [no-any-return]
beava/_app.py:231: error: "Transport" has no attribute "_client"  [attr-defined]
beava/_app.py:253: error: Returning Any from function declared to return "dict[str, Any]"  [no-any-return]
beava/_app.py:253: error: "Transport" has no attribute "_client"  [attr-defined]
```

**Why deferred:** These errors come from the existing `app.upsert()` and
`app.delete()` methods (Phase 18-07 commit `4efb36f`). They pre-date Plan
19-03; my plan's `benches/` code is mypy-clean (`mypy
--follow-imports=silent benches/` passes). The fix is to add `_client`
to the `Transport` Protocol (or pre-narrow with `assert isinstance(...,
HttpTransport)` inside `app.upsert()` / `app.delete()`). Out of scope for
Plan 19-03 because (a) the bench harness doesn't break, (b) fixing
unrelated production code in a bench plan would risk drift, (c) the
errors are tolerated in the existing CI gate.

**Suggested follow-up:** Cleanup task during Phase 12 follow-up or a Phase 18
SUMMARY pass — narrow types in `app.upsert` / `app.delete` to satisfy mypy
strict mode.

### test_app.py / test_transport_*.py fail with stale WAL on disk

**Discovered during:** Plan 19-03 broader test suite verification.

**Symptom:** `pytest tests/test_app.py tests/test_transport_tcp.py
tests/test_transport_http.py` reports 18 pre-existing failures with
`ConnectionRefused`, traced to the `beava_server` fixture in
`python/tests/conftest.py` not isolating `BEAVA_WAL_DIR`. The default WAL
path `./beava-wal` collides with stale files left by previous test runs.

**Why deferred:** Same SCOPE BOUNDARY argument. Plan 19-03 added a LOCAL
override (`tests/bench/conftest.py::beava_server_isolated`) that injects
`BEAVA_WAL_DIR=tmp_path/wal` and `BEAVA_SNAPSHOT_DIR=tmp_path/snap`. The
shared `tests/conftest.py` is left untouched so unrelated tests are not
affected. Mutating the shared fixture is the right cleanup, but it's
larger than Plan 19-03's remit.

**Suggested follow-up:** Either:
  - Hoist the `beava_server_isolated` pattern up into the shared
    `tests/conftest.py` (replacing `beava_server` so all tests get
    auto-isolation), then verify nothing else relies on the bare default
    WAL dir; OR
  - Add a `tests/conftest.py` autouse fixture that cleans `./beava-wal/`
    before each session; less robust because it interacts with cwd.
