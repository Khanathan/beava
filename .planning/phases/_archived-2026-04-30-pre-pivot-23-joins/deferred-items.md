# Phase 23 — Deferred Items

Items discovered during Plan 23-03 execution that are out-of-scope per
CLAUDE.md deviation rules (pre-existing, or belong to a later phase).

---

## 1. Full-suite pytest session-fixture state leakage (pre-existing)

**Symptom:** `pytest python/tests/` fails on
`test_v0_stream_table_join.py::test_stream_table_enrich_tcp_roundtrip`
with `row_u1["n"] == 2` (expected 1).

**Scope verdict:** Pre-existing — reproduces with
`test_v0_joins_e2e.py` stashed (git stash confirmed: same failure,
407 passed). Caused by:

* The `app` fixture in `python/tests/conftest.py` is
  `scope="session"` — all tests share one server process.
* Multiple tests define streams named `Clicks` and write to
  `user_id="u1"`, so cross-test pushes accumulate in the shared
  entity state.
* `test_v0_stream_table_join.py`'s `test_stream_table_enrich_tcp_roundtrip`
  assumes exclusive ownership of `u1`.

**Mitigation applied in 23-03:** Renamed our own new-test user keys
to `ssj_u1` / `eu1` / `tt_u1` so Phase 23-03 tests do not themselves
add to the pollution. Pre-existing overlap between 23-01 round-trip
and `test_v0_register_roundtrip.py::test_server_executes_table_aggregation`
(which also writes to `u1`) remains.

**Suggested fix (Phase 24 or dedicated test-hygiene plan):**
Switch `app` fixture to `scope="function"` with a fresh server per
test, OR add a `reset_store()` teardown, OR rename every test's
primary key to a test-unique prefix.

---

## 2. 23-01's `Clicks` + `u1` pattern is a landmine

Future tests that use a stream named `Clicks` with `user_id="u1"`
will collide with 23-01's `test_stream_table_enrich_tcp_roundtrip`.
Document this in the Phase 24 test-hygiene plan; consider adding a
lint rule or conftest warning if a test pushes to a key that another
test in the session already wrote.

---

## 3. Per-Table row storage (folded into Phase 24 scope)

7 tests in `tests/test_join_table_table.rs` are `#[ignore]`'d pending
proper per-Table row storage. The plan's original scope included
this; CEO decision 2026-04-14 (Option 1) moved it into Phase 24's
watermark + retraction work where it's the natural foundational task.
See `23-03-SUMMARY.md::Phase 24 handoff` for the full decision record.
