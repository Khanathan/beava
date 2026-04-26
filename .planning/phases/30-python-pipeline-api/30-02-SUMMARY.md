---
phase: 30-python-pipeline-api
plan: 02
subsystem: python-pipeline-api
tags: [cli, pyo3, python, client, replica, e2e, phase-30]
one_liner: "Pure-Rust `tally query` / `tally inspect` subcommands + 9-test E2E pytest suite wiring the PyO3 Pipeline + CLI against a live server."

dependency-graph:
  requires:
    - Phase 28-04: tally::client::clone::{run_clone, CloneArgs, CloneError} + FrozenClient::get / iter_entities / scope / snapshot_taken_at
    - Phase 28-02: hand-rolled arg-parsing pattern in src/bin/tally_cli.rs
    - Phase 30-01: tally.Pipeline PyO3 class + OutOfScopeError Python exception + wheel layout
    - Phase 27: OP_SNAPSHOT_FETCH server opcode
  provides:
    - "`tally query --remote H:P --streams S --key K --stream S` CLI subcommand — one-shot historical lookup, JSON to stdout, exits non-zero with OutOfScope marker on scope violation."
    - "`tally inspect --remote H:P --streams S` CLI subcommand — per-stream key-count JSON."
    - "python-native/tests/integration/ — 9 E2E pytests exercising Pipeline + CLI against a real tally server."
    - "python-native CI job runs E2E suite after 30-01's unit suite."
  affects:
    - "CI python-native job timeout 15 -> 25 min + release-bin build + pytest-timeout."

tech-stack:
  added: []  # no new deps — only std/serde_json/tokio already in tree
  patterns:
    - "Hand-rolled arg parsing (Phase 28-02 style) extended for two new subcommands."
    - "E2E seeding via base-snapshot file (same pattern as tests/integration/test_tally_clone.py)."
    - "BTreeMap for deterministic JSON key ordering in `inspect` output."
    - "Session-scoped discovery fixtures (`tally_server_bin`, `tally_cli_bin`) prefer release, fall back to debug."

key-files:
  created:
    - python-native/tests/integration/__init__.py
    - python-native/tests/integration/conftest.py
    - python-native/tests/integration/test_pipeline_e2e.py
  modified:
    - src/bin/tally_cli.rs (adds Query/Inspect subcommands + 7 unit tests)
    - .github/workflows/ci.yml (release-bin build + E2E step + pytest-timeout + 25min timeout)

decisions:
  - "CLI path: `tally query`/`tally inspect` wrap `run_clone` + `FrozenClient` directly rather than the plan-sketched `Session/StateStore`. Phase 29 hasn't landed; Plan 30-01 made the same call for the PyO3 Pipeline — we follow."
  - "Arg parsing: stayed with Phase 28-02's hand-rolled style. User instructions explicitly called this out."
  - "E2E seeding: base-snapshot file, not App.push. OP_SNAPSHOT_FETCH reads the persisted base snapshot; v0 has no manual checkpoint opcode so live-pushed events don't surface. Mirrors the proven pattern from test_tally_clone.py."
  - "Integration tests under `python-native/tests/integration/` (not `python/tests/integration/`). Running pytest from python/ shadows the installed wheel with the source tree — same rationale as Plan 30-01 Deviation 2."
  - "Inspect output uses BTreeMap for deterministic JSON key ordering — easier CI log diffs. Tests compare via `json.loads(...) == {...}` so ordering doesn't affect correctness."

metrics:
  duration: "~40 min"
  completed: 2026-04-14
  tasks_completed: 3
  tests_added:
    - "tally_cli unit: 7 new tests (16 total, all pass)"
    - "python-native E2E: 9 new tests (all pass; runtime ~1s locally)"
  rust_tests_total_bin_cli: 16
---

# Phase 30 Plan 02: CLI subcommands + E2E test suite Summary

## What shipped

1. **`tally query`** — `--remote H:P --streams S[,..] [--keys K,..|--key-prefix P] [--token T] --key LOOKUP_KEY --stream LOOKUP_STREAM`. Runs a one-shot `run_clone` historical snapshot fetch, then prints the JSON-serialized `SerializableEntityState` for the target entity. In-scope-but-absent → stdout `null`, rc 0. Scope violation → stderr `error: OutOfScopeError: ...`, rc 2 (load-bearing "OutOfScope" marker for T-30-07).
2. **`tally inspect`** — `--remote H:P --streams S[,..] [--keys K,..|--key-prefix P] [--token T]`. Runs the same historical fetch and prints `{stream_name: key_count}` JSON (BTreeMap, deterministic ordering). Streams declared in scope with zero loaded keys surface as `0`, never absent.
3. **9 E2E tests** — spawn a real `tally` server, seed a 3-entity base snapshot (u1, u2, u3 on `Transactions`), register the stream via HTTP /pipelines, then exercise:
   - `Pipeline.run()` happy path
   - `Pipeline.get(in_scope_key, stream)` returns a dict with `streams` field
   - `Pipeline.get(oos_key, stream)` raises `OutOfScopeError` (fulfils the skipped 30-01 test)
   - `Pipeline.get(key, oos_stream)` raises `OutOfScopeError`
   - `Pipeline.inspect()` returns `{"Transactions": 2}`
   - CLI `query` in-scope → rc 0 + JSON on stdout
   - CLI `query` OOS → rc != 0 + "OutOfScope" in stderr
   - CLI `inspect` → rc 0 + `{"Transactions": 2}` (JSON-compared)
   - CLI `inspect` with empty-scope keys → `{"Transactions": 0}`
4. **CI** — `python-native` job bumped 15 → 25 min, adds `pytest-timeout` to both installs, splits Plan 30-01 unit run (`--ignore=tests/integration`) from a new Plan 30-02 E2E step that first builds release binaries (`cargo build --release --features client --bins`) then runs `pytest tests/integration/ --timeout=120`.

## Actual CLI flag names registered (vs. planned)

Registered exactly as planned:

| Flag            | Subcommand     | Notes                                                 |
| --------------- | -------------- | ----------------------------------------------------- |
| `--remote`      | query, inspect | required                                              |
| `--streams`     | query, inspect | required; comma-separated                             |
| `--keys`        | query, inspect | optional; comma-separated; MX with `--key-prefix`     |
| `--key-prefix`  | query, inspect | optional; MX with `--keys`                            |
| `--token`       | query, inspect | optional; falls back to `TALLY_TOKEN` env var         |
| `--key`         | query          | required (lookup key)                                 |
| `--stream`      | query          | required (lookup stream)                              |
| `--mode`        | query, inspect | `historical` default; `streaming` rejected (Phase 31) |

`--since` from the plan's sketch was intentionally omitted — it's only
meaningful for streaming/resume (Phase 31), and Plan 30-01's Pipeline
class doesn't expose it either.

## Fixture pattern reused

`python-native/tests/integration/conftest.py` is a direct adaptation of
`tests/integration/test_tally_clone.py` (Plan 28-04) — same
`_enc_varint[_string]` + `_write_base_snapshot_file` + `_find_free_port`
+ `_wait_for_tcp` + `_register_stream_http` helpers, same env-var
wiring (`TALLY_TCP_PORT`, `TALLY_HTTP_PORT`, `TALLY_ADMIN_TOKEN`,
`TALLY_SNAPSHOT_PATH`, `TALLY_SNAPSHOT`).

`_SeededServer` dataclass-style view yields `.remote`, `.token`,
`.streams`, `.in_scope_keys`, `.out_of_scope_keys` so tests read like
English.

## Client.push_many / flush barrier — not used

Per the design note at the top of `conftest.py`, v0's
`OP_SNAPSHOT_FETCH` reads the persisted base snapshot on disk. Live
events pushed via the Python SDK's `App.push` wouldn't appear in the
replica until the server took a new snapshot, and v0 exposes no manual
`take_snapshot_now` opcode. The robust alternative — pre-seed a v7 base
snapshot file before the server starts — is what every replica test in
this repo already uses, so we follow the same recipe. No `flush()`
barrier was needed.

## E2E test runtime (local)

~1 s total for 9 tests with `cargo build` already warm. Each test
roundtrips one server spawn + TCP wait (~150 ms) + a 5-attempt
retry-budget run_clone (sub-ms on 127.0.0.1). Generous CI budget
(`--timeout=120`) gives a ~120× safety factor against CI variance.

## Phase 29 API surface mismatches

None encountered — we never touched Phase 29's not-yet-written
`Session` / `StateStore`. Plan 30-01 already resolved the plan-vs-reality
gap by building on `run_clone` + `FrozenClient`; Plan 30-02 inherits
that resolution without change.

## `python-native` CI job coverage confirmation

Both Plan 30-01 and Plan 30-02 pytest suites now run in CI, after the
wheel-install step:

```yaml
- Run Plan 30-01 unit + error tests          # tests/ excluding tests/integration/
- Build release binaries for E2E             # cargo build --release --features client --bins
- Run Plan 30-02 E2E integration tests        # tests/integration/ with --timeout=120
```

Verification: `python -c "...yaml.safe_load..."` asserts each required
step + the 25-min job timeout (see the plan's Task 3 automated check,
which passed).

## Deviations from Plan

### 1. [Rule 3 - Blocking] Kept hand-rolled arg parser (no clap)

**Found during:** Task 1 start.
**Issue:** The plan sketched clap `Subcommand` + `Args` derive macros,
but Phase 28-02's existing `tally_cli.rs` uses a hand-rolled parser
and `Cargo.toml` doesn't depend on clap. User instructions explicitly
called this out: "Follow 28-02's hand-rolled arg-parsing pattern (no
`clap`)."
**Fix:** Extended the existing `parse_args` state machine with `query`
and `inspect` branches plus `--key` / `--stream` flags. Semantic
validation (required flags per subcommand) added inline.
**Commit:** `9b68afa`.

### 2. [Rule 3 - Blocking] Built on run_clone + FrozenClient, not Session + StateStore

**Found during:** Task 1 sketch review.
**Issue:** Plan's `action` block references
`tally::client::{Session, ClientConfig, Scope, Mode}` and
`session.state_store().get(..)` / `.inspect()` — that surface is Phase
29's spec, but Phase 29 hasn't landed. Plan 30-01 hit the same issue
and resolved it by using Phase 28's shipped surface (see 30-01 SUMMARY).
**Fix:** `handle_query` / `handle_inspect` in `src/bin/tally_cli.rs`
call `tally::client::clone::run_clone` and then `FrozenClient::get` /
`iter_entities` — exactly the surface `python-native/src/pipeline.rs`
uses. This keeps the Python + CLI surfaces backed by the same code
path.
**Note:** the plan's proposed `src/client/cli.rs` module was not
created — all CLI logic lives directly in `src/bin/tally_cli.rs`
alongside `handle_clone` + `handle_sync` (consistent with Phase 28-02's
layout). A future Phase 29/31 can hoist the shared session machinery
when the real `Session` type materialises.

### 3. [Rule 3 - Blocking] Integration tests under `python-native/tests/integration/`

**Found during:** Task 2 layout planning.
**Issue:** Plan said `python/tests/integration/`. Plan 30-01 Deviation
2 already documented that pytest rooted at `python/` (where
`pyproject.toml` declares `testpaths = ["tests"]`) shadows the
installed wheel with the source tree, silently skipping all native
tests. Mirroring the same trap here would mean the E2E tests never
actually exercise the shipped wheel.
**Fix:** Placed tests under `python-native/tests/integration/` so CI
runs them with `working-directory: python-native`, matching Plan
30-01's rootdir choice. No import-path changes needed.

### 4. [Rule 3 - Blocking] Seeded via base-snapshot file, not App.push

**Found during:** Task 2 seeding design.
**Issue:** Plan said "push fixture events via the existing pure-Python
SDK". But `OP_SNAPSHOT_FETCH` reads the persisted base snapshot on
disk; live-pushed events only appear in the next snapshot, and v0
exposes no `take_snapshot_now` opcode. Pushing via `App.push` then
calling `Pipeline.run()` would return an empty replica.
**Fix:** Reused the proven pattern from
`tests/integration/test_tally_clone.py` (Phase 28-04): pre-seed a v7
base-snapshot file with `(entity_key, [stream_name])` tuples, then
start the server with `TALLY_SNAPSHOT_PATH` + `TALLY_SNAPSHOT=1`. The
server's scope filter strips out-of-scope entities correctly, and
in-scope entities show up via `FrozenClient::get` as seeded.
**Trade-off:** We don't exercise the `App.push` → snapshot → replica
round-trip end-to-end. That's a separate gap (needs a server-side
manual-checkpoint endpoint) and is out of scope for Plan 30. The
existing `python/tests/` E2E suite covers `App.push` → `App.get`
(same-server, not replica).

### 5. Additional test: OOS stream (not just OOS key)

**Found during:** Task 2 coverage review.
**Issue:** Plan's "must-haves" list covers OOS key, but the threat
model's T-30-07 and the plan's `<success_criteria>` #8 say "Out-of-scope
key lookups ALWAYS surface as `OutOfScopeError`." An OOS **stream**
is also a scope violation, covered by the same Rust-side
`OutOfScopeError::new("stream ... not in declared scope ...")` branch
but not in the planned test matrix.
**Fix:** Added
`TestPipelinePython::test_out_of_scope_stream_raises` to exercise the
stream-level branch too. Zero extra fixture cost.

## Auth gates

None. Plan 30-02 is all offline + localhost.

## Threat Flags

None — the threat register's T-30-06 through T-30-10 are the only new
surfaces introduced, and all have the mitigations the plan called for:

| Threat  | Mitigation shipped                                                                                 |
| ------- | -------------------------------------------------------------------------------------------------- |
| T-30-06 | `CloneError` Display already redacts token fields (inherited from Phase 28-04). No changes needed. |
| T-30-07 | CLI maps OOS to exit 2 + stderr `"OutOfScopeError"` marker; asserted in E2E. Confirmed.            |
| T-30-08 | `_wait_for_tcp` has 15s budget; each test has `@pytest.mark.timeout(60)`; CI `--timeout=120`.      |
| T-30-09 | CI step `cargo build --release --features client --bins` runs before pytest; fixture resolves binaries relative to the repo root, not `$PATH`. |
| T-30-10 | No sleep-loops in fixtures; snapshot seeding is deterministic.                                     |

## Verification evidence

| Check                                                                        | Result                                    |
| ---------------------------------------------------------------------------- | ----------------------------------------- |
| `cargo build --bin tally_cli`                                                | OK                                        |
| `cargo build --bin tally_cli --no-default-features --features client`        | OK (zero Python dep)                      |
| `cargo test --bin tally_cli`                                                 | 16 pass (9 old + 7 new)                   |
| `./target/debug/tally_cli --help`                                            | Shows query + inspect + all new flags     |
| `./target/debug/tally_cli query` / `inspect` (no args)                       | Exits non-zero with clear error           |
| `pytest python-native/tests/`                                                | 32 pass + 1 skip (old 30-01 skip)         |
| `pytest python-native/tests/integration/`                                    | 9 pass in ~1 s                            |
| CI YAML automated check (Task 3 `verify`)                                    | PASS (timeout ≥ 20m, pytest-timeout, release build, E2E step) |
| `cargo check --all-targets`                                                  | OK (one pre-existing unused-var warning in `tests/test_operators_v0.rs`; out of scope) |

## Commits

| Task | Commit     | Scope                                                                 |
| ---- | ---------- | --------------------------------------------------------------------- |
| 1    | `9b68afa`  | feat(30-02): tally query + inspect CLI subcommands                    |
| 2    | `b1eea58`  | test(30-02): E2E suite for Pipeline + CLI                             |
| 3    | `fcafb5c`  | ci(30-02): run E2E in python-native job                               |

## Self-Check: PASSED

- [x] `src/bin/tally_cli.rs` present, registers `Query` + `Inspect`
      (grep `"query"` / `"inspect"` hits both `match` arms and the
      `Subcommand` enum).
- [x] `python-native/tests/integration/conftest.py` present with
      `seeded_server` + `tally_server_bin` + `tally_cli_bin` fixtures.
- [x] `python-native/tests/integration/test_pipeline_e2e.py` present
      with 9 test functions across 3 classes.
- [x] `.github/workflows/ci.yml` declares release-bin build + E2E step
      + pytest-timeout + 25m job timeout.
- [x] Commit hashes `9b68afa`, `b1eea58`, `fcafb5c` exist in `git log`.
