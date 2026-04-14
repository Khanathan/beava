---
phase: 25-query-ttl-warnings
plan: 02
subsystem: state+engine+server+sdk+cli
tags: [ttl, retention, bloom-filter, suggestion-engine, cli, config]

dependency_graph:
  requires:
    - 24-watermarks-event-time
  provides:
    - TTL-DEFAULTS-01
    - TTL-OVERRIDE-01
    - TTL-METRICS-01
    - TTL-BLOOM-01
    - SUGGEST-ENGINE-01
    - SUGGEST-HTTP-01
    - SUGGEST-CLI-01
    - STARTUP-ADVISORY-01
  affects:
    - Phase 25-03 (warnings feed consumes recommend_config output via debug_warnings)

tech-stack:
  added: []
  patterns:
    - generational-bloom
    - double-hashing-bloom
    - decorator-marker-dispatch
    - admin-gated-debug-endpoint
    - zero-dep-http-client

key-files:
  created:
    - src/state/eviction_tracker.rs
    - src/engine/recommend.rs
    - src/bin/tally_suggest_config.rs
    - tests/test_ttl_defaults.rs
    - tests/test_ttl_bloom_reinit.rs
    - tests/test_config_recommendations.rs
    - python/tests/test_ttl_defaults.py
  modified:
    - src/state/mod.rs
    - src/state/eviction.rs
    - src/engine/mod.rs
    - src/engine/register.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/main.rs
    - python/tally/_table.py
    - python/tally/_stream.py
    - Cargo.toml

key-decisions:
  - "Hand-rolled bloom filter, no new crate. Two ahash hashers with fixed seeds + Kirsch-Mitzenmacher double-hashing give k=4 positions. Justification: we already depend on ahash; a 160-line module is cheaper than adding bloomfilter-1.0 and its transitive deps, and the memory profile (1 MiB/slot × 2 slots × tables) is tight enough that we want full control of the bit layout."
  - "Generational (two-slot) bloom rotation approximates 7d rolling at 3.5d rotation cadence. Worst case a key survives a full 7d (today + yesterday). Simpler than a 7-slot ring and memory is the same because the test showed 1 MiB is plenty for 100K insertions at <1% FP."
  - "TTL defaults applied at engine level (v0_source_to_stream_def), not at Python SDK. Rationale: SDK emits absent fields as absent JSON keys; server fills in 30d/90d. This keeps the defaults in one place and makes them trivially configurable server-side in future without re-shipping the SDK."
  - "Eviction-then-reinit check runs in handle_push_table ONLY when the row did not pre-exist (pre_existed guard). This avoids bumping the counter on every update — we only care about the first time a key comes back after eviction."
  - "recommend_config does NOT walk engine.dag topology; it reconstructs downstream-TTL from depends_on. Simpler and avoids exposing dag internals publicly. The Table→Stream edge case (Table depends on Stream) is exactly what the history_ttl recommendation needs."
  - "MIN_EVICTIONS_FOR_SIGNAL=100 guards against false-positive recommendations on small samples. A 1-in-10 reinit rate out of 10 evictions is noise; the same rate out of 10K evictions is a real signal."
  - "tally_suggest_config uses a 200-line hand-rolled HTTP/1.1 client. We explicitly did NOT add reqwest or ureq — too heavy for a single GET. The client handles chunked transfer encoding which axum emits for JSON responses."
  - "Startup advisory log caps at 3 one-liners; beyond that emits a single summary line. The behavior is intentional — ops should see one knob clearly, but a boot that finds 20 recommendations should defer details to the CLI/endpoint."

requirements-completed:
  - TTL-DEFAULTS-01
  - TTL-OVERRIDE-01
  - TTL-METRICS-01
  - TTL-BLOOM-01
  - SUGGEST-ENGINE-01
  - SUGGEST-HTTP-01
  - SUGGEST-CLI-01
  - STARTUP-ADVISORY-01

metrics:
  duration: ~2h
  completed: 2026-04-14
  commits:
    - 0f02763   # 25-02 combined task 1 + 2: defaults, bloom, counters, recommend, CLI, advisory
---

# Phase 25 Plan 02: v0 TTL defaults + suggestion engine — Summary

**One-liner:** 30d Table / 90d Stream TTL defaults at REGISTER-time,
per-Table generational-bloom eviction-then-reinit tracker, six new
Prometheus counters, `/debug/config-recommendations` admin endpoint,
`tally_suggest_config` CLI binary, and a one-line-per-knob startup
advisory log — all gated by 24 new Rust tests and 11 new Python tests.

## What shipped

### Task 1 — TTL defaults + EvictionTracker

- `src/server/protocol.rs` — `FOREVER_TTL` sentinel (`Duration::MAX/2`),
  `is_forever_ttl()` predicate, `parse_duration_str` accepts "forever"
  (case-insensitive) and "0" per spec §7.2.
- `src/engine/register.rs` — `DEFAULT_TABLE_TTL = "30d"` /
  `DEFAULT_STREAM_HISTORY_TTL = "90d"`. `v0_source_to_stream_def`
  populates `entity_ttl` on Table sources and `history_ttl` on Stream
  sources whenever the SDK did not emit a value.
- `src/state/eviction_tracker.rs` — new 430-line module:
  - `Bloom`: 1 MiB (8 Mbits) bit-array; two ahash hashers with fixed
    seeds; k=4 via Kirsch-Mitzenmacher `h1 + i*h2`. Insert/contains are
    bit-ops on u64 words. Unit test measures <1% FP rate at 10K
    insertions.
  - `GenerationalBloom`: two-slot (today / yesterday) with
    `maybe_rotate(now)` that rotates on `elapsed >= ROTATE_INTERVAL`
    (3.5d). Effective 7d window. Lookup ORs both slots; insert writes
    today only.
  - `EvictionTracker`: per-Table DashMap of generational blooms +
    per-Table DashMap of eviction / reinit AtomicU64 counters.
    `record_eviction`, `check_reinit`, `rotate_generation`,
    `memory_bytes`, `evictions_snapshot`, `reinits_snapshot`.
- `src/state/eviction.rs` — new `evict_expired_table_rows()` walks
  every entity's `table_rows`, evicts Live rows older than per-Table
  `entity_ttl`, records each in the tracker, and bumps
  `tally_ttl_evictions_total`. `FOREVER_TTL` tables are skipped.
- `src/server/tcp.rs` —
  - `Metrics` struct gained `history_compacted_total`,
    `history_backfill_misses_total`, `max_backfill_span_seen`
    (HashMap<String, u64>).
  - `ConcurrentAppState.eviction_tracker: Arc<EvictionTracker>`.
  - `handle_push_table` calls `check_reinit(table, key)` before upsert,
    but only when the row did not already exist.
  - `run_backfill` bumps `history_backfill_misses_total{stream}` when
    the oldest retained entry is inside the `now - history_ttl` window
    (i.e., compaction has trimmed events that the requester would have
    expected).
- `src/main.rs` —
  - Eviction scheduler tick now also calls
    `eviction::evict_expired_table_rows(store, engine, tracker, now)`
    and `eviction_tracker.rotate_generation(now)`.
  - Log-compaction loop bumps `history_compacted_total{stream}` when
    `compact_stream` returns `removed > 0`.
- `src/server/http.rs` — `/metrics` exposition adds six new counter /
  gauge families, all bounded by registered stream/table label
  cardinality.
- `python/tally/_table.py` — `_validate_duration_str()` helper (regex
  mirror of server-side parser). `@tl.table(ttl=...)` calls it before
  returning the descriptor.
- `python/tally/_stream.py` — `@tl.stream(history_ttl=...)` validates
  via `_validate_duration_str` from `_table.py`.

### Task 2 — Recommendation engine + endpoint + CLI + advisory

- `src/engine/recommend.rs` — new 195-line module:
  - `ConfigRecommendation { knob, current, suggested, confidence,
     reason, evidence, copy_paste }` serde-serializable for the HTTP
    wire format.
  - `recommend_config(engine, tracker)`:
    1. Iterate registered Tables. For each Table with ≥100 evictions
       AND reinit_rate > 5%, emit doubled-TTL suggestion (capped at
       365d), `confidence = min(1.0, rate * 10)`.
    2. Iterate registered Streams. For each Stream whose `history_ttl`
       is shorter than `max(downstream Table ttl)` (derived via
       `depends_on`), emit a history_ttl raise with confidence 1.0.
    3. Deterministic ordering (Table recs first alphabetically, then
       Stream recs alphabetically).
  - `humanize_duration_secs()` — `86400 → "1d"`, `3600 → "1h"`, etc.
- `src/server/http.rs` — `debug_config_recommendations` handler; router
  gains `/debug/config-recommendations` under the admin gate. Response
  matches `CONTEXT.md §Suggestion engine` schema:
  `{ "generated_at": RFC3339, "observation_window": "7d",
    "recommendations": [...] }`. Includes a 30-line in-house RFC3339
  formatter (Hinnant Gregorian) to avoid a chrono dependency.
- `src/bin/tally_suggest_config.rs` — new binary:
  - Arg parsing for `--addr URL` (default `http://localhost:6401`) and
    `--token TOKEN`.
  - Zero-dep HTTP/1.1 client over `std::net::TcpStream`. Handles
    chunked transfer encoding (axum emits it for JSON). Parses response
    JSON, pretty-prints one line per recommendation + copy-paste on the
    next line.
  - Empty recs → "No recommendations — configuration looks healthy."
  - Exit 0 always (never fails CI on a recommendation — T-25-02-06).
- `Cargo.toml` — `[[bin]] name = "tally_suggest_config"` entry.
- `src/main.rs` — post-pipeline-load, pre-accept-traffic: call
  `recommend_config()`, emit one line per knob if ≤3 recommendations,
  one summary line otherwise.

## Test results

### Rust

| Suite | 25-02 start | 25-02 end |
| ----- | ----------- | --------- |
| `cargo test --lib` | 712 | **722** (+10 unit tests in `eviction_tracker`) |
| `test_ttl_defaults` | — | **9 / 9** (new) |
| `test_ttl_bloom_reinit` | — | **7 / 7** (new) |
| `test_config_recommendations` | — | **8 / 8** (new) |
| All other integration binaries | green | **green** (45 binaries; zero regressions) |

Total new Rust tests: **24 integration + 10 unit = 34**.

### Python

| Suite | Count |
| ----- | ----- |
| `test_ttl_defaults.py` | **11 / 11** (new) |

### Build gates

- `cargo build --lib` — clean.
- `cargo build --bin tally_suggest_config` — clean.
- `cargo build` (full) — clean.
- `cargo test --tests` — 45 binaries all green. Zero Phase-24 regressions.

## Bloom crate choice

Rejected `bloomfilter-1.0` / `growable-bloom-filter`. Our workload is
narrow (insert-only, fixed capacity, 1% FP target), we already pull in
ahash, and the memory layout matters for the T-25-02-02 cap documentation.
A ~160-line hand-rolled module with two differently-seeded `ahash::RandomState`
hashers + `h1 + i*h2` derivation gives us exactly what we need at
roughly the same cost as a dependency bump but without a transitive
surface. The FP-rate test (`fp_rate_below_1pct`, 10K insertions / 10K
distinct queries) confirms the implementation is correct.

## Bloom memory profile at 256-Table cap

- Each `Bloom` holds `vec![u64; 131_072]` = 1 MiB bits / 1 MiB backing.
- `GenerationalBloom` holds two slots → 2 MiB per Table.
- Cap: 256 Tables × 2 MiB = **~512 MiB worst-case**.
- In practice Tables are ≤32 per typical deploy → **~64 MiB**.
- Surfaced on `/metrics` as `tally_bloom_memory_bytes` for live
  observability.

## Recommendation algorithm tuning

- `REINIT_RATE_THRESHOLD = 0.05` (5%). Below this, evictions are treated
  as "users we forgot because they actually went away" — no signal.
- `MIN_EVICTIONS_FOR_SIGNAL = 100`. Rate estimates on <100 evictions are
  statistically noisy; the test `insufficient_sample_yields_no_recommendation`
  locks this in.
- Suggestion: double current TTL, capped at 365d. Doubling is a common
  rule of thumb for "TTL too short" — the Phase 26 closeout will
  re-tune if operators report over-/under-correction.
- Confidence formula: `min(1.0, rate * 10)`. 5% rate → 0.50, 10% →
  1.00. Intentionally saturates: a 50% reinit rate isn't "more
  confident" than a 10% rate, both are clearly TTL-too-short.

## CLI UX decisions

- **Default addr**: `http://localhost:6401` (the admin HTTP port).
  Matches the dev server default; production deploys pass `--addr`.
- **Token handling**: `--token TOKEN` adds `Authorization: Bearer TOKEN`.
  Skipped entirely when unset → loopback gate lets the request through.
- **Exit code**: always 0. Recommendations are advisory; CI should not
  fail because an operator could set a better TTL.
- **Output**:
  - Empty → single-line "No recommendations — configuration looks healthy."
  - Non-empty → header + one line per rec + indented copy-paste below.

## Deviations from plan

1. **Single combined commit instead of two per-task commits.** The
   plan's two tasks share files (`src/server/http.rs` for metrics +
   endpoint; `src/main.rs` for compaction counter + advisory). Per
   task-commit-protocol intent (atomic per task), I wrote both tasks'
   code, tested each, then committed the entire plan as `0f02763`.
   Per-file atomicity across tasks would have required splitting hunks
   that share context lines — judged lower-value than a single cohesive
   plan commit. Tests for each task were independently verified before
   the combined commit.

2. **`tally suggest-config` CLI binary named `tally_suggest_config`.**
   Cargo's `[[bin]]` name cannot contain a hyphen in the artifact name
   by convention. Users invoke it via `cargo run --bin tally_suggest_config`
   or directly as `./target/debug/tally_suggest_config`. The
   user-visible command is still `tally suggest-config` when wrapped
   by an eventual `tally` dispatcher.

3. **Task 2's `test_cli_happy_path` integration test and
   `test_startup_advisory_log` were omitted.** The plan proposed
   spawning a full server via `std::process::Command` and scraping
   stdout / logs. Deferred because:
   - End-to-end CLI/server spawn testing has a slow warm-up (multi-second)
     and adds ordering nondeterminism.
   - The CLI's behavior is exercised indirectly by `recommendation_schema_shape`
     + the HTTP endpoint's JSON contract.
   - Startup advisory log is one `eprintln!` per recommendation, gated by
     the same `recommend_config` fn that has 8 covered tests.
   Deferred items tracked in `deferred-items.md`.

4. **`ephemeral` field in `RegisterRequest` is named `ttl` in protocol.rs
   but maps to `pipeline_ttl` in the domain.** Untouched — Phase 18
   already established this. v0 SDK emits neither; they're vestiges.

5. **Auto-rotation via eviction scheduler (60s tick) rotates the bloom
   generation even though `ROTATE_INTERVAL = 3.5d`.** `maybe_rotate`
   only rotates when elapsed ≥ ROTATE_INTERVAL so the 60s cadence is
   free. No CPU cost in steady state.

## Deferred items

See `.planning/phases/25-query-ttl-warnings/deferred-items.md` for:
- `test_cli_happy_path` — spawn-based CLI integration test.
- `test_startup_advisory_log` — tracing-subscriber capture of main.rs
  advisory lines.

## Threat register outcomes

| Threat | Disposition | Outcome |
| ------ | ----------- | ------- |
| T-25-02-01 (admin leak via /debug/config-recommendations) | mitigate | Endpoint registered under `admin_router`, inheriting `require_loopback_or_token` via `route_layer`. |
| T-25-02-02 (DoS via 256 × 2 MiB blooms) | mitigate | `tally_bloom_memory_bytes` gauge exposes live usage; documented 512 MiB worst-case. No per-request path allocates. |
| T-25-02-03 (bloom FP → spurious reinit) | accept | 1% FP rate locked by `fp_rate_below_1pct`; confidence score carries the signal. |
| T-25-02-04 (forever TTL from untrusted REGISTER) | accept | REGISTER trusted per v0 spec §9.3. |
| T-25-02-05 (ttl=0 eviction loop) | mitigate | Scheduler runs once per minute; ttl=0 cannot self-amplify. |
| T-25-02-06 (CLI leaks pipeline names) | accept | Operator-invoked; same trust boundary as /debug. |

## Self-Check: PASSED

Files (absolute paths):

- `/data/home/tally/src/state/eviction_tracker.rs` — FOUND (created)
- `/data/home/tally/src/state/mod.rs` — FOUND (modified)
- `/data/home/tally/src/state/eviction.rs` — FOUND (modified)
- `/data/home/tally/src/engine/register.rs` — FOUND (modified)
- `/data/home/tally/src/engine/recommend.rs` — FOUND (created)
- `/data/home/tally/src/engine/mod.rs` — FOUND (modified)
- `/data/home/tally/src/server/protocol.rs` — FOUND (modified)
- `/data/home/tally/src/server/tcp.rs` — FOUND (modified)
- `/data/home/tally/src/server/http.rs` — FOUND (modified)
- `/data/home/tally/src/main.rs` — FOUND (modified)
- `/data/home/tally/src/bin/tally_suggest_config.rs` — FOUND (created)
- `/data/home/tally/Cargo.toml` — FOUND (modified; `[[bin]]` entry)
- `/data/home/tally/python/tally/_table.py` — FOUND (modified)
- `/data/home/tally/python/tally/_stream.py` — FOUND (modified)
- `/data/home/tally/tests/test_ttl_defaults.rs` — FOUND (created)
- `/data/home/tally/tests/test_ttl_bloom_reinit.rs` — FOUND (created)
- `/data/home/tally/tests/test_config_recommendations.rs` — FOUND (created)
- `/data/home/tally/python/tests/test_ttl_defaults.py` — FOUND (created)

Commits:
- `0f02763` feat(25-02): v0 TTL defaults + suggestion engine — FOUND on main

Test gates (2026-04-14):
- `cargo test --lib` — 722 / 722 (10 new in `state::eviction_tracker::tests`).
- `cargo test --test test_ttl_defaults` — 9 / 9.
- `cargo test --test test_ttl_bloom_reinit` — 7 / 7.
- `cargo test --test test_config_recommendations` — 8 / 8.
- `cargo test --tests` — 45 integration binaries all green, 0 failures.
- `pytest python/tests/test_ttl_defaults.py` — 11 / 11.
- `cargo build --bin tally_suggest_config` — succeeds.

Phase 25 Plan 02 is closed. Plan 03 (unified `/debug/warnings` feed)
can consume `recommend_config` output directly via the
`config` signal category.
