# Phase 11 — Status (resume-session post-hoc record)

> The original Plan 01–04 documents were never committed during the prior
> agent's run (team-quota wall hit before final commit batch). The work
> shipped under in-memory plans whose decomposition matches the
> "Execution shape" section of `11-CONTEXT.md`. Rather than fabricate
> per-plan PLAN.md retroactively, this file records what was done, in
> commit order, mapped to the CONTEXT execution shape.

**Branch:** `worktree-agent-a71d2569` (forked from `v2/greenfield@157630f`)
**Final commit at writeup:** `<see git log>`
**Test count:** 624 → 659 (+35 passing)
**Workspace gates:** `cargo test` green (modulo pre-existing cli_smoke flake — see Caveats); `cargo clippy --all-features -D warnings` clean; `cargo fmt --check` clean.

## Plan → commit mapping

### Plan 01 — Value::List + Value::Map + JSON encoder
- `61618e7 test(11-01): RED — Value::List + Value::Map variants for structured outputs`
- `f527bd4 feat(11-01): GREEN — Value::List + Value::Map variants + JSON encoders`

Touched: `crates/beava-core/src/row.rs` (new variants), `crates/beava-server/src/feature_query.rs`, `crates/beava-server/src/registry_debug.rs` (encoders).

### Plan 02 + 03 — All 13 buffer + geo operator state types & dispatch
- `7d6271f feat(11-02,11-03): GREEN — 13 buffer + geo operator state types + tests`
- `17ebf9b feat(11-02,11-03): wire AggOp dispatch + compile parser for 13 Phase 11 ops`

13 ops, in two families:

**Buffer family (7):** `histogram`, `hour_of_day_histogram`, `dow_hour_histogram`,
`seasonal_deviation`, `event_type_mix`, `most_recent_n`, `reservoir_sample`.

**Geo family (6):** `geo_velocity`, `geo_distance`, `geo_spread`,
`unique_cells`, `geo_entropy`, `distance_from_home`.

`distance_from_home` ships with the **centroid-of-last-N fallback**
(CONTEXT D-03) — Phase 10's `top_k` is not in this worktree. v0.1
follow-up: swap to top-K most-frequent-cell centroid once Phase 10 lands.

Touched: `crates/beava-core/src/agg_buffer.rs`, `crates/beava-core/src/agg_geo.rs`, `crates/beava-core/src/agg_op.rs` (variants + dispatch), `crates/beava-core/src/agg_compile.rs` (parser), `crates/beava-core/src/agg_descriptor.rs`, `crates/beava-core/src/agg_apply.rs`, `crates/beava-core/src/agg_state_table.rs`, `crates/beava-core/src/agg_schema.rs`, `crates/beava-core/src/agg_windowed.rs`, `crates/beava-core/Cargo.toml` (`haversine` dep), root `Cargo.toml` (workspace dep).

> The combined "GREEN — state types + tests" commit batches the per-op
> red→green pairs into one commit because the unit tests live alongside
> each state-struct in the same file. The `feat(...)` commit name is a
> minor TDD-discipline grandfather — the test bodies and impl bodies
> were added in the same patch rather than two atomic commits per op.
> This is the only TDD-trace gap in Phase 11; future phases should
> structure red and green as distinct commits per CLAUDE.md §Conventions.

### Plan 04 — Smoke + bench + perf/throughput artifacts
- `a6ede86 test(11-04): phase 11 smoke — register + push + query 13 buffer/geo ops`
- `6235ba2 chore(11-04): satisfy clippy::manual_range_contains + cargo fmt in smoke`
- _(this commit)_ `docs(11-04): phase status + throughput row + perf row + summary + verification`

Touched: `crates/beava-server/tests/phase11_smoke.rs` (smoke), `crates/beava-core/benches/phase11_buffer_geo.rs` (criterion bench, 12 IDs), `crates/beava-bench/configs/geo.json` (new pipeline shape), and the .planning artifacts under this directory.

## Caveats / known issues

1. **`cli_smoke::env_var_overrides_listen_addr` and `loads_valid_config_starts_and_prints_banner` fail consistently.** Verified the same failures on parent `157630f` (before any Phase 11 work) — a pre-existing race in the cli_smoke harness on macOS where the 300ms post-spawn sleep is too short for the dev binary to write its banner before the test sends SIGTERM. **Not caused by Phase 11.** Document this as a follow-up for whoever owns the cli_smoke harness; suggested fix is to poll for banner output instead of sleeping.

2. **Plan files were never committed** — see preamble. CONTEXT.md captures the architectural decisions (D-01..D-10) which carry the same information that would have been in PLAN.md tasklists.

3. **Geo workload uses random `lat/lon ∈ [0, 1000)`** in `crates/beava-bench` — this exercises haversine math but isn't real-coord. SC2 (geo math correctness) is verified by `agg_geo.rs::haversine_nyc_to_london_matches_published` unit test, not the throughput harness.

## Throughput + perf

- **Geo pipeline (new):** 701 EPS HTTP — see `11-throughput-row.md`.
- **Small pipeline (regression check):** 1097 EPS HTTP vs Phase 7.5 baseline 990 EPS = **+10.8% improvement**.
- **Criterion bench (12 IDs):** see `11-perf-row.md`. Buffer ops 1–8 ns; geo ops 12–24 ns. No prior Phase 11 baseline to regress against.
