# Changelog

All notable changes to Beava. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows [Semantic Versioning](https://semver.org/) once it reaches `1.0.0`.

## [0.1.0] - 2026-04-17 (v1.0-launch)

First public release. Apache 2.0. Single-binary feature server in Rust.

### Added

- **HTTP Ingest & Read API** (Phase 45): 6 endpoints.
  - `POST /push/{stream}` — single-event push with schema validation (HTTP-01).
  - `POST /push-batch/{stream}` — batch push with per-event accept/reject response + correct event-time bucketing (HTTP-02).
  - `POST /push/{stream}/ndjson` — chunked NDJSON streaming via axum-extra JsonLines (HTTP-03).
  - `GET /features/{key}` — feature read across all tables; `?table=X` filter (HTTP-04).
  - `GET /streams` + `GET /streams/{name}` — list and inspect registered streams (HTTP-05).
  - Loopback-or-token auth inherited unchanged; `--public` exposes read-only routes (HTTP-06/07).
- **Docker image** `beavadb/beava:latest` + `:0.1.0` — multi-stage cargo-chef to distroless/cc-debian12:nonroot. Under 200 MB, runs non-root.
- **GitHub Actions CI**: fmt, clippy, nextest, Python SDK 3.10/3.11/3.12 matrix. Green badge on README.
- **New docs pages**: `docs/getting-started.md`, `docs/concepts.md`, `docs/operations.md`, `docs/architecture.md`, `docs/faq.md`, `docs/comparison.md`, `docs/python-sdk.md` (polish pass), `docs/http-api.md` (polish pass), `docs/event-time.md` (deep reference from Phase 46).
- **New examples**: `examples/fraud-scoring/`, `examples/session-features/`, `examples/curl-ingest/`.

### Fixed

- **2a batch-path event-time bucketing** (CORR-01): `push_batch_with_cascade_no_features` now accepts `&[(&Value, SystemTime)]` and groups by event-time bucket — no more shared `now` collapsing a batch's distinct event times.
- **2d.ii backfill event-time** (CORR-06): `run_backfill` uses `parse_event_time(&payload, entry.timestamp)` so replayed events bucket by payload `_event_time`, not log entry wall-clock.
- **2d.iii TTL clock source** (CORR-07): `entity_ttl` / `history_ttl` sourced from `WatermarkTracker::observed_max(stream)` — 30-day-old historical events no longer evict immediately on replay.
- **2d.iv fork replica cascade** (CORR-08): `replica_ingest_batch` calls `watermarks.observe()` per event so fork watermarks advance correctly.
- **2d.vi/vii dirty-set race** (CORR-10): atomic swap of `DashSet<String>` via `take_dirty_and_advance_gen()`.

### Changed

- Per-stream `watermark_lateness` (`@bv.stream(watermark_lateness="10m")`) now supported; default 5 s preserved for backward compat (CORR-03/04).

### Observability

- New Prometheus counter `beava_ring_buffer_drops_total{stream, operator_kind, reason}` with bounded labels (OBS-01).
- `beava_late_events_dropped_total` and `beava_ring_buffer_drops_total` are mutually exclusive (OBS-02).
- `docs/event-time.md` is the authoritative event-time reference (OBS-03).

### Security

- No TLS in-process. Terminate at Caddy/nginx/Fly.io edge. Admin-token auth via `BEAVA_ADMIN_TOKEN`.

### Known Limitations

- Single-node only. HA is Beava Cloud (roadmap).
- At-least-once delivery semantics. Exactly-once is NOT claimed.
- `tally` binary name preserved for v1.0-launch (rename to `beava` in v1.1).

## [Unreleased]

### Added

- Fork-replay benchmark (`benchmark/fork-replay/`) — rate-limited or unthrottled pusher + fork driver + orchestrator. Measures catchup wall-clock for `bv.fork()` against an upstream with accumulated events. Committed baseline (5 M event stress test): **5,000,000 events → 11.5 s fork catchup → 436,109 replay EPS → 100% entities preserved (1,000 / 1,000), 0 feature-value mismatches on 20 sampled keys** on 10-core Apple M4, 1,000 distinct entities, simple `count` pipeline. Replay now exceeds peak ingest EPS because the replica-side batch path amortizes the event-log `write()` syscall and engine read lock across the batch.
- Replica-side batch ingest (`replica_ingest_batch`) — the LOG_FETCH catchup loop now accumulates up to 1,000 events and flushes them through a single code path that holds the engine read lock once, calls `store.mark_dirty_many` once per touched stream, and issues one `append_many_with_ts` `libc::write()` syscall per stream. Per-event `event_time` semantics are preserved: `LogEntry.timestamp` is written per-event so downstream forks observing this replica via `handle_log_fetch` see the same ts_ms stream upstream would have emitted.
- `EventLog::append_many_with_ts` — batched log append with per-event timestamps (the existing `append_many` uses a single batch-wide `now`). Single `libc::write()` call per batch, same partial-write fallback as the single-event path.
- Recovery benchmark (`benchmark/recovery/`) — snapshot-restore wall-clock at peak EPS. Committed baseline: 10.3M events / 4.7 GB on-disk state → 7.04 s recovery → 24,945 / 24,945 entities preserved (100%) on 10-core Apple M4.
- Server-side PUSH latency instrumentation in `handle_push_batch`. `/debug/latency` and `/metrics` `beava_push_latency_p99_seconds` now report a meaningful number under batch-push load (previously reported 0 because the batch path was not wired to the histogram).

### Fixed

- **LOG_FETCH emits events for keyless streams under scope filter.** Streams registered via `@bv.stream` have no `key_field` on the server side (the key lives on downstream `@bv.table`). A fork passing `keys` or `key_prefix` was silently dropping every event at the "keyless + filter" branch, producing `catchup_seconds = 0` with `keys_total = 0`. Fallback path now decodes the event and scans all string fields for a scope match — lets the common pattern (keyless stream → keyed table) just work.
- README and site copy corrected against AUDIT-V11: removed fabricated claims about "every push fsynced before ack" (actually: write-appended before ack, fsync on a 1 s timer), "~1 s data loss window" (actually: delta snapshot interval 30 s + fsync window 1 s), "load snapshot + replay WAL tail on restart" (actually: snapshot-only restore; WAL is not replayed into operator state).
- `BEAVA_MEMORY_LIMIT_MB` copy clarified: drives an operational signal at 85%/95% RSS via `/debug/warnings`; does NOT reject writes. The operator is expected to alert and resize before OOM.

### Removed

- S2 (s2.dev) archive backend (reverted — out of core scope, v1.1 concern).
- Python SDK `RetryPolicy` / `DEFAULT_POLICY` / `NO_RETRY` (reverted — application-layer concern).
- `ServerBusyError` + `at_memory_ceiling` reject-writes gate (reverted — signal-only is the current semantic).
- `beava_fsync_stall_seconds_total` metric (reverted — operational polish, not core).
- `BEAVA_FSYNC_INTERVAL_MS` and `BEAVA_SNAPSHOT_INTERVAL_MS` env-var tunables (reverted — hardcoded to 1 s and 30 s respectively, matching Redis `appendfsync everysec`).

## Pre-history

This project was published as open source in 2026. Pre-publication commits are in the git log for archaeological purposes; they predate SemVer and the Keep a Changelog format.
