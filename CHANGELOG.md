# Changelog

All notable changes to Beava. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows [Semantic Versioning](https://semver.org/) once it reaches `1.0.0`.

## [Unreleased]

### Added

- Fork-replay benchmark (`benchmark/fork-replay/`) — rate-limited or unthrottled pusher + fork driver + orchestrator. Measures catchup wall-clock for `bv.fork()` against an upstream with accumulated events. Committed baseline (5 M event stress test): **5,000,000 events → 30.3 s fork catchup → 165,092 replay EPS → 100% entities preserved (1,000 / 1,000)** on 10-core Apple M4, 1,000 distinct entities, simple `count` pipeline. Replay EPS is about half of peak ingest EPS (315K), meaning a 1-hour backlog catches up in ~2 hours on this hardware.
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
