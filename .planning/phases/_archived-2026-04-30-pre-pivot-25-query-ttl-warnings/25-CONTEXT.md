# Phase 25: Query surface, TTL, warnings - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation + Phase 24 artifacts

<domain>
## Phase Boundary

Three concerns, each shippable independently but grouped here for cohesion:

1. **Query surface** — GET_MULTI opcode for multi-table feature-vector assembly in one TCP round-trip. No SCAN, no SUBSCRIBE in v0 (reserved with NotImplemented).

2. **TTL defaults + suggestion engine** — Table `ttl="30d"`, Stream `history_ttl="90d"`, tombstone `7d`. User-overridable on decorator. Background suggestion engine tracks eviction/compaction/backfill-miss signals and exposes recommendations via HTTP + CLI.

3. **Unified warnings** — `/debug/warnings` endpoint with severity-sorted JSON feed across categories (config, data_quality, operational, safety, performance). Used by UI; no external alert delivery in v0.

**Out of scope:**
- SCAN / SUBSCRIBE opcodes — reserved, return "not implemented"
- External alert delivery (email/PagerDuty/webhook) — post-v0
- Per-key TTL / per-row tombstone grace override — post-v0
- Test migration (Phase 26)

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Query surface

- `GET_MULTI` opcode: payload `{table_names: [string], key: JSON}` → response `{table → row | null}`
- Client-composable: single-table GET still works via existing opcode
- Null-collapse: never-seen, tombstoned, and pending keys all return null
- `SCAN` and `SUBSCRIBE` opcodes reserved but return `Err("not implemented in v0; reserved for post-v0")`

### TTL defaults

- `@tl.table(ttl="30d")` default — row-level TTL, not per-key
- `@tl.stream(history_ttl="90d")` default — event log retention
- Tombstone retention 7d (per Phase 24's `TOMBSTONE_GRACE`)
- User-overridable on decorator: `@tl.table(ttl="180d")`, `@tl.stream(history_ttl="30d")`
- Defaults applied at `app.register` if not specified

### Suggestion engine

- Per-Table metrics:
  - `tally_ttl_evictions_total{table}` — count of keys evicted by TTL
  - `tally_ttl_eviction_then_reinit_total{table}` — keys evicted then re-seen within 7d (signal: TTL too short)
  - Bloom filter of recently-evicted keys (7d rolling, ~1-2 MB per Table)
- Per-Stream metrics:
  - `tally_history_compacted_total{stream}` — events aged out of log
  - `tally_history_backfill_misses_total{stream}` — backfill requests that hit compaction boundary
  - `tally_max_backfill_span_seen{stream}` — largest observed backfill window
- `/debug/config-recommendations` HTTP endpoint returns JSON:
  ```
  {
    "generated_at": "...",
    "observation_window": "7d",
    "recommendations": [
      {
        "knob": "UserProfile.ttl",
        "current": "30d",
        "suggested": "60d",
        "confidence": 0.72,
        "reason": "12% of TTL-evicted keys reactivated within 24h",
        "evidence": {"evictions_24h": 48213, "reinit_after_eviction_24h": 5786},
        "copy_paste": "@tl.table(key=\"user_id\", ttl=\"60d\")"
      }
    ]
  }
  ```
- `tally suggest-config` CLI (new binary in `src/bin/`) hits the endpoint, prints copy-pasteable decorator lines grouped by file
- Advisory log on startup: if any recommendation exists, print a terse one-liner per knob

### Warnings

- `/debug/warnings` endpoint: severity-sorted JSON feed
- Schema:
  ```
  {
    "warnings": [
      {
        "id": "ttl-too-short-UserProfile",
        "severity": "info" | "warning" | "error" | "critical",
        "category": "config" | "data_quality" | "operational" | "safety" | "performance",
        "title": "...",
        "detail": "...",
        "action": {...},
        "first_seen": "...",
        "evidence_url": "/debug/config-recommendations#UserProfile.ttl"
      }
    ]
  }
  ```
- Signal sources:
  - `config`: from suggestion engine
  - `data_quality`: late-event drop rate > 1% (from Phase 24), schema validation failures
  - `operational`: memory pressure > 85%, snapshot write failures, disk usage
  - `safety`: registration failures (from cycle detection etc.)
  - `performance`: p99 latency SLO breach (user-configurable threshold)
- Dedupe via `id` (same id updates existing; doesn't duplicate)
- UI renders; no external alert delivery in v0

### Observability integration

- All new metrics exposed via existing `/metrics` (Prometheus format) from Phase 10.2/22
- Suggestion engine + warnings metrics plumb through the same aggregation tracker
- `/debug/streams/:name` (from Phase 24) extended with eviction/compaction counters

</decisions>

<code_context>
## Existing Code Insights

- `src/server/http.rs` — existing `/metrics`, `/debug/key/:key`, `/debug/streams/:name` endpoints; add `/debug/warnings` + `/debug/config-recommendations`
- `src/state/store.rs` — TTL eviction already exists in v2.0; add bloom-filter tracking for eviction→reinit signal
- `src/state/snapshot.rs` — add eviction/compaction counters to snapshot (optional; can be in-memory only)
- `src/server/tcp.rs` — add GET_MULTI opcode
- `src/server/protocol.rs` — add opcode constants
- `python/tally/_protocol.py` — encode GET_MULTI
- `python/tally/_app.py` — `app.get_multi(tables, key)` SDK method

</code_context>

<specifics>
## Specific Ideas

- **Bloom filter for eviction tracking**: 2 MB per Table for 7-day window, ~1% false positive. Cheap.
- **Startup advisory log**: single-line summary per recommendation. Don't spam.
- **Warning dedupe**: in-memory registry, keyed by warning `id`. Rebuild on restart (warnings are always computed on-demand from observable state).

</specifics>

<deferred>
## Deferred Ideas

- SCAN (range/predicate queries) — post-v0
- SUBSCRIBE (live push notifications) — post-v0
- External alert delivery (email, PagerDuty, webhooks) — post-v0
- Per-key TTL overrides — post-v0
- Tunable tombstone grace per-table — post-v0

</deferred>

---

*Phase: 25-query-ttl-warnings*
*Design decisions from v0-restructure-spec.md §7-8*
