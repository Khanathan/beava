# Phase 25: Query surface, TTL, warnings - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation

<domain>
## Phase Boundary

Three independent deliverables that close out the v0 engine's user-facing surface:

1. **GET_MULTI opcode** — read features from multiple Tables for a single key in one round-trip (ML inference use case)
2. **Unified `/debug/warnings` endpoint** — severity-sorted JSON feed of health/config/data-quality signals; feeds the UI
3. **TTL defaults + suggestion engine** — per-Table `ttl` + per-Stream `history_ttl` defaults with user override, telemetry-driven recommendations exposed via `/debug/config-recommendations` + `tally suggest-config` CLI

Each can be plan-split independently. Sequential because GET_MULTI touches the TCP protocol, which other tests depend on.

**Out of scope:**
- SCAN opcode (v0.1)
- SUBSCRIBE / live updates (v0.1)
- External alert delivery (webhooks, email, PagerDuty) — UI-only in v0
- Per-feature TTL (stick with per-Table/per-Stream granularity)

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### GET_MULTI opcode

Protocol shape:
```
GET_MULTI (0x0D)
  count: u8                   # number of table names (max 32)
  [table_name: string] × count
  key: string                 # (or JSON array for composite keys)
  → Response: {table_name → row_json | null}
```

Response semantics:
- Null-collapse on not-found (per E2.2 lock): never-seen / tombstoned / pending all return `null`
- No partial results — if any table name is malformed, whole request errors

Python SDK: `app.get_multi(["UserProfile", "UserSpend", "UserRisk"], key="u1")` → `{table_name: row_or_None}`

Use case: ML feature vector assembly — 3× tables × 1× round-trip instead of 3× round-trips.

### /debug/warnings unified endpoint

Response shape (frozen):
```json
{
  "generated_at": "2026-04-14T10:00:00Z",
  "observation_window": "7d",
  "warnings": [
    {
      "id": "<stable-dedupe-key>",
      "severity": "info | warning | error | critical",
      "category": "config | data_quality | operational | safety | performance",
      "title": "<short>",
      "detail": "<longer>",
      "action": {"type": "config_change | investigate", ...},
      "first_seen": "<timestamp>",
      "evidence": {<source-specific data>}
    }
  ]
}
```

Signal sources (all feed the same endpoint):
- **config** — TTL recommendations, history_ttl recommendations (from suggestion engine, below)
- **data_quality** — late-event drop rate above threshold, schema validation failures in aggregated operators
- **operational** — memory pressure > 85%, snapshot failure in last cycle, disk usage high
- **safety** — REGISTER failures, panic in log, backfill errors
- **performance** — p99 latency SLO breach (placeholder SLOs for v0; expose knobs)

Dedupe via `id` — same warning re-fires update existing entry rather than duplicate.

Severity ladder: `info` (advisory only) | `warning` (should look) | `error` (something failed recently) | `critical` (ongoing problem).

No external delivery (webhooks, alerts) in v0 — UI reads the endpoint and renders.

### TTL defaults + override + suggestion engine

Defaults:
- `@tl.table(ttl="30d")` — entity-level TTL
- `@tl.stream(history_ttl="90d")` — event log retention
- Tombstone retention: 7d after delete

Override via decorator kwargs (already in Phase 21 SDK; engine must honor).

**Suggestion engine** — three tiers:

1. **Passive metrics** (always on):
   - `tally_ttl_evictions_total{table}`
   - `tally_ttl_eviction_then_reinit_total{table}` (bloom-filter-tracked)
   - `tally_history_compacted_total{stream}`
   - `tally_history_backfill_misses_total{stream}`

2. **`/debug/config-recommendations`** HTTP endpoint:
   ```json
   {
     "recommendations": [{
       "knob": "UserProfile.ttl",
       "current": "30d",
       "suggested": "60d",
       "confidence": 0.72,
       "reason": "12% of TTL-evicted keys reactivated within 24h.",
       "evidence": {"evictions_24h": 48213, "reinit_after_eviction_24h": 5786},
       "copy_paste": "@tl.table(key=\"user_id\", ttl=\"60d\")"
     }]
   }
   ```
   Feeds directly into `/debug/warnings` category=`config`.

3. **`tally suggest-config` CLI** — pretty-printer over the endpoint; prints copy-paste decorator lines grouped by file.

**Bloom filter for reinit detection:** per-Table recently-evicted keys bloom filter (~2MB for 1M evictions/7d at 1% FP rate). Age out via double-buffering every 24h.

### Observability bus

Suggestion engine + warnings endpoint share an internal "SignalRegistry" that:
- Accepts writes from operator/storage/protocol paths
- Dedupes by signal id
- Ages out old signals (7d default observation window)
- Serves `/debug/warnings` responses by serializing the current registry state

### Scope boundary

- Don't touch core aggregation/join code from Phases 22/23 except to emit signals (late drops, REGISTER failures, etc.)
- Don't touch event-time / watermark logic (Phase 24 closed)
- Focus is additive: new opcode, new endpoint, new CLI command, new TTL config knobs

</decisions>

<code_context>
## Existing Code Insights

- `src/server/tcp.rs` — protocol dispatch; add GET_MULTI opcode
- `src/server/http.rs` — HTTP management; add /debug/warnings, /debug/config-recommendations
- `src/state/store.rs` — EntityState + TableRows (from Phase 24); TTL eviction logic lives here
- `src/state/eviction.rs` — TTL eviction (if exists); extend with bloom-filter reinit tracking
- `python/tally/_app.py` — App class; add `get_multi()` method
- `python/tally/__init__.py` — CLI entry; add `tally` command group with `suggest-config` subcommand
- Phase 24's per-stream watermark + late-drop counter — already in metrics; connect to warnings
- Existing `/metrics` endpoint — Prometheus format; extend with TTL-related counters

</code_context>

<specifics>
## Specific Ideas

- **Plan split:**
  - **25-01**: GET_MULTI opcode (protocol + Python SDK)
  - **25-02**: /debug/warnings unified endpoint + SignalRegistry internal bus
  - **25-03**: TTL suggestion engine + /debug/config-recommendations + `tally suggest-config` CLI (depends on 25-02's registry)

- **Warnings integration**: when suggestion engine produces a rec, emit into SignalRegistry with category=`config`. /debug/warnings picks it up for free.

- **CLI scaffolding**: check if a `tally` CLI binary exists. If not, this phase introduces it. Keep it simple — argparse + HTTP client.

</specifics>

<deferred>
## Deferred Ideas

- SCAN opcode with predicate language (v0.1)
- SUBSCRIBE opcode with backpressure (v0.1)
- Per-feature TTL (v0.1 if demand)
- External alert delivery (v0.1+)
- Warning history persistence (today: in-memory registry; restart loses signals)

</deferred>

---

*Phase: 25-query-surface-ttl-warnings*
*Sources: `.planning/research/v0-restructure-spec.md`, Phase 24 summary, v0 design conversation 2026-04-14*
