# Phase 51: cross-shard-queries-joins - Discussion Log

> Audit trail only. Decisions in CONTEXT.md.

**Date:** 2026-04-18
**Areas discussed:** Global watermark cadence · Hot-shard threshold · JoinShardKeyMismatch channel

## Global watermark publish cadence
User clarifying question first: "why do we need global watermark?" Answered: three external consumers (GET /streams/{name}, fork/replica OP_SUBSCRIBE wire value, unlabeled beava_watermark_lag_seconds SRE gauge). TTL eviction and co-located joins use shard-local only. Lazy publish avoids per-event atomic cost.

**Chosen:** Every N=1024 events per shard per stream. → D-01, D-02, D-03.

## /debug/shards hot-shard threshold
**Chosen:** `keys_owned > 1.5× fleet mean` (user picked tighter than 2× recommendation). Tunable via BEAVA_HOT_SHARD_THRESHOLD env. → D-07, D-08.

## JoinShardKeyMismatch error channel
**Chosen:** Dual-channel — synchronous error to SDK caller during registration + emit to /debug/warnings. Pipeline does not start. Error message format locked for grep-testability. → D-10, D-11, D-12.

## Claude's Discretion
- Global-watermark atomic storage type.
- Reactor utilization computation (tokio metrics vs explicit EWMA).
- Suggested-common-field heuristic in JoinShardKeyMismatch message.
