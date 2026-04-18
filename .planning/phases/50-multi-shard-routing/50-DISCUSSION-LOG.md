# Phase 50: multi-shard-routing - Discussion Log

> Audit trail only. Decisions in CONTEXT.md.

**Date:** 2026-04-18
**Areas discussed:** Shard lifecycle · macOS dispatcher · Panic recovery · Metrics migration

## Shard thread lifecycle
**Chosen:** Spawn-all-at-boot + ready-gate. Alternatives: lazy-on-first-accept, spawn+bind-parallel (rejected).
→ D-01.

## macOS N>1 dispatcher shape
**Chosen:** Single listener + inline dispatch. Alternatives: dedicated dispatcher thread (rejected — extra hop), thread-per-listener (rejected — over-engineered for dev).
→ D-04, D-05.

## Shard panic recovery policy
**Chosen:** Quarantine + 503 per shard (catch_unwind, mark DOWN, 503 for that shard only, /ready flips). Alternatives: fail-the-whole-server (rejected — too harsh), auto-restart (rejected — state loss).
→ D-02, D-03.

## Metrics migration cutover
**Chosen:** Parallel period (hand-rolled + metrics-crate both live in Wave 2; hand-rolled removed in Wave 4). Alternatives: hard cutover (rejected — breaks alert rules), explicit /metrics-v2 (rejected — two surfaces forever).
→ D-06, D-07.

## Claude's Discretion
- TCP error-code discriminants for SHARD_OVERLOAD, SHARD_KEY_MISSING.
- Shard-thread spawn shell (std::thread::Builder naming).
- metrics-crate wiring idiom (macros vs Recorder).
- Ready-barrier primitive (Notify vs Condvar vs WaitGroup).
