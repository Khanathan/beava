---
status: partial
phase: 04-persistence-and-operational-readiness
source: [04-VERIFICATION.md]
started: 2026-04-09T00:00:00Z
updated: 2026-04-09T00:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Crash recovery end-to-end
expected: Kill and restart server, GET reflects pre-crash state (features pushed before kill are still available after restart)
result: [pending]

### 2. Concurrent push during snapshot
expected: PUSH latency stays under 1ms while snapshot is writing (clone-then-spawn_blocking does not block event loop)
result: [pending]

### 3. TTL eviction observable
expected: Wait 2x window after last event for an entity, confirm entity disappears from memory and GET returns empty
result: [pending]

## Summary

total: 3
passed: 0
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps
