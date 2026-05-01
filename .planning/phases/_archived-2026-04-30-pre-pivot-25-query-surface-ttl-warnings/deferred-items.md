# Phase 25 — Deferred Items

## From Plan 25-03

### R3: Tombstone-grace-expired read rate recommendation

- **What:** A third recommendation rule — when a Table's
  `grace_expired_read_count` exceeds 10/hr over 24h, suggest doubling
  `tombstone_grace`.
- **Why deferred:** Requires adding a `grace_expired_read_count` counter
  on the merged-GET path in the Phase 24 table_rows storage code. The
  25-02 executor did not take on this cross-cutting change when
  absorbing the 25-03 TTL/bloom/recommendation scope.
- **What's in place:** `src/engine/recommend.rs` has R1 (reinit rate) and
  R2 (history_ttl below downstream). The signal emission + endpoint
  plumbing already supports arbitrary additional rules — a follow-up
  plan only needs to add the counter + one more rule case in
  `recommend_config`.
- **Suggested follow-up:** Minor plan in Phase 26 (or a TTL tightening
  plan) wiring grace_expired_read_count into `StateStore` and extending
  `recommend_config`.
