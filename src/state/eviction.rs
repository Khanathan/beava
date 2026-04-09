//! TTL-based key eviction.
//!
//! Keys with no events for 2x the largest window are evicted from memory.
//! Evicted keys re-initialize fresh on next event (CLAUDE.md spec).
