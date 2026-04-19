//! `BEAVA_FJALL_*` env-var name constants (Phase 53 Plan 02, TPC-PERSIST-06).
//!
//! These string constants are the single source of truth for env-var names
//! used by docs, error messages, and the Plan 06 operations runbook. The
//! clamp + parse logic lives in [`crate::shard::fjall_backend`] (close to
//! the fjall-consuming hot path); this file provides the ops-facing surface.
//!
//! Layout follows the existing `src/config/shards.rs` pattern (one file per
//! env-var family). The project uses a `src/config/` module directory
//! (imported as `crate::config`) rather than a single `src/config.rs` file;
//! callers reach these constants via either
//! `beava::config::fjall::BEAVA_FJALL_FSYNC_MS` or the re-export from
//! `beava::shard::fjall_backend::BEAVA_FJALL_FSYNC_MS`.
//!
//! See `.planning/phases/53-fjall-state-backend/53-RESEARCH.md`
//! §BEAVA_FJALL_* Environment Variables for the authoritative clamp table.

pub use crate::shard::fjall_backend::{
    BEAVA_FJALL_BLOCK_SIZE, BEAVA_FJALL_CACHE_MB, BEAVA_FJALL_COMPACTION_WORKERS,
    BEAVA_FJALL_FLUSH_WORKERS, BEAVA_FJALL_FSYNC_DISABLE, BEAVA_FJALL_FSYNC_MS,
    BEAVA_FJALL_MAX_MEMTABLE_MB,
};
