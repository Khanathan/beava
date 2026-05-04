//! beava-bench library surface (Phase 13.5+).
//!
//! - `blast_shape` (Phase 19+) — pre-encoded frame pool used by the legacy v18
//!   binary harness in `src/bin/`.
//! - `cli` (Phase 13.5 Plan 08+) — `beava bench <mode>` subcommand surface
//!   with 4 modes: throughput / mixed / memory / fsync.
//! - `harness` (Phase 13.5 Plan 08+) — minimal in-process TestServer harness
//!   shared by the CLI mode modules.
//! - `workloads` (Phase 13.5 Plan 09+) — adtech / fraud / ecommerce dataset
//!   workloads + legacy small/medium/large pipeline shapes.
pub mod blast_shape;
pub mod cli;
pub mod harness;
pub mod workloads;
