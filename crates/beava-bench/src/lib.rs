//! End-to-end throughput harness for the Beava server.
//!
//! - [`blast_shape`] — pre-encoded frame pool used by the standalone bench
//!   binaries to eliminate per-iteration encode + RNG cost from the hot loop.
//! - [`cli`] — `beava bench <mode>` subcommand surface (throughput / mixed /
//!   memory / fsync) plus the interactive walkthrough.
//! - [`harness`] — minimal in-process `TestServer` harness shared by the CLI
//!   mode modules.
//! - [`workloads`] — adtech / fraud / ecommerce dataset workloads plus the
//!   small / medium / large pipeline shapes backed by `configs/*.json`.
pub mod blast_shape;
pub mod cli;
pub mod harness;
pub mod workloads;
