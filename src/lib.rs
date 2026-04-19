#![warn(missing_docs)]
//! Beava — real-time feature server, single-binary Rust.
//!
//! This crate exposes the engine, server, client, and state surfaces. See
//! `docs/architecture.md` for the system design and `docs/http-api.md` for
//! the public HTTP surface.

/// Runtime configuration modules (v1.2 TPC Wave 1: BEAVA_SHARDS, shard count resolution).
// Phase 49: Wave 1 config surface — D-10/D-11.
#[allow(missing_docs)]
pub mod config;

/// Client library for connecting to a Beava server (snapshot fetch, subscribe).
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod client;

/// Human-friendly duration parser (`"30s"`, `"5m"`, `"1h"`).
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod duration;

/// Core streaming engine: pipeline registration, operator execution, DAG evaluation.
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod engine;

/// Error types returned by the Beava public API.
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod error;

/// Shard routing: `shard_hint` trait and hash-based routing primitive (v1.2 TPC Wave 0).
// Phase 48: Wave 0 scaffolding — call-and-discard at N=1. Full routing lands in Wave 2.
#[allow(missing_docs)]
pub mod routing;

/// Per-shard state: Shard struct, ShardedStateStore trait, ShardedStateStoreV1 impl (v1.2 TPC Wave 1).
// Phase 49: Wave 1 plumbing — N=1 only. N>1 routing lands in Wave 2.
#[allow(missing_docs)]
pub mod shard;

/// HTTP + TCP server, auth, replica, and observability surfaces (feature = "server").
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[cfg(feature = "server")]
#[allow(missing_docs)]
pub mod server;

/// State store: event log, snapshots, eviction, and key-value persistence.
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod state;

/// Shared types: `FeatureValue`, `EventRecord`, `KeyedRow`, and wire formats.
// Phase 47 audit: deferred sub-module docs to post-launch (D-11 scope: crate root only)
#[allow(missing_docs)]
pub mod types;

/// Prometheus metrics recorder wiring (Phase 50-01, D-06 parallel period).
/// Installs global metrics-exporter-prometheus recorder alongside hand-rolled /metrics.
#[allow(missing_docs)]
pub mod metrics;

/// Offline reshard migration tool (TPC-DX-03, Phase 52-04).
/// Provides `reshard_data_dir`, `rehash_to_shard`, `swap_replace`, and CLI helpers.
#[allow(missing_docs)]
pub mod reshard;

/// Phase 53-04: `tally migrate-to-fjall` — convert v8 snapshot entity state to
/// per-shard fjall partitions in-place. Closes TPC-PERSIST-03.
#[cfg(not(feature = "state-inmem"))]
#[allow(missing_docs)]
pub mod migrate_to_fjall;
