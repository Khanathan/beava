#![warn(missing_docs)]
//! Beava — real-time feature server, single-binary Rust.
//!
//! This crate exposes the engine, server, client, and state surfaces. See
//! `docs/architecture.md` for the system design and `docs/http-api.md` for
//! the public HTTP surface.

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
