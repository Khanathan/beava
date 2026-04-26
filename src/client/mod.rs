//! Client-side surface. After Phase 38-01 (Option K mothball) this module
//! keeps only the minimal types reused by the Option M replica-mode server
//! (`src/server/replica_client.rs`) and the feature-flag smoke test
//! `tests/client_engine_roundtrip.rs`.
//!
//! What lives here:
//!   - `wire` — the `Scope` codec + opcode / frame-tag constants reused by
//!     `server::replica_client::{write_scope, ...}` (Phase 36).
//!   - `Session` / `SessionMode` / `OutOfScopeError` — tiny value types kept
//!     for the Phase 28-01 feature-flag machinery so that `--no-default-features
//!     --features client` continues to compile a non-trivial surface.
//!
//! What used to live here and was deleted in Phase 38-01:
//!   - `clone.rs` — embedded historical-clone path (Option K). Superseded by
//!     Phase 36's `server::replica_client::fetch_historical_snapshot`.
//!   - `streaming.rs` — embedded `StreamingClient` (Option K). Superseded by
//!     Phase 36's replica-mode SUBSCRIBE loop.
//!   - `state.rs` — `StreamingStore` wrapper. Dead after the above.
//!   - `session.rs` — shared handshake helpers. Phase 36's replica_client has
//!     its own inline equivalents against the server codec directly; the
//!     client-side duplicate is no longer reused by anything live.
//!   - `FrozenClient` + its `get` / `iter_entities` surface. Only caller was
//!     the deleted `python-native` PyO3 Pipeline.
//!
//! This module stays UNCONDITIONAL (no `#[cfg]`) so both server and client
//! feature builds compile it identically.

pub mod wire;

/// Session execution mode. Historical is the only mode retained; Streaming
/// was the Option K add-on and is now served by the Option M replica-mode
/// server boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionMode {
    Historical,
}

/// Client-side session descriptor. A value type kept for the feature-flag
/// smoke test; the live Option M path uses `ReplicaBootConfig` in `main.rs`
/// instead.
#[derive(Debug, Clone)]
pub struct Session {
    pub remote: String,
    pub streams: Vec<String>,
    pub keys: Option<Vec<String>>,
    pub key_prefix: Option<String>,
    pub mode: SessionMode,
    pub token: Option<String>,
}

impl Session {
    pub fn new(remote: impl Into<String>, streams: Vec<String>) -> Self {
        Self {
            remote: remote.into(),
            streams,
            keys: None,
            key_prefix: None,
            mode: SessionMode::Historical,
            token: None,
        }
    }
}

/// Raised when a query targets a stream / key outside the declared scope.
/// Retained as a public type even though the embedded `FrozenClient` that
/// originally threw it is gone — external consumers (if any) that linked
/// against the Phase 28-04 surface still see the same error type name.
#[derive(Debug, thiserror::Error)]
#[error("query out of scope: {0}")]
pub struct OutOfScopeError(pub String);

impl OutOfScopeError {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_new_defaults() {
        let s = Session::new("host:6400", vec!["Txn".into()]);
        assert_eq!(s.remote, "host:6400");
        assert_eq!(s.streams, vec!["Txn".to_string()]);
        assert!(matches!(s.mode, SessionMode::Historical));
    }

    #[test]
    fn out_of_scope_display() {
        let e = OutOfScopeError::new("foo");
        assert_eq!(format!("{}", e), "query out of scope: foo");
    }
}
