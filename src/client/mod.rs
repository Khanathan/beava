//! Client-side engine embedding. Phase 28 ships the type surface;
//! Phase 29 wires the session manager + log consumer.
//!
//! This module is UNCONDITIONAL (not feature-gated) so both server and
//! client builds compile it. Keeping it unconditional is cheap (no
//! server-only imports live here) and lets server-side tests reference
//! client types if useful.
//!
//! Phase 29 will replace the stub `remote` / `streams` / `keys` /
//! `key_prefix` fields with Phase 27's real `Scope` struct. Today these
//! are plain `String`/`Vec<String>` so that Phase 28 does NOT depend on
//! Phase 27 code landing first.

/// Session execution mode.
///
/// Phase 28 ships only `Historical` (bounded replay from S3 log).
/// `Streaming` is added in Phase 31.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionMode {
    Historical,
    // Streaming, // Phase 31 — tails the live log after historical catch-up.
}

/// Client-side session handle.
///
/// Stub for Phase 28: fields describe the scope-shaped arguments the
/// CLI parses (`--remote`, `--streams`, `--keys`, `--key-prefix`,
/// `--token`). Phase 29 swaps the stringly-typed scope fields for the
/// real `tally::server::replica::Scope` struct (still server-gated in
/// Phase 27) and adds lifecycle methods (`connect`, `bootstrap`, `run`).
///
/// Intentionally NO:
/// - async / tokio / axum types
/// - lifecycle methods
/// - reference to `tally::server::*`
#[derive(Debug, Clone)]
pub struct Session {
    /// Remote tally server address, e.g. `"tally.example.com:6400"`.
    pub remote: String,
    /// Stream names the session subscribes to.
    pub streams: Vec<String>,
    /// Optional explicit key set. `None` = all keys in scope.
    pub keys: Option<Vec<String>>,
    /// Optional key prefix scoping. Mutually exclusive with `keys` at
    /// the validation layer (Phase 29); not enforced in the stub.
    pub key_prefix: Option<String>,
    /// Replay mode. Phase 28 only supports `Historical`.
    pub mode: SessionMode,
    /// Optional auth token for protected replicas.
    pub token: Option<String>,
}

impl Session {
    /// Construct a `Session` with defaults: `Historical` mode, no
    /// keys/prefix/token.
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

/// Raised by Phase 29's scope validator when a query touches a
/// stream/key outside the session's scope. Defined here in Phase 28
/// so Phase 29 can import `tally::client::OutOfScopeError` without a
/// circular-crate dance.
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
        assert!(s.keys.is_none());
        assert!(s.key_prefix.is_none());
        assert!(s.token.is_none());
    }

    #[test]
    fn out_of_scope_display() {
        let e = OutOfScopeError::new("foo");
        assert_eq!(format!("{}", e), "query out of scope: foo");
    }
}
