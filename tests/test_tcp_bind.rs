//! Phase 20 TRAC-05: unit tests for the pure `resolve_tcp_bind` helper that
//! decides the TCP listener bind address from CLI flag, env var, and default.
//!
//! Default is 127.0.0.1 — this is a security property (raw TCP exposes
//! PUSH/SET/MSET with no auth layer), NOT a convenience, so the default path
//! gets its own named test.

use beava::server::auth::resolve_tcp_bind;

#[test]
fn test_default_bind_is_loopback() {
    // No CLI, no env -> loopback default. This is the security-critical case.
    assert_eq!(resolve_tcp_bind(None, None, "6400"), "127.0.0.1:6400");
}

#[test]
fn test_explicit_bind_override() {
    // CLI wins.
    assert_eq!(
        resolve_tcp_bind(None, Some("0.0.0.0"), "6400"),
        "0.0.0.0:6400"
    );
    // Env is honored when no CLI.
    assert_eq!(
        resolve_tcp_bind(Some("10.0.0.5"), None, "6400"),
        "10.0.0.5:6400"
    );
    // CLI takes precedence over env.
    assert_eq!(
        resolve_tcp_bind(Some("10.0.0.5"), Some("192.168.1.1"), "7000"),
        "192.168.1.1:7000"
    );
    // Empty strings fall back to the loopback default.
    assert_eq!(resolve_tcp_bind(Some(""), None, "6400"), "127.0.0.1:6400");
    assert_eq!(
        resolve_tcp_bind(None, Some("   "), "6400"),
        "127.0.0.1:6400"
    );
}
