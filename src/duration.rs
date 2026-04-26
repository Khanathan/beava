//! Phase 28-01: shared duration parsing — feature-independent.
//!
//! Extracted from `crate::server::protocol` so the `engine` and `state`
//! modules can use it under `--no-default-features --features client`
//! (where `server` is gated out). The server's `protocol` module still
//! re-exports these names for backward compatibility.

use crate::error::TallyError;

/// Sentinel meaning "no eviction, ever". Returned by `parse_duration_str`
/// for the literal string "forever". Eviction schedulers MUST skip any
/// stream/table whose ttl equals this value. v0-restructure-spec §7.2.
pub const FOREVER_TTL: std::time::Duration = std::time::Duration::from_secs(u64::MAX / 2);

/// Is this a `forever` sentinel (per [`FOREVER_TTL`])?
pub fn is_forever_ttl(d: std::time::Duration) -> bool {
    d >= FOREVER_TTL
}

/// Parse a human-readable duration string into std::time::Duration.
/// Supported suffixes: ms (milliseconds), s (seconds), m (minutes), h (hours), d (days).
/// Phase 25-02: also accepts "forever" (→ [`FOREVER_TTL`]) and "0" (→ zero duration).
pub fn parse_duration_str(s: &str) -> Result<std::time::Duration, TallyError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TallyError::Protocol("empty duration string".into()));
    }
    // Phase 25-02 sentinels — locked by v0-restructure-spec §7.2.
    if s.eq_ignore_ascii_case("forever") {
        return Ok(FOREVER_TTL);
    }
    if s == "0" {
        return Ok(std::time::Duration::ZERO);
    }
    // Check for "ms" suffix first (two-character suffix)
    if let Some(num_str) = s.strip_suffix("ms") {
        let millis: u64 = num_str
            .parse()
            .map_err(|_| TallyError::Protocol(format!("invalid duration number: {}", s)))?;
        return Ok(std::time::Duration::from_millis(millis));
    }
    // Single-character suffix
    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b's') => (&s[..s.len() - 1], 1u64),
        Some(b'm') => (&s[..s.len() - 1], 60u64),
        Some(b'h') => (&s[..s.len() - 1], 3600u64),
        Some(b'd') => (&s[..s.len() - 1], 86400u64),
        _ => {
            return Err(TallyError::Protocol(format!(
                "unknown duration suffix: {}",
                s
            )));
        }
    };
    let value: u64 = num_str
        .parse()
        .map_err(|_| TallyError::Protocol(format!("invalid duration number: {}", s)))?;
    Ok(std::time::Duration::from_secs(value * multiplier))
}
