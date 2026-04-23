//! Centralized default values for devex-first zero-config registration.
//!
//! These values materialize at runtime (push handlers, query handlers) when
//! the descriptor field is None. The descriptor preserves the user's literal
//! choice — None means "use the default below"; Some(N) means "user chose N".

/// Default tolerance for delayed event delivery. If `tolerate_delay_ms` is None
/// on an EventDescriptor, the server treats the effective value as this constant.
pub const DEFAULT_TOLERATE_DELAY_MS: u64 = 5_000; // 5 seconds

/// Default retention horizon for raw events. If `keep_events_for_ms` is None,
/// events are kept for this duration before eviction.
pub const DEFAULT_KEEP_EVENTS_FOR_MS: u64 = 604_800_000; // 7 days

/// Default dedupe window when `dedupe_key` is set but `dedupe_window_ms` is None.
pub const DEFAULT_DEDUPE_WINDOW_MS: u64 = 86_400_000; // 24 hours
