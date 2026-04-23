//! beava-core: shared library for Beava v2.
//!
//! This crate will grow over phases 2–10:
//! - Phase 2: operator trait, feature registry, where-filter DSL
//! - Phase 3: core aggregate primitives + apply loop
//! - Phase 4: WAL record format
//! - Phase 5: snapshot/recovery
//! - Phases 6–8: primitive catalogue
//!
//! Phase 1 ships a placeholder so the workspace compiles.

/// Compile-time crate version exposed for banner / diagnostics.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_non_empty() {
        assert!(!VERSION.is_empty());
    }
}
