//! beava-server: the server binary's logic crate.
//!
//! Why a lib + bin split? Integration tests (Plan 05 onwards) import the public API
//! from this crate (`TestServer`, future `Server::run` etc.). The `main.rs` is a thin
//! wrapper that parses args and calls into the library.
//!
//! Growth plan:
//! - Plan 02: `config` module
//! - Plan 03: `logging` module
//! - Plan 04: `http` module + `Server` type + graceful shutdown
//! - Plan 05: `testing::TestServer` (feature-gated or cfg(test) as appropriate)

/// Semantic version of the server binary.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Human-readable banner used by `main.rs` and logs.
pub fn banner() -> String {
    format!("beava v{} (skeleton)", VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_includes_version() {
        let b = banner();
        assert!(b.contains(VERSION));
        assert!(b.starts_with("beava v"));
    }
}
