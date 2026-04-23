//! beava-server: the server binary's logic crate.
//!
//! Growth plan — see 01-CONTEXT.md:
//! - Plan 02 (this): `cli` module + CLI wiring; re-exports `beava_core::config::Config`
//! - Plan 03: `logging` module
//! - Plan 04: `http` module + `Server` type + graceful shutdown
//! - Plan 05: `testing::TestServer`

pub mod cli;
pub mod http;
pub mod logging;
pub mod register;
pub mod registry_debug;
pub mod server;
pub mod shutdown;

#[cfg(any(feature = "testing", test))]
pub mod testing;

pub use beava_core::config::{self, Config, ConfigError};
pub use server::{Server, ServerError};

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
