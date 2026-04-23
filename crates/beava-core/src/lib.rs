//! beava-core: shared library for Beava v2.
//!
//! This crate will grow over phases 2–10:
//! - Phase 2: schema + registry (this phase)
//! - Phase 3: Python SDK integration
//! - Phase 4: expression evaluation + stateless op execution
//! - Phase 5: aggregation operators + apply loop
//! - Phase 6: WAL persistence
//! - Phase 7: snapshot/recovery
//! - Phases 8–10: advanced operators + infra

pub mod agg_descriptor;
pub mod agg_op;
pub mod agg_schema;
pub mod agg_state;
pub mod agg_where;
pub mod agg_windowed;
pub mod config;
pub mod defaults;
pub mod eval;
pub mod expr;
pub mod expr_builtins;
pub mod op_chain;
pub mod op_node;
pub mod register_validate;
pub mod registry;
pub mod registry_diff;
pub mod row;
pub mod schema;
pub mod schema_propagate;
pub mod wire;

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
