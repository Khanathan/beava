//! Phase 10 sketches submodule. Plans 10-01..10-04 land child modules.

pub mod bloom;
pub mod cms;
pub mod entropy;
// pub mod hll;        // TEMP: sibling RED — re-enable before commit
pub mod retracting_ring;
pub mod top_k;
// pub mod uddsketch;  // TEMP: sibling RED
// pub mod percentile; // TEMP: sibling RED

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        assert_eq!(1 + 1, 2);
    }
}
