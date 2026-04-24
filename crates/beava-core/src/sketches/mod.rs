//! Phase 10 sketches submodule. Plans 10-01..10-04 land child modules.

pub mod bloom;
pub mod cms;
pub mod entropy;
pub mod hll;
pub mod retracting_ring;
pub mod uddsketch;

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        assert_eq!(1 + 1, 2);
    }
}
