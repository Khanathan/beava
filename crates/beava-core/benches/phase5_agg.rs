// Phase 5 aggregation hot-path bench — RED state (plan 05.5-04 task 1.a).

fn main() {}

#[cfg(test)]
mod tests {
    #[test]
    fn groups_registered() {
        assert_eq!(
            phase5_agg_benches::EXPECTED_GROUPS,
            11,
            "bench must register 8 AggOp update + 2 WindowedOp + 1 apply group"
        );
    }
}
