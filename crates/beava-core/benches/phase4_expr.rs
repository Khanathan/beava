// Phase 4 expr/eval/op-chain bench — RED state (plan 05.5-03 task 1.a).
//
// Task 1.b fills in the criterion groups and the phase4_expr_benches
// constant module. Until that module exists, this file fails to compile —
// producing the RED signal required by TDD.

// RED: `phase4_expr_benches` does not yet exist; this use fails to compile.
use phase4_expr_benches::EXPECTED_GROUPS as _;

fn main() {
    // Contract: 3 parse + 5 eval + 2 op-chain = 10 bench groups.
    // This assertion is statically verified at compile time via the const.
    let _ = EXPECTED_GROUPS;
}
