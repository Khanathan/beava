//! Phase 5.5 Plan 06 — baseline coverage contract (RED gate).
//!
//! Asserts that `.planning/perf-baselines.md` contains a populated hw-class
//! section with all 28 expected bench IDs.  Fails before plan 06 green lands
//! because the file still holds the Plan 01 scaffold placeholder.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/beava-server → ../../ = repo root
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .join("../..")
        .canonicalize()
        .expect("canonicalize repo root")
}

/// Detect the current hw-class string using the same recipe as
/// `scripts/capture-baselines.sh` (CONTEXT D-03).
fn current_hw_class() -> String {
    #[cfg(target_os = "macos")]
    {
        let cpu = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let ncpu = std::process::Command::new("sysctl")
            .args(["-n", "hw.ncpu"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let uname = std::process::Command::new("uname")
            .arg("-sr")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        format!(
            "{} / {} / {} cores",
            cpu.replace(' ', "-"),
            uname.replace(' ', "-"),
            ncpu
        )
    }
    #[cfg(target_os = "linux")]
    {
        let cpu = std::process::Command::new("sh")
            .args([
                "-c",
                "lscpu | awk -F: '/Model name/ {print $2}' | xargs | tr ' ' '-'",
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let uname = std::process::Command::new("uname")
            .arg("-sr")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let nproc = std::process::Command::new("nproc")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        format!("{} / {} / {} cores", cpu, uname.replace(' ', "-"), nproc)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        "unknown / unknown / unknown cores".to_string()
    }
}

/// All 28 expected bench IDs from plans 02–05.
const EXPECTED_BENCHES: &[&str] = &[
    // Plan 05.5-02: phase25_wire (6)
    "encode/register_small",
    "encode/register_medium",
    "encode/register_near_limit",
    "decode/register_small",
    "decode/register_medium",
    "decode/register_near_limit",
    // Plan 05.5-03: phase4_expr (10)
    "parse/small",
    "parse/medium",
    "parse/deep",
    "eval/arith",
    "eval/compare",
    "eval/boolean",
    "eval/nullcheck",
    "eval/cast",
    "op_chain/compile_4op",
    "op_chain/apply_4op",
    // Plan 05.5-04: phase5_agg (11)
    "agg_op/count",
    "agg_op/sum",
    "agg_op/avg",
    "agg_op/min",
    "agg_op/max",
    "agg_op/variance",
    "agg_op/stddev",
    "agg_op/ratio",
    "windowed/fold_count_5m_1Mevt",
    "windowed/fold_sum_5m_1Mevt",
    "apply/3agg_100ent_1Kevt",
    // Plan 05.5-05: pytest-benchmark (1)
    "test_register_compile_10_descriptors",
];

#[test]
fn baselines_populated_for_current_hw_class() {
    let root = repo_root();
    let baselines_path = root.join(".planning/perf-baselines.md");

    let contents = std::fs::read_to_string(&baselines_path)
        .unwrap_or_else(|e| panic!("read {baselines_path:?}: {e}"));

    let hw_class = current_hw_class();
    let section_header = format!("## hw-class: {hw_class}");

    // 1. File must contain a section for this hw-class.
    assert!(
        contents.contains(&section_header),
        "perf-baselines.md must contain a `{section_header}` section.\n\
         Run `./scripts/capture-baselines.sh` and paste the output into the file.\n\
         Detected hw-class: {hw_class}"
    );

    // 2. Count data rows (lines starting with `|` containing a time unit after
    //    the hw-class section heading).
    let after_section = contents.split(&section_header).nth(1).unwrap_or("");

    // Find the next section boundary (another `## `) to scope the count.
    let section_content = if let Some(next_section) = after_section.find("\n## ") {
        &after_section[..next_section]
    } else {
        after_section
    };

    let data_rows: Vec<&str> = section_content
        .lines()
        .filter(|line| {
            line.starts_with('|')
                && (line.contains(" ns")
                    || line.contains(" µs")
                    || line.contains(" ms")
                    || line.contains("ops/s")
                    || line.contains("µs/iter")
                    || line.contains("ns/iter")
                    || line.contains("ms/iter"))
        })
        .collect();

    assert!(
        data_rows.len() >= 28,
        "Expected ≥ 28 data rows in the `{section_header}` section, \
         found {}: {data_rows:?}",
        data_rows.len()
    );

    // 3. Each of the 28 expected bench IDs must appear in the section.
    let mut missing: Vec<&str> = Vec::new();
    for bench_id in EXPECTED_BENCHES {
        if !section_content.contains(bench_id) {
            missing.push(bench_id);
        }
    }

    assert!(
        missing.is_empty(),
        "The following bench IDs are missing from the `{section_header}` section:\n  {}\n\
         Run `./scripts/capture-baselines.sh` and paste the output into the baselines file.",
        missing.join("\n  ")
    );
}
