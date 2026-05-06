//! Regression guard for the Phase 19.1-01 bench wall_clock measurement fix.
//!
//! See `.planning/phases/19.1-realistic-bench-rebaseline/19.1-01-PLAN.md` and
//! memory `project_phase19_bench_wallclock_fix.md` for context. The bug under-
//! reported EPS by ~5x for any N where bench-time < 1 s because
//! `let elapsed = start.elapsed();` was captured AFTER awaiting `get_task`
//! (1 s sleep loop) and `rss_task` (500 ms sleep loop). Phase 19 verdict was
//! PASS-WITH-DEFICIT only because of this bug; the real number clears the M4
//! threshold by 2.5x.
//!
//! These three source-text checks pin the structural fix:
//! 1. `let elapsed = start.elapsed();` appears BEFORE both background-task
//!    awaits (in source-byte-offset terms).
//! 2. `get_task` definition uses `tokio::select!` (not raw `loop { sleep }`).
//! 3. `rss_task` definition uses `tokio::select!`.
//!
//! Failing the test means the bench is mis-measuring wall_clock_ms again.

// Plan 13.7.6-32 migrated v18's production harness from
// `src/bin/beava-bench-v18.rs` (deleted) to `src/harness/production.rs`. The
// structural pins below now scan the harness module; the contract — `let
// elapsed = start.elapsed();` precedes the `get_task` / `rss_task` awaits and
// both background tasks use `tokio::select!` — is preserved across the
// migration.
const BENCH_V18_SOURCE: &str = include_str!("../src/harness/production.rs");

/// Find the byte offset of the (last occurrence of the) given marker. Used to
/// pin source-text ordering for the wall_clock capture relative to
/// background-task awaits.
fn find_offset(source: &str, marker: &str) -> Option<usize> {
    source.rfind(marker)
}

/// The wall_clock capture (`let elapsed = start.elapsed();`) MUST appear before
/// the lines that await `get_task` and `rss_task`. If `elapsed` is captured
/// after those awaits, the inner sleep loops (1000 ms get_task, 500 ms
/// rss_task) inflate `wall_clock_ms` for any N where the genuine bench time
/// is shorter than the sleep interval.
#[test]
fn test_elapsed_captured_before_background_task_awaits() {
    let source = BENCH_V18_SOURCE;

    // The capture site we care about. There is exactly one such line in the
    // worker-driver function; if there is more than one, the bench has been
    // restructured and this test needs an update.
    let elapsed_marker = "let elapsed = start.elapsed();";
    let elapsed_offset = find_offset(source, elapsed_marker).unwrap_or_else(|| {
        panic!("could not find `{elapsed_marker}` in harness/production.rs — bench may have been restructured")
    });

    let get_await_marker = "let _ = get_task.await;";
    let get_await_offset = find_offset(source, get_await_marker).unwrap_or_else(|| {
        panic!(
            "could not find `{get_await_marker}` in harness/production.rs — bench may have been restructured"
        )
    });

    let rss_await_marker = "let _ = rss_task.await;";
    let rss_await_offset = find_offset(source, rss_await_marker).unwrap_or_else(|| {
        panic!(
            "could not find `{rss_await_marker}` in harness/production.rs — bench may have been restructured"
        )
    });

    assert!(
        elapsed_offset < get_await_offset,
        "regression: `let elapsed = start.elapsed();` (offset {elapsed_offset}) is captured AFTER `let _ = get_task.await;` (offset {get_await_offset}). \
         The 1 s get_task sleep contaminates wall_clock_ms for N < 500 k events. \
         Move the elapsed capture before the background-task awaits. \
         See .planning/phases/19.1-realistic-bench-rebaseline/19.1-01-PLAN.md."
    );

    assert!(
        elapsed_offset < rss_await_offset,
        "regression: `let elapsed = start.elapsed();` (offset {elapsed_offset}) is captured AFTER `let _ = rss_task.await;` (offset {rss_await_offset}). \
         The 500 ms rss_task sleep contaminates wall_clock_ms for N < 500 k events. \
         Move the elapsed capture before the background-task awaits. \
         See .planning/phases/19.1-realistic-bench-rebaseline/19.1-01-PLAN.md."
    );
}

/// Locate the `let get_task = tokio::spawn(async move {` block and check its
/// body for `tokio::select!`. Without `tokio::select!`, the task can only
/// notice `stop` after a full sleep cycle (`get_interval_ms`, default 1000),
/// which is the proximate source of the wall_clock contamination.
#[test]
fn test_get_task_uses_tokio_select() {
    let source = BENCH_V18_SOURCE;
    let task_start = source
        .find("let get_task = tokio::spawn(async move {")
        .expect("could not find `let get_task = tokio::spawn(async move {` — bench restructured?");

    // Bound the search at the next `let ` declaration that opens a new
    // top-level slot in `run_workload` so we don't accidentally pick up a
    // `tokio::select!` from an unrelated worker task.
    let after = &source[task_start..];
    let task_end = after
        .find("// Spawn N parallel push workers")
        .or_else(|| after.find("let mut workers ="))
        .unwrap_or(after.len());
    let task_body = &after[..task_end];

    assert!(
        task_body.contains("tokio::select!"),
        "regression: `get_task` body does not use `tokio::select!`. \
         A raw `loop {{ sleep(...) }}` makes the task linger up to `get_interval_ms` (default 1000) \
         after `stop` is set, which inflates wall_clock_ms for short benches. \
         Convert to `tokio::select! {{ _ = stop_rx.recv() => break, _ = sleep(...) => {{ ... }} }}`. \
         See .planning/phases/19.1-realistic-bench-rebaseline/19.1-01-PLAN.md."
    );
}

/// Same contract as `test_get_task_uses_tokio_select` for the RSS sampler:
/// without `tokio::select!`, the rss_task sleeps up to 500 ms past the stop
/// signal, contaminating wall_clock_ms.
#[test]
fn test_rss_task_uses_tokio_select() {
    let source = BENCH_V18_SOURCE;
    let task_start = source
        .find("let rss_task = tokio::spawn(async move {")
        .expect("could not find `let rss_task = tokio::spawn(async move {` — bench restructured?");

    // Bound at the next major declaration so we don't peek into the next task.
    let after = &source[task_start..];
    let task_end = after
        .find("// Batch-get latency sampler")
        .or_else(|| after.find("let stop_get ="))
        .or_else(|| after.find("let get_task ="))
        .unwrap_or(after.len());
    let task_body = &after[..task_end];

    assert!(
        task_body.contains("tokio::select!"),
        "regression: `rss_task` body does not use `tokio::select!`. \
         A raw `loop {{ sleep(500) }}` makes the task linger up to 500 ms after `stop` is set, \
         which inflates wall_clock_ms for short benches. \
         Convert to `tokio::select! {{ _ = stop_rx.recv() => break, _ = sleep(500) => {{ ... }} }}`. \
         See .planning/phases/19.1-realistic-bench-rebaseline/19.1-01-PLAN.md."
    );
}
