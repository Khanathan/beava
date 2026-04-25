//! Smoke tests for beava-runtime-core (Phase 18 Plan 01).
//!
//! These tests follow strict TDD red-green order per CLAUDE.md §Conventions.
//! Each block is annotated with which task's RED it was first written for.

// ─── Task 1.1 RED: EventLoop::new() constructs ────────────────────────────────

#[test]
fn event_loop_new_constructs_and_returns() {
    let el = beava_runtime_core::EventLoop::new().expect("EventLoop::new()");
    // Just verify it constructed — minimal assertion.
    drop(el);
}
