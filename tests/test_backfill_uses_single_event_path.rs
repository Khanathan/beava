//! CORR-05 (2d.i closure): assert run_backfill uses the single-event path.
//!
//! Passes without any code change — this is the "not-a-bug" verification.
//! If a future refactor routes run_backfill through handle_push_batch, this
//! test flips RED and forces the audit conversation.

use std::fs;

const TCP_RS: &str = "src/server/tcp.rs";
const FN_MARKER: &str = "fn run_backfill("; // matches `pub async fn run_backfill(` too.

fn extract_fn_body(src: &str, marker: &str) -> String {
    let idx = src
        .find(marker)
        .unwrap_or_else(|| panic!("marker {marker:?} not found in {TCP_RS}"));
    // Walk forward to the first `{` after the fn header, then balance braces.
    let after_header = &src[idx..];
    let brace_rel = after_header.find('{').expect("no { after fn header");
    let body_start = idx + brace_rel + 1;
    let bytes = src.as_bytes();
    let mut depth = 1i32;
    let mut end = body_start;
    while end < bytes.len() && depth > 0 {
        match bytes[end] {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            _ => {}
        }
        end += 1;
    }
    assert!(depth == 0, "unbalanced braces in {TCP_RS} run_backfill");
    src[body_start..end - 1].to_string()
}

#[test]
fn run_backfill_uses_push_for_backfill_not_handle_push_batch() {
    let src = fs::read_to_string(TCP_RS).expect("read src/server/tcp.rs");
    let body = extract_fn_body(&src, FN_MARKER);

    assert!(
        body.contains("push_for_backfill("),
        "CORR-05: run_backfill must call push_for_backfill (single-event path); \
         body snippet (first 200 chars): {:.200}",
        &body
    );
    let batch_matches = body.matches("push_batch_with_cascade_no_features(").count();
    let handle_push_batch_matches = body.matches("handle_push_batch(").count();
    assert_eq!(
        batch_matches, 0,
        "CORR-05: run_backfill must NOT call push_batch_with_cascade_no_features \
         (2a batch path); {batch_matches} matches found"
    );
    assert_eq!(
        handle_push_batch_matches, 0,
        "CORR-05: run_backfill must NOT call handle_push_batch; \
         {handle_push_batch_matches} matches found"
    );
}
