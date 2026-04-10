//! Static asset serving for the embedded Debug UI (DBUI-05).
//!
//! All files under `src/server/ui/` are compiled into the Tally binary via
//! `rust-embed` at build time. Runtime access is through `UiAssets::get(path)`
//! and the two axum handlers below, which are mounted by
//! `src/server/http.rs::build_router` at `/` and `/static/{*file}`.
//!
//! Path-traversal defense: `rust_embed::get()` only resolves strings that were
//! embedded at compile time, so `../` cannot escape the embed root. We also
//! reject any path containing `..`, absolute paths, or NUL bytes as an
//! explicit defense-in-depth measure (RESEARCH §Security Domain — T-10-01).
//!
//! Case sensitivity (RESEARCH §Pitfall 9): `UiAssets::get("INDEX.HTML")`
//! returns `None` on Linux CI even if macOS tolerates it. All embedded paths
//! and handler lookups MUST be lowercase.

use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

/// Compile-time embedded assets rooted at `src/server/ui/`.
///
/// With the `debug-embed` feature (enabled in `Cargo.toml` by Plan 10-01),
/// both debug and release builds read bytes from the embedded blob — there is
/// no filesystem fallback. Path lookups are case-sensitive and resolve only
/// strings that were embedded at build time.
#[derive(Embed)]
#[folder = "src/server/ui/"]
pub struct UiAssets;

/// Handler for `GET /` — returns `index.html` from the embedded UI.
///
/// Returns 404 (not 500) if `index.html` is missing from the embed root so
/// that Plan 10-04's HTML authoring cycle can run before a committed index
/// without breaking `cargo check`.
pub async fn ui_index() -> Response {
    serve_asset("index.html")
}

/// Handler for `GET /static/{*file}` — serves arbitrary embedded assets.
///
/// Examples of paths that reach this handler:
///   /static/app.css               -> src/server/ui/app.css
///   /static/app.js                -> src/server/ui/app.js
///   /static/vendor/htmx.min.js    -> src/server/ui/vendor/htmx.min.js
///   /static/vendor/d3.min.js      -> src/server/ui/vendor/d3.min.js
///   /static/vendor/dagre-d3.min.js -> src/server/ui/vendor/dagre-d3.min.js
pub async fn ui_static(Path(file): Path<String>) -> Response {
    // Defense-in-depth: reject any `..` segment, absolute paths, or NUL bytes.
    // `rust_embed` would already return None for paths that escape the embed
    // root, but we want to fail loudly AND avoid any pathological behavior
    // inside its internal path normalization (T-10-01 mitigation).
    //
    // Invariant: axum's `Path<String>` percent-decodes the captured segment
    // before this handler runs, so encoded traversal attempts like `..%2f`
    // arrive here as raw `../` and are caught by `file.contains("..")`. Do
    // NOT weaken this check to a startswith/pattern scan — the substring
    // check is intentional defense-in-depth on top of rust-embed's
    // compile-time scoping.
    if file.contains("..") || file.starts_with('/') || file.contains('\0') {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    serve_asset(&file)
}

/// Look up `path` in the embedded asset tree and build an HTTP response with
/// the correct Content-Type (via the `mime-guess` feature) and a conservative
/// `Cache-Control` header. Returns 404 "not found" for missing paths.
fn serve_asset(path: &str) -> Response {
    match UiAssets::get(path) {
        Some(content) => {
            // `mime-guess` feature gives us a string MIME from the file's
            // extension. rust_embed's `metadata.mimetype()` returns a borrow
            // tied to `content.metadata`, so we clone into a `String` before
            // `content.data.into_owned()` consumes the binding.
            let mime: String = content.metadata.mimetype().to_string();
            // `.expect()` is safe here: `mime_guess` emits ASCII-only MIME
            // strings, the status code is a const, and the body is an owned
            // Vec<u8> — the only way `Response::builder().body()` can fail is
            // if a header name/value is invalid, which cannot happen with the
            // inputs above. Phase 10 review IN-01.
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                // Conservative cache: embedded bytes only change on binary
                // rebuild, but browsers reload between local dev sessions.
                .header(header::CACHE_CONTROL, "public, max-age=300")
                .body(Body::from(content.data.into_owned()))
                .expect("response builder accepts ASCII MIME from mime_guess")
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}
