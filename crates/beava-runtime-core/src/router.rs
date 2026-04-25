//! Path dispatch for HTTP requests in the hand-rolled event loop (Phase 18).
//!
//! Simple match-based dispatch on path prefix. No regex, no macros —
//! direct pattern matching for zero overhead.

/// Recognized HTTP routes dispatched by the hand-rolled event loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// POST /push/:event_name — fire-and-forget push.
    Push { event_name: String },
    /// POST /push-sync/:event_name — synchronous push (awaits fsync).
    PushSync { event_name: String },
    /// POST /push-batch/:event_name — batched push.
    PushBatch { event_name: String },
    /// POST /get — batch feature read.
    Get,
    /// GET /get/:feature/:key — single feature read.
    GetSingle { feature: String, key: String },
    /// POST /upsert/:table — table upsert.
    Upsert { table: String },
    /// POST /delete/:table — table tombstone.
    Delete { table: String },
    /// POST /retract — retraction.
    Retract,
    /// POST /register — pipeline registration.
    Register,
    /// Path not in the table.
    NotFound,
    /// Path matched but wrong method.
    MethodNotAllowed,
}

/// Parse a path string into a `Route`.
///
/// Does NOT validate the HTTP method — that check is done at dispatch time
/// so the router can return `MethodNotAllowed` with the actual allowed methods.
pub struct Router;

impl Router {
    /// Dispatch `(method, path)` into a `Route`.
    pub fn route(method: &str, path: &str) -> Route {
        // Normalise: strip trailing slash for tolerance.
        let path = path.trim_end_matches('/');

        // /push/:event_name
        if let Some(rest) = path.strip_prefix("/push/") {
            return if method == "POST" {
                Route::Push {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /push-sync/:event_name
        if let Some(rest) = path.strip_prefix("/push-sync/") {
            return if method == "POST" {
                Route::PushSync {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /push-batch/:event_name
        if let Some(rest) = path.strip_prefix("/push-batch/") {
            return if method == "POST" {
                Route::PushBatch {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /get/:feature/:key — must check before /get (exact match)
        if let Some(rest) = path.strip_prefix("/get/") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() == 2 {
                return if method == "GET" {
                    Route::GetSingle {
                        feature: parts[0].to_owned(),
                        key: parts[1].to_owned(),
                    }
                } else {
                    Route::MethodNotAllowed
                };
            }
        }
        // /get (exact)
        if path == "/get" {
            return if method == "POST" {
                Route::Get
            } else {
                Route::MethodNotAllowed
            };
        }
        // /upsert/:table
        if let Some(rest) = path.strip_prefix("/upsert/") {
            return if method == "POST" {
                Route::Upsert {
                    table: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /delete/:table
        if let Some(rest) = path.strip_prefix("/delete/") {
            return if method == "POST" {
                Route::Delete {
                    table: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /retract
        if path == "/retract" {
            return if method == "POST" {
                Route::Retract
            } else {
                Route::MethodNotAllowed
            };
        }
        // /register
        if path == "/register" {
            return if method == "POST" {
                Route::Register
            } else {
                Route::MethodNotAllowed
            };
        }

        Route::NotFound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_push_post() {
        assert_eq!(
            Router::route("POST", "/push/Transaction"),
            Route::Push {
                event_name: "Transaction".to_owned()
            }
        );
    }

    #[test]
    fn route_push_wrong_method() {
        assert_eq!(Router::route("GET", "/push/Foo"), Route::MethodNotAllowed);
    }

    #[test]
    fn route_get_single() {
        assert_eq!(
            Router::route("GET", "/get/txn_count/user123"),
            Route::GetSingle {
                feature: "txn_count".to_owned(),
                key: "user123".to_owned(),
            }
        );
    }

    #[test]
    fn route_get_batch() {
        assert_eq!(Router::route("POST", "/get"), Route::Get);
    }

    #[test]
    fn route_not_found() {
        assert_eq!(Router::route("GET", "/unknown"), Route::NotFound);
    }

    #[test]
    fn route_trailing_slash_normalised() {
        assert_eq!(
            Router::route("POST", "/push/Txn/"),
            Route::Push {
                event_name: "Txn".to_owned()
            }
        );
    }
}
