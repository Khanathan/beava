/// Single error enum for all Beava error domains.
/// Variants: Parse, Type, Window, Expression, Protocol per CONTEXT.md.
#[derive(Debug, thiserror::Error)]
pub enum BeavaError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("type error: expected {expected}, got {got} for field '{field}'")]
    Type {
        field: String,
        expected: String,
        got: String,
    },

    #[error("window error: {0}")]
    Window(String),

    #[error("expression error: {0}")]
    Expression(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    /// Phase 25-01: Reserved opcode / unimplemented feature signaled to the
    /// client. Maps to `STATUS_ERROR` at the handler boundary (connection
    /// stays open; NOT torn down). Distinct from `Protocol` so call sites
    /// and tests can disambiguate "reserved for future version" from
    /// "malformed frame".
    #[error("not implemented in v0: {0}")]
    NotImplemented(String),

    /// Phase 50-06 (D-10, TPC-CORR-03): One or more tuple shard_key fields were
    /// absent from the event payload. Rejected BEFORE routing — shard threads
    /// never see malformed events. HTTP: 400; TCP: SHARD_KEY_MISSING (0x12).
    #[error("shard_key field(s) missing from event payload: {missing:?}")]
    ShardKeyMissing { missing: Vec<String> },
}
