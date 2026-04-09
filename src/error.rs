/// Single error enum for all Tally error domains.
/// Variants: Parse, Type, Window, Expression, Protocol per CONTEXT.md.
#[derive(Debug, thiserror::Error)]
pub enum TallyError {
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
}
