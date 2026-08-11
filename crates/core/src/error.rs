use thiserror::Error;

/// Errors produced by the AgentMesh core domain layer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoreError {
    #[error("agent `{0}` not found")]
    AgentNotFound(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),
}
