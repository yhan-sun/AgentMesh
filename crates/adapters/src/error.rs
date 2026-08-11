use thiserror::Error;

/// Errors produced by agent adapters.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("agent `{0}` not found")]
    NotFound(String),

    #[error("agent `{0}` is unavailable: {1}")]
    Unavailable(String, String),

    #[error("{0}")]
    CommandNotFound(String),

    #[error("failed to parse agent protocol output: {0}")]
    ProtocolParse(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("agent `{0}` failed: {1}")]
    Agent(String, String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
