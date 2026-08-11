use thiserror::Error;

/// Errors produced by the daemon crate.
#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("daemon is not running for this scope")]
    NotRunning,

    #[error("daemon lock is held but daemon is not responding")]
    LockHeldNotResponding,

    #[error("failed to start daemon process: {0}")]
    Spawn(String),

    #[error("daemon did not become healthy within {0}s")]
    StartupTimeout(u64),

    #[error("daemon protocol version mismatch; restart the AgentMesh daemon")]
    ProtocolMismatch,

    #[error("daemon authentication failed")]
    Unauthorized,

    #[error("daemon api error ({code}): {message}")]
    Api { code: String, message: String },

    #[error("http error: {0}")]
    Http(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl DaemonError {
    pub fn api(code: impl Into<String>, message: impl Into<String>) -> Self {
        DaemonError::Api {
            code: code.into(),
            message: message.into(),
        }
    }
}
