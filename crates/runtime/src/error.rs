use thiserror::Error;

/// Errors produced by the process runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("process error: {0}")]
    Io(#[from] std::io::Error),

    #[error("process `{program}` could not be started: {message}")]
    Spawn { program: String, message: String },

    #[error("process is not running: {0}")]
    NotRunning(#[from] tokio::sync::mpsc::error::SendError<()>),
}
