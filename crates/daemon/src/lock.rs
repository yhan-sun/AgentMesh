//! OS advisory file lock ensuring a single daemon per scope.

use std::fs::{File, OpenOptions};
use std::path::Path;

use fs2::FileExt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LockError {
    #[error("failed to open lock file `{path}`: {source}")]
    Open {
        path: String,
        source: std::io::Error,
    },

    #[error("daemon lock is held by another process (path: {path})")]
    Held { path: String },
}

/// Held OS file lock for a daemon scope. Releasing the lock is dropping
/// this guard; the lock file itself may remain on disk.
pub struct ScopeLock {
    _file: File,
}

impl ScopeLock {
    /// Try to acquire an exclusive lock; fails when another process holds it.
    pub fn acquire(path: &Path) -> Result<Self, LockError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| LockError::Open {
                path: parent.display().to_string(),
                source,
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| LockError::Open {
                path: path.display().to_string(),
                source,
            })?;
        file.try_lock_exclusive().map_err(|_| LockError::Held {
            path: path.display().to_string(),
        })?;
        Ok(Self { _file: file })
    }
}
