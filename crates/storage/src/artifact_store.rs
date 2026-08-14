//! Local file storage for artifacts too large for inline SQLite.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::StorageError;

/// Default artifact file root: `<user-data>/artifacts/<task-id>/`.
pub fn default_artifact_root() -> PathBuf {
    crate::database::user_data_dir().join("artifacts")
}

/// Minimal local artifact file store.
///
/// Artifacts are stored under `<root>/<task-id>/<artifact-id>.bin`. File
/// names derive from ids only — never from untrusted artifact names.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn with_default_root() -> Self {
        Self::new(default_artifact_root())
    }

    /// Write artifact content to disk; returns the stored path.
    pub fn store(
        &self,
        task_id: Uuid,
        artifact_id: Uuid,
        content: &[u8],
    ) -> Result<PathBuf, StorageError> {
        let dir = self.root.join(task_id.to_string());
        std::fs::create_dir_all(&dir).map_err(|source| StorageError::CreateArtifactDir {
            path: dir.display().to_string(),
            source,
        })?;
        let path = dir.join(format!("{artifact_id}.bin"));
        std::fs::write(&path, content).map_err(|source| StorageError::WriteArtifactFile {
            path: path.display().to_string(),
            source,
        })?;
        Ok(path)
    }

    /// Read artifact content back from disk.
    pub fn load(&self, path: &Path) -> Result<Vec<u8>, StorageError> {
        std::fs::read(path).map_err(|source| StorageError::WriteArtifactFile {
            path: path.display().to_string(),
            source,
        })
    }

    /// Delete an artifact file from disk (Phase 14 artifact pruning).
    pub fn delete(&self, path: &Path) -> Result<(), StorageError> {
        std::fs::remove_file(path).map_err(|source| StorageError::DeleteArtifactFile {
            path: path.display().to_string(),
            source,
        })
    }
}
