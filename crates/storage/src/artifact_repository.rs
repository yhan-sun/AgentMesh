//! Artifact persistence: SQL rows in, domain objects out.

use std::path::PathBuf;

use agentmesh_core::{Artifact, ArtifactKind};
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::artifact_store::ArtifactStore;
use crate::database::Database;
use crate::error::StorageError;

/// Maximum size of artifact content stored inline in SQLite (256 KiB).
///
/// Larger artifacts must be persisted to disk (`Artifact.path`) and stored
/// by reference only.
pub const MAX_INLINE_CONTENT: usize = 256 * 1024;

/// Persists [`Artifact`]s produced by tasks.
///
/// Content up to [`MAX_INLINE_CONTENT`] is stored inline in SQLite; larger
/// content is written to the [`ArtifactStore`] and the database row keeps
/// only its path.
#[derive(Clone)]
pub struct ArtifactRepository {
    database: Database,
    store: ArtifactStore,
}

impl ArtifactRepository {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            store: ArtifactStore::with_default_root(),
        }
    }

    pub fn with_store(database: Database, store: ArtifactStore) -> Self {
        Self { database, store }
    }

    /// Insert an artifact owned by `task_id`.
    ///
    /// Content larger than [`MAX_INLINE_CONTENT`] is written to the
    /// [`ArtifactStore`]; the row keeps only its path.
    pub async fn insert(&self, task_id: Uuid, artifact: &Artifact) -> Result<(), StorageError> {
        let (content, stored_path) = if artifact.content.len() > MAX_INLINE_CONTENT {
            let stored = self.store.store(task_id, artifact.id, &artifact.content)?;
            (None, Some(stored))
        } else {
            let inline =
                std::str::from_utf8(&artifact.content).map_err(StorageError::InvalidContent)?;
            (Some(inline), None)
        };
        let metadata = serde_json::to_string(&artifact.metadata).map_err(|source| {
            StorageError::InvalidMetadata {
                artifact_id: artifact.id.to_string(),
                source,
            }
        })?;

        sqlx::query(
            "INSERT INTO artifacts (id, task_id, kind, name, mime_type, path, content, metadata, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(artifact.id.to_string())
        .bind(task_id.to_string())
        .bind(artifact_kind_str(artifact.kind))
        .bind(&artifact.name)
        .bind(&artifact.mime_type)
        .bind(stored_path.as_ref().map(|p| p.display().to_string()))
        .bind(content)
        .bind(metadata)
        .bind(Utc::now().to_rfc3339())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::InsertArtifact {
            artifact_id: artifact.id.to_string(),
            task_id: task_id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load all artifacts of a task.
    pub async fn list_by_task(&self, task_id: Uuid) -> Result<Vec<Artifact>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, kind, name, mime_type, path, content, metadata
             FROM artifacts WHERE task_id = ? ORDER BY created_at",
        )
        .bind(task_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadArtifacts {
            task_id: task_id.to_string(),
            source,
        })?;

        rows.iter()
            .map(|row| {
                let artifact_id: String = row.get("id");
                let metadata: String = row.get("metadata");
                let metadata = serde_json::from_str(&metadata).map_err(|source| {
                    StorageError::InvalidMetadata {
                        artifact_id: artifact_id.clone(),
                        source,
                    }
                })?;
                Ok(Artifact {
                    id: Uuid::parse_str(&artifact_id).map_err(|err| {
                        StorageError::LoadArtifacts {
                            task_id: task_id.to_string(),
                            source: sqlx::Error::Decode(err.to_string().into()),
                        }
                    })?,
                    name: row.get("name"),
                    kind: artifact_kind_from_str(&row.get::<String, _>("kind")).ok_or_else(
                        || StorageError::LoadArtifacts {
                            task_id: task_id.to_string(),
                            source: sqlx::Error::Decode("unknown artifact kind".into()),
                        },
                    )?,
                    mime_type: row.get("mime_type"),
                    path: row
                        .get::<Option<String>, _>("path")
                        .map(std::path::PathBuf::from),
                    content: row.get::<String, _>("content").into_bytes(),
                    metadata,
                })
            })
            .collect()
    }

    /// Prune file-backed artifacts of terminal tasks older than `older_than`
    /// (Phase 14 `agentmesh artifacts prune`).
    ///
    /// Only *file-backed* artifacts (content stored on disk, `path` set) are
    /// ever removed. The SQLite metadata row is kept — its `path` is cleared —
    /// so task / workflow / apply history stays queryable. Artifacts whose
    /// task is part of a running or interrupted workflow, or whose session
    /// still has an active/applied/archived workspace, are never touched.
    ///
    /// With `check = true` nothing is deleted; only the candidate count is
    /// returned.
    pub async fn prune_files(
        &self,
        older_than: &DateTime<Utc>,
        check: bool,
    ) -> Result<PruneResult, StorageError> {
        let older = older_than.to_rfc3339();
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT a.id, a.path FROM artifacts a
             JOIN tasks t ON t.id = a.task_id
             WHERE a.path IS NOT NULL
               AND a.created_at < ?
               AND t.status IN ('completed', 'failed', 'cancelled')
               AND NOT EXISTS (
                   SELECT 1 FROM workflow_steps s
                   JOIN workflows w ON w.id = s.workflow_id
                   WHERE s.task_id = a.task_id AND w.status IN ('running', 'interrupted')
               )
               AND NOT EXISTS (
                   SELECT 1 FROM workspaces ws
                   WHERE ws.agent_session_id = t.agent_session_id
                     AND ws.state IN ('active', 'applied', 'archived')
               )",
        )
        .bind(&older)
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::PruneArtifacts)?;

        let mut pruned = 0usize;
        if !check {
            for (id, path) in &rows {
                let path_buf = PathBuf::from(path);
                match self.store.delete(&path_buf) {
                    Ok(()) => {}
                    // The file may already be gone; still clear the row path.
                    Err(StorageError::DeleteArtifactFile { .. }) if !path_buf.exists() => {}
                    Err(err) => return Err(err),
                }
                sqlx::query("UPDATE artifacts SET path = NULL WHERE id = ?")
                    .bind(id)
                    .execute(self.database.pool())
                    .await
                    .map_err(StorageError::PruneArtifacts)?;
                pruned += 1;
            }
        }
        Ok(PruneResult {
            candidates: rows.len(),
            pruned,
        })
    }
}

/// Result of an artifact prune run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneResult {
    /// Number of file-backed artifacts that qualified for pruning.
    pub candidates: usize,
    /// Number actually pruned (0 for a `--check` preview).
    pub pruned: usize,
}

fn artifact_kind_str(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Text => "text",
        ArtifactKind::File => "file",
        ArtifactKind::Patch => "patch",
        ArtifactKind::Json => "json",
        ArtifactKind::Log => "log",
        ArtifactKind::TestResult => "test_result",
    }
}

fn artifact_kind_from_str(value: &str) -> Option<ArtifactKind> {
    match value {
        "text" => Some(ArtifactKind::Text),
        "file" => Some(ArtifactKind::File),
        "patch" => Some(ArtifactKind::Patch),
        "json" => Some(ArtifactKind::Json),
        "log" => Some(ArtifactKind::Log),
        "test_result" => Some(ArtifactKind::TestResult),
        _ => None,
    }
}
