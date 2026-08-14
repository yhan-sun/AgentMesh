//! Apply persistence: one row per ApplyManager run (Phase 13).
//!
//! A successful apply of a workspace's result is recorded here so a repeated
//! `agentmesh apply ... --yes` is rejected as `AlreadyApplied` while `--check`
//! stays available. Statuses are stored as stable snake_case strings
//! ([`ApplyStatus::as_str`]).

use std::path::PathBuf;

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Lifecycle status of a persisted apply run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStatus {
    /// Preflight completed, not yet executed (used for preview/plan records).
    Planned,
    /// Execution is in progress.
    Applying,
    /// The workspace result was written to the source repository.
    Completed,
    /// Execution (or rollback) failed; the error is stored.
    Failed,
}

impl ApplyStatus {
    /// Stable snake_case string used when persisting.
    pub fn as_str(&self) -> &'static str {
        match self {
            ApplyStatus::Planned => "planned",
            ApplyStatus::Applying => "applying",
            ApplyStatus::Completed => "completed",
            ApplyStatus::Failed => "failed",
        }
    }

    /// Parse a stable [`Self::as_str`] value; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "planned" => ApplyStatus::Planned,
            "applying" => ApplyStatus::Applying,
            "completed" => ApplyStatus::Completed,
            "failed" => ApplyStatus::Failed,
            _ => return None,
        })
    }
}

/// SQL row shape of the `applies` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRow {
    pub id: Uuid,
    /// Either the source task or the source workflow; exactly one is set.
    pub task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    /// The workspace whose result is being applied.
    pub workspace_id: Uuid,
    pub source_repository: PathBuf,
    pub base_revision: String,
    pub status: ApplyStatus,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    /// SHA-256 fingerprint of the workspace content at apply time (Phase 14),
    /// used by cleanup to detect changes made after the apply.
    pub workspace_snapshot_hash: Option<String>,
}

/// Outcome of an atomic [`ApplyRepository::claim_workspace`] insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimResult {
    /// The apply row was inserted and owns the workspace.
    Claimed,
    /// A completed apply already exists for the workspace.
    AlreadyCompleted,
    /// Another apply is currently in progress for the workspace.
    InProgress,
}

/// Persists [`ApplyRow`]s.
#[derive(Clone)]
pub struct ApplyRepository {
    database: Database,
}

impl ApplyRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a row (must not already exist).
    pub async fn create(&self, row: &ApplyRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO applies
                (id, task_id, workflow_id, workspace_id, source_repository, base_revision, status, error, created_at, completed_at, workspace_snapshot_hash)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.task_id.map(|id| id.to_string()))
        .bind(row.workflow_id.map(|id| id.to_string()))
        .bind(row.workspace_id.to_string())
        .bind(row.source_repository.display().to_string())
        .bind(&row.base_revision)
        .bind(row.status.as_str())
        .bind(&row.error)
        .bind(&row.created_at)
        .bind(&row.completed_at)
        .bind(&row.workspace_snapshot_hash)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreateApply {
            apply_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Atomically claim a workspace for an apply (Phase 14 P0).
    ///
    /// The partial UNIQUE index `(workspace_id) WHERE status IN
    /// ('applying','completed')` guarantees at most one active/completed apply
    /// per workspace at the database layer. A concurrent insert fails the
    /// UNIQUE constraint and is reported as `ClaimResult::InProgress` or
    /// `ClaimResult::AlreadyCompleted` — never silently allowed.
    pub async fn claim_workspace(&self, row: &ApplyRow) -> Result<ClaimResult, StorageError> {
        let insert = sqlx::query(
            "INSERT INTO applies
                (id, task_id, workflow_id, workspace_id, source_repository, base_revision, status, error, created_at, completed_at, workspace_snapshot_hash)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.task_id.map(|id| id.to_string()))
        .bind(row.workflow_id.map(|id| id.to_string()))
        .bind(row.workspace_id.to_string())
        .bind(row.source_repository.display().to_string())
        .bind(&row.base_revision)
        .bind(row.status.as_str())
        .bind(&row.error)
        .bind(&row.created_at)
        .bind(&row.completed_at)
        .bind(&row.workspace_snapshot_hash)
        .execute(self.database.pool())
        .await;
        match insert {
            Ok(_) => Ok(ClaimResult::Claimed),
            Err(source)
                if source
                    .as_database_error()
                    .map(|err| err.is_unique_violation())
                    .unwrap_or(false) =>
            {
                // Another applying/completed apply exists for this workspace;
                // distinguish the two states.
                let existing = sqlx::query_as::<_, (String,)>(
                    "SELECT status FROM applies
                     WHERE workspace_id = ? AND status IN ('applying','completed')
                     ORDER BY created_at DESC LIMIT 1",
                )
                .bind(row.workspace_id.to_string())
                .fetch_optional(self.database.pool())
                .await
                .map_err(|source| StorageError::LoadApply {
                    apply_id: row.workspace_id.to_string(),
                    source,
                })?;
                match existing {
                    Some((status,)) if status == ApplyStatus::Completed.as_str() => {
                        Ok(ClaimResult::AlreadyCompleted)
                    }
                    Some(_) => Ok(ClaimResult::InProgress),
                    None => Err(StorageError::CreateApply {
                        apply_id: row.id.to_string(),
                        source,
                    }),
                }
            }
            Err(source) => Err(StorageError::CreateApply {
                apply_id: row.id.to_string(),
                source,
            }),
        }
    }

    /// Load an apply by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<ApplyRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM applies WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadApply {
                apply_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_apply(&row)).transpose()
    }

    /// All applies, newest first.
    pub async fn list(&self) -> Result<Vec<ApplyRow>, StorageError> {
        let rows = sqlx::query("SELECT * FROM applies ORDER BY created_at DESC")
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListApplies)?;
        rows.iter().map(row_to_apply).collect()
    }

    /// All applies, newest first, with an optional status filter and limit.
    pub async fn list_with_filter(
        &self,
        limit: usize,
        status: Option<ApplyStatus>,
    ) -> Result<Vec<ApplyRow>, StorageError> {
        let limit = limit.clamp(1, 200);
        let mut query = String::from("SELECT * FROM applies WHERE 1 = 1");
        if status.is_some() {
            query.push_str(" AND status = ?");
        }
        query.push_str(" ORDER BY created_at DESC LIMIT ?");
        let mut q = sqlx::query(&query);
        if let Some(status) = status {
            q = q.bind(status.as_str());
        }
        let rows = q
            .bind(limit as i64)
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListApplies)?;
        rows.iter().map(row_to_apply).collect()
    }

    /// All applies for a specific workflow, oldest first.
    pub async fn list_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<ApplyRow>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM applies WHERE workflow_id = ? ORDER BY created_at ASC")
                .bind(workflow_id.to_string())
                .fetch_all(self.database.pool())
                .await
                .map_err(StorageError::ListApplies)?;
        rows.iter().map(row_to_apply).collect()
    }

    /// The snapshot hash of the most recent *completed* apply of a workspace.
    ///
    /// Cleanup recomputes the current workspace fingerprint and compares it
    /// against this value; a mismatch means the workspace changed after it was
    /// applied (`WorkspaceChangedAfterApply`) and must not be removed.
    pub async fn latest_snapshot_hash(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<String>, StorageError> {
        let row = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT workspace_snapshot_hash FROM applies
             WHERE workspace_id = ? AND status = 'completed'
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(workspace_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadApply {
            apply_id: workspace_id.to_string(),
            source,
        })?;
        Ok(row.and_then(|(hash,)| hash))
    }

    /// Whether a completed apply already exists for a workspace.
    ///
    /// The idempotency guard: the same workspace result must not be applied
    /// twice.
    pub async fn has_completed_for_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<bool, StorageError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM applies WHERE workspace_id = ? AND status = 'completed' LIMIT 1",
        )
        .bind(workspace_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadApply {
            apply_id: workspace_id.to_string(),
            source,
        })?;
        Ok(row.is_some())
    }

    /// Mark an apply as `Completed` with its completion time.
    pub async fn mark_completed(&self, id: Uuid) -> Result<(), StorageError> {
        self.update_status(id, ApplyStatus::Completed, None).await
    }

    /// Mark an apply as `Failed`, storing the bounded error message.
    pub async fn mark_failed(&self, id: Uuid, error: &str) -> Result<(), StorageError> {
        self.update_status(id, ApplyStatus::Failed, Some(error))
            .await
    }

    async fn update_status(
        &self,
        id: Uuid,
        status: ApplyStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result =
            sqlx::query("UPDATE applies SET status = ?, error = ?, completed_at = ? WHERE id = ?")
                .bind(status.as_str())
                .bind(error)
                .bind(Utc::now().to_rfc3339())
                .bind(id.to_string())
                .execute(self.database.pool())
                .await
                .map_err(|source| StorageError::UpdateApply {
                    apply_id: id.to_string(),
                    source,
                })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::ApplyNotFound(id.to_string()));
        }
        Ok(())
    }
}

fn row_to_apply(row: &sqlx::sqlite::SqliteRow) -> Result<ApplyRow, StorageError> {
    let id: String = row.get("id");
    let parse = |value: &str| -> Result<Uuid, StorageError> {
        Uuid::parse_str(value).map_err(|err| StorageError::LoadApply {
            apply_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })
    };
    Ok(ApplyRow {
        id: parse(&id)?,
        task_id: row
            .get::<Option<String>, _>("task_id")
            .as_deref()
            .map(parse)
            .transpose()?,
        workflow_id: row
            .get::<Option<String>, _>("workflow_id")
            .as_deref()
            .map(parse)
            .transpose()?,
        workspace_id: parse(&row.get::<String, _>("workspace_id"))?,
        source_repository: row.get::<String, _>("source_repository").into(),
        base_revision: row.get("base_revision"),
        status: ApplyStatus::from_str(&row.get::<String, _>("status")).ok_or_else(|| {
            StorageError::LoadApply {
                apply_id: id.clone(),
                source: sqlx::Error::Decode("unknown apply status".into()),
            }
        })?,
        error: row.get("error"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
        workspace_snapshot_hash: row.get("workspace_snapshot_hash"),
    })
}
