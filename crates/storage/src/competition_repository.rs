//! Best-of-N Competition persistence (Phase 23).

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Stable competition-group lifecycle statuses (stored as strings).
pub mod competition_status {
    pub const PENDING: &str = "pending";
    pub const RUNNING: &str = "running";
    pub const EVALUATING: &str = "evaluating";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
}

/// Stable competition-candidate lifecycle statuses.
pub mod candidate_status {
    pub const PENDING: &str = "pending";
    pub const RUNNING: &str = "running";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
    pub const DISQUALIFIED: &str = "disqualified";
}

/// SQL row shape of `competition_groups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompetitionGroupRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub source_workspace: Option<String>,
    pub base_revision: String,
    pub candidate_count: i64,
    pub status: String,
    pub winner_candidate_id: Option<String>,
    pub winner_task_id: Option<Uuid>,
    pub winner_workspace_id: Option<Uuid>,
    pub winner_snapshot_hash: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// SQL row shape of `competition_candidates`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompetitionCandidateRow {
    pub id: Uuid,
    pub group_id: Uuid,
    pub candidate_id: String,
    pub agent_id: String,
    pub session_lane: String,
    pub task_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub snapshot_hash: Option<String>,
    pub evaluation_group_id: Option<Uuid>,
    pub status: String,
    pub summary: Option<String>,
    pub patch_path: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Persists competition groups and their candidates.
#[derive(Clone)]
pub struct CompetitionRepository {
    database: Database,
}

impl CompetitionRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    // ---------- groups ----------

    pub async fn create_group(&self, row: &CompetitionGroupRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO competition_groups
                (id, workflow_id, source_workspace, base_revision, candidate_count, status,
                 winner_candidate_id, winner_task_id, winner_workspace_id, winner_snapshot_hash,
                 created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.workflow_id.to_string())
        .bind(&row.source_workspace)
        .bind(&row.base_revision)
        .bind(row.candidate_count)
        .bind(&row.status)
        .bind(&row.winner_candidate_id)
        .bind(row.winner_task_id.map(|id| id.to_string()))
        .bind(row.winner_workspace_id.map(|id| id.to_string()))
        .bind(&row.winner_snapshot_hash)
        .bind(&row.created_at)
        .bind(&row.updated_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    pub async fn get_group(&self, id: Uuid) -> Result<Option<CompetitionGroupRow>, StorageError> {
        let row = sqlx::query(
            "SELECT id, workflow_id, source_workspace, base_revision, candidate_count, status,
                    winner_candidate_id, winner_task_id, winner_workspace_id, winner_snapshot_hash,
                    created_at, updated_at
             FROM competition_groups WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadPlan {
            plan_id: id.to_string(),
            source,
        })?;
        row.map(|r| row_to_group(&r)).transpose()
    }

    pub async fn get_group_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<CompetitionGroupRow>, StorageError> {
        let row = sqlx::query(
            "SELECT id, workflow_id, source_workspace, base_revision, candidate_count, status,
                    winner_candidate_id, winner_task_id, winner_workspace_id, winner_snapshot_hash,
                    created_at, updated_at
             FROM competition_groups
             WHERE workflow_id = ?
             ORDER BY created_at DESC
             LIMIT 1",
        )
        .bind(workflow_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadPlan {
            plan_id: workflow_id.to_string(),
            source,
        })?;
        row.map(|r| row_to_group(&r)).transpose()
    }

    pub async fn list_groups_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<CompetitionGroupRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, workflow_id, source_workspace, base_revision, candidate_count, status,
                    winner_candidate_id, winner_task_id, winner_workspace_id, winner_snapshot_hash,
                    created_at, updated_at
             FROM competition_groups
             WHERE workflow_id = ?
             ORDER BY created_at ASC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadPlan {
            plan_id: workflow_id.to_string(),
            source,
        })?;
        rows.iter().map(row_to_group).collect()
    }

    pub async fn set_group_status(&self, id: Uuid, status: &str) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE competition_groups SET status = ?, updated_at = ? WHERE id = ?")
            .bind(status)
            .bind(now)
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdatePlan {
                plan_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    pub async fn set_group_winner(
        &self,
        id: Uuid,
        winner_candidate_id: &str,
        winner_task_id: Option<Uuid>,
        winner_workspace_id: Option<Uuid>,
        winner_snapshot_hash: Option<&str>,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE competition_groups
             SET status = ?, winner_candidate_id = ?, winner_task_id = ?,
                 winner_workspace_id = ?, winner_snapshot_hash = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(competition_status::COMPLETED)
        .bind(winner_candidate_id)
        .bind(winner_task_id.map(|t| t.to_string()))
        .bind(winner_workspace_id.map(|w| w.to_string()))
        .bind(winner_snapshot_hash)
        .bind(now)
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: id.to_string(),
            source,
        })?;
        Ok(())
    }

    // ---------- candidates ----------

    pub async fn create_candidate(
        &self,
        row: &CompetitionCandidateRow,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO competition_candidates
                (id, group_id, candidate_id, agent_id, session_lane, task_id, workspace_id,
                 snapshot_hash, evaluation_group_id, status, summary, patch_path,
                 created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.group_id.to_string())
        .bind(&row.candidate_id)
        .bind(&row.agent_id)
        .bind(&row.session_lane)
        .bind(row.task_id.map(|id| id.to_string()))
        .bind(row.workspace_id.map(|id| id.to_string()))
        .bind(&row.snapshot_hash)
        .bind(row.evaluation_group_id.map(|id| id.to_string()))
        .bind(&row.status)
        .bind(&row.summary)
        .bind(&row.patch_path)
        .bind(&row.created_at)
        .bind(&row.updated_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    pub async fn list_candidates_for_group(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<CompetitionCandidateRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, group_id, candidate_id, agent_id, session_lane, task_id, workspace_id,
                    snapshot_hash, evaluation_group_id, status, summary, patch_path,
                    created_at, updated_at
             FROM competition_candidates
             WHERE group_id = ?
             ORDER BY candidate_id ASC",
        )
        .bind(group_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadPlan {
            plan_id: group_id.to_string(),
            source,
        })?;
        rows.iter().map(row_to_candidate).collect()
    }

    pub async fn get_candidate(
        &self,
        group_id: Uuid,
        candidate_id: &str,
    ) -> Result<Option<CompetitionCandidateRow>, StorageError> {
        let row = sqlx::query(
            "SELECT id, group_id, candidate_id, agent_id, session_lane, task_id, workspace_id,
                    snapshot_hash, evaluation_group_id, status, summary, patch_path,
                    created_at, updated_at
             FROM competition_candidates
             WHERE group_id = ? AND candidate_id = ?",
        )
        .bind(group_id.to_string())
        .bind(candidate_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadPlan {
            plan_id: format!("{group_id}/{candidate_id}"),
            source,
        })?;
        row.map(|r| row_to_candidate(&r)).transpose()
    }

    pub async fn update_candidate_task_and_workspace(
        &self,
        group_id: Uuid,
        candidate_id: &str,
        task_id: Uuid,
        workspace_id: Option<Uuid>,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE competition_candidates
             SET task_id = ?, workspace_id = ?, updated_at = ?
             WHERE group_id = ? AND candidate_id = ?",
        )
        .bind(task_id.to_string())
        .bind(workspace_id.map(|w| w.to_string()))
        .bind(now)
        .bind(group_id.to_string())
        .bind(candidate_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: format!("{group_id}/{candidate_id}"),
            source,
        })?;
        Ok(())
    }

    pub async fn update_candidate_completion(
        &self,
        group_id: Uuid,
        candidate_id: &str,
        status: &str,
        snapshot_hash: Option<&str>,
        summary: Option<&str>,
        patch_path: Option<&str>,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE competition_candidates
             SET status = ?, snapshot_hash = ?, summary = ?, patch_path = ?, updated_at = ?
             WHERE group_id = ? AND candidate_id = ?",
        )
        .bind(status)
        .bind(snapshot_hash)
        .bind(summary)
        .bind(patch_path)
        .bind(now)
        .bind(group_id.to_string())
        .bind(candidate_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: format!("{group_id}/{candidate_id}"),
            source,
        })?;
        Ok(())
    }

    pub async fn update_candidate_evaluation_group(
        &self,
        group_id: Uuid,
        candidate_id: &str,
        evaluation_group_id: Uuid,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE competition_candidates
             SET evaluation_group_id = ?, updated_at = ?
             WHERE group_id = ? AND candidate_id = ?",
        )
        .bind(evaluation_group_id.to_string())
        .bind(now)
        .bind(group_id.to_string())
        .bind(candidate_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: format!("{group_id}/{candidate_id}"),
            source,
        })?;
        Ok(())
    }

    pub async fn update_candidate_status(
        &self,
        group_id: Uuid,
        candidate_id: &str,
        status: &str,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE competition_candidates
             SET status = ?, updated_at = ?
             WHERE group_id = ? AND candidate_id = ?",
        )
        .bind(status)
        .bind(now)
        .bind(group_id.to_string())
        .bind(candidate_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: format!("{group_id}/{candidate_id}"),
            source,
        })?;
        Ok(())
    }
}

fn row_to_group(row: &sqlx::sqlite::SqliteRow) -> Result<CompetitionGroupRow, StorageError> {
    let id_str: String = row.get("id");
    let wf_str: String = row.get("workflow_id");
    let winner_task_str: Option<String> = row.get("winner_task_id");
    let winner_ws_str: Option<String> = row.get("winner_workspace_id");
    Ok(CompetitionGroupRow {
        id: Uuid::parse_str(&id_str).map_err(|err| StorageError::LoadPlan {
            plan_id: id_str.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        workflow_id: Uuid::parse_str(&wf_str).map_err(|err| StorageError::LoadPlan {
            plan_id: wf_str.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        source_workspace: row.get("source_workspace"),
        base_revision: row.get("base_revision"),
        candidate_count: row.get("candidate_count"),
        status: row.get("status"),
        winner_candidate_id: row.get("winner_candidate_id"),
        winner_task_id: winner_task_str
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id_str.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        winner_workspace_id: winner_ws_str
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id_str.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        winner_snapshot_hash: row.get("winner_snapshot_hash"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_candidate(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<CompetitionCandidateRow, StorageError> {
    let id_str: String = row.get("id");
    let group_str: String = row.get("group_id");
    let task_str: Option<String> = row.get("task_id");
    let ws_str: Option<String> = row.get("workspace_id");
    let eval_group_str: Option<String> = row.get("evaluation_group_id");

    Ok(CompetitionCandidateRow {
        id: Uuid::parse_str(&id_str).map_err(|err| StorageError::LoadPlan {
            plan_id: id_str.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        group_id: Uuid::parse_str(&group_str).map_err(|err| StorageError::LoadPlan {
            plan_id: group_str.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        candidate_id: row.get("candidate_id"),
        agent_id: row.get("agent_id"),
        session_lane: row.get("session_lane"),
        task_id: task_str
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id_str.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        workspace_id: ws_str
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id_str.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        snapshot_hash: row.get("snapshot_hash"),
        evaluation_group_id: eval_group_str
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id_str.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        status: row.get("status"),
        summary: row.get("summary"),
        patch_path: row.get("patch_path"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}
