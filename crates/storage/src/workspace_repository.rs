//! Workspace persistence.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Lifecycle state of a persisted workspace (Phase 14).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceState {
    /// Agent workspace is normally retained.
    Active,
    /// At least one apply succeeded from this workspace to the source.
    Applied,
    /// The user explicitly kept it but it is no longer active.
    Archived,
    /// The filesystem workspace is gone.
    Missing,
    /// AgentMesh explicitly and safely removed it.
    Removed,
}

impl WorkspaceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkspaceState::Active => "active",
            WorkspaceState::Applied => "applied",
            WorkspaceState::Archived => "archived",
            WorkspaceState::Missing => "missing",
            WorkspaceState::Removed => "removed",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "active" => WorkspaceState::Active,
            "applied" => WorkspaceState::Applied,
            "archived" => WorkspaceState::Archived,
            "missing" => WorkspaceState::Missing,
            "removed" => WorkspaceState::Removed,
            _ => return None,
        })
    }

    /// Whether a workspace in this state is still usable as a worktree.
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            WorkspaceState::Active | WorkspaceState::Applied | WorkspaceState::Archived
        )
    }
}

/// A persisted Git worktree bound to an agent session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRow {
    pub id: Uuid,
    pub agent_session_id: Uuid,
    pub repository_root: std::path::PathBuf,
    pub path: std::path::PathBuf,
    pub branch: String,
    pub base_revision: String,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Persists [`WorkspaceRow`]s.
#[derive(Clone)]
pub struct WorkspaceRepository {
    database: Database,
}

impl WorkspaceRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn create(&self, workspace: &WorkspaceRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workspaces (id, agent_session_id, repository_root, path, branch, base_revision, state, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(workspace.id.to_string())
        .bind(workspace.agent_session_id.to_string())
        .bind(workspace.repository_root.display().to_string())
        .bind(workspace.path.display().to_string())
        .bind(&workspace.branch)
        .bind(&workspace.base_revision)
        .bind(workspace.state.as_str())
        .bind(workspace.created_at.to_rfc3339())
        .bind(workspace.updated_at.to_rfc3339())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreateWorkspace {
            workspace_id: workspace.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a workspace by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<WorkspaceRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workspaces WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkspace {
                workspace_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_workspace(&row)).transpose()
    }

    /// Load the workspace bound to an agent session.
    pub async fn get_by_agent_session(
        &self,
        agent_session_id: Uuid,
    ) -> Result<Option<WorkspaceRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workspaces WHERE agent_session_id = ?")
            .bind(agent_session_id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkspace {
                workspace_id: agent_session_id.to_string(),
                source,
            })?;
        row.map(|row| row_to_workspace(&row)).transpose()
    }

    /// Load a workspace by its path.
    pub async fn get_by_path(
        &self,
        path: &std::path::Path,
    ) -> Result<Option<WorkspaceRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workspaces WHERE path = ?")
            .bind(path.display().to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkspace {
                workspace_id: path.display().to_string(),
                source,
            })?;
        row.map(|row| row_to_workspace(&row)).transpose()
    }

    /// Update the workspace state.
    pub async fn set_state(&self, id: Uuid, state: WorkspaceState) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE workspaces SET state = ?, updated_at = ? WHERE id = ?")
            .bind(state.as_str())
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkspace {
                workspace_id: id.to_string(),
                source,
            })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::WorkspaceNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Update `updated_at` to now.
    pub async fn touch(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE workspaces SET updated_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkspace {
                workspace_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Update the workspace's branch (used by lifecycle tests and diagnostics).
    pub async fn set_branch(&self, id: Uuid, branch: &str) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE workspaces SET branch = ?, updated_at = ? WHERE id = ?")
            .bind(branch)
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkspace {
                workspace_id: id.to_string(),
                source,
            })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::WorkspaceNotFound(id.to_string()));
        }
        Ok(())
    }

    /// All workspaces, newest first, optionally filtered by state.
    pub async fn list(
        &self,
        state: Option<WorkspaceState>,
        limit: usize,
    ) -> Result<Vec<WorkspaceRow>, StorageError> {
        let limit = limit.clamp(1, 200);
        let mut query = String::from("SELECT * FROM workspaces WHERE 1 = 1");
        if state.is_some() {
            query.push_str(" AND state = ?");
        }
        query.push_str(" ORDER BY created_at DESC LIMIT ?");
        let mut q = sqlx::query(&query);
        if let Some(state) = state {
            q = q.bind(state.as_str());
        }
        let rows = q
            .bind(limit as i64)
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListWorkspaces)?;
        rows.iter().map(row_to_workspace).collect()
    }

    /// All workspaces joined with their agent session's `agent_id`, newest
    /// first. Used by the `agentmesh workspaces` listing.
    pub async fn list_with_agent(
        &self,
        state: Option<WorkspaceState>,
        limit: usize,
    ) -> Result<Vec<(WorkspaceRow, String)>, StorageError> {
        let limit = limit.clamp(1, 200);
        let mut query = String::from(
            "SELECT w.*, s.agent_id AS agent_id
             FROM workspaces w
             LEFT JOIN agent_sessions s ON s.id = w.agent_session_id
             WHERE 1 = 1",
        );
        if state.is_some() {
            query.push_str(" AND w.state = ?");
        }
        query.push_str(" ORDER BY w.created_at DESC LIMIT ?");
        let mut q = sqlx::query(&query);
        if let Some(state) = state {
            q = q.bind(state.as_str());
        }
        let rows = q
            .bind(limit as i64)
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListWorkspaces)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let agent_id: Option<String> = row.get("agent_id");
            out.push((row_to_workspace(row)?, agent_id.unwrap_or_default()));
        }
        Ok(out)
    }
}

fn row_to_workspace(row: &sqlx::sqlite::SqliteRow) -> Result<WorkspaceRow, StorageError> {
    let workspace_id: String = row.get("id");
    let parse = |value: String| -> Result<Uuid, StorageError> {
        Uuid::parse_str(&value).map_err(|err| StorageError::LoadWorkspace {
            workspace_id: workspace_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })
    };
    Ok(WorkspaceRow {
        id: parse(row.get("id"))?,
        agent_session_id: parse(row.get("agent_session_id"))?,
        repository_root: row.get::<String, _>("repository_root").into(),
        path: row.get::<String, _>("path").into(),
        branch: row.get("branch"),
        base_revision: row.get("base_revision"),
        state: WorkspaceState::from_str(&row.get::<String, _>("state")).ok_or_else(|| {
            StorageError::LoadWorkspace {
                workspace_id: workspace_id.clone(),
                source: sqlx::Error::Decode("unknown workspace state".into()),
            }
        })?,
        created_at: row_to_time(row, "created_at", &workspace_id)?,
        updated_at: row_to_time(row, "updated_at", &workspace_id)?,
    })
}

fn row_to_time(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
    workspace_id: &str,
) -> Result<DateTime<Utc>, StorageError> {
    let value: String = row.get(column);
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::LoadWorkspace {
            workspace_id: workspace_id.to_string(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })
}
