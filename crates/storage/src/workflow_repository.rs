//! Workflow persistence: one row per daemon-owned workflow.

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// SQL row shape of the `workflows` table. Statuses and timestamps are stored
/// as stable strings; the daemon maps them onto the orchestrator's enums.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRow {
    pub id: Uuid,
    pub preset: String,
    pub goal: String,
    pub status: String,
    pub context_id: Option<Uuid>,
    /// Serialized [`agentmesh_orchestrator::workflow::WorkflowOptions`].
    pub options_json: String,
    pub review_rounds: i64,
    pub runtime_owner: Option<String>,
    pub runtime_heartbeat_at: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    /// Bumped on every successful replan apply (Phase 19); starts at 1.
    pub graph_revision: i64,
    /// The failed workflow this workflow recovers (Phase 20); `None` otherwise.
    pub parent_workflow_id: Option<Uuid>,
    /// The node id of the parent whose failure this workflow recovers.
    pub recovery_of_node_id: Option<String>,
    /// Which recovery attempt this is for the parent (1 = first).
    pub recovery_attempt: i64,
    /// The explicit source project/repository this workflow operates on
    /// (Phase 22); `None` keeps the legacy daemon-cwd behavior.
    pub source_workspace: Option<String>,
}

/// Persists workflows.
#[derive(Clone)]
pub struct WorkflowRepository {
    database: Database,
}

impl WorkflowRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Insert a workflow (must not already exist).
    pub async fn create(&self, row: &WorkflowRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflows (id, preset, goal, status, context_id, options_json, review_rounds, runtime_owner, runtime_heartbeat_at, error, created_at, updated_at, completed_at, graph_revision, parent_workflow_id, recovery_of_node_id, recovery_attempt, source_workspace)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(&row.preset)
        .bind(&row.goal)
        .bind(&row.status)
        .bind(row.context_id.map(|id| id.to_string()))
        .bind(&row.options_json)
        .bind(row.review_rounds)
        .bind(&row.runtime_owner)
        .bind(&row.runtime_heartbeat_at)
        .bind(&row.error)
        .bind(&row.created_at)
        .bind(&row.updated_at)
        .bind(&row.completed_at)
        .bind(row.graph_revision)
        .bind(row.parent_workflow_id.map(|id| id.to_string()))
        .bind(&row.recovery_of_node_id)
        .bind(row.recovery_attempt)
        .bind(&row.source_workspace)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreateWorkflow {
            workflow_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a workflow by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<WorkflowRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workflows WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkflow {
                workflow_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_workflow(&row)).transpose()
    }

    /// All workflows, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowRow>, StorageError> {
        let rows = sqlx::query("SELECT * FROM workflows ORDER BY created_at DESC")
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListWorkflows)?;
        rows.iter().map(row_to_workflow).collect()
    }

    /// Update a workflow's status (and optionally clear/preserve other state).
    pub async fn update_status(
        &self,
        id: Uuid,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result =
            sqlx::query("UPDATE workflows SET status = ?, error = ?, updated_at = ? WHERE id = ?")
                .bind(status)
                .bind(error)
                .bind(Utc::now().to_rfc3339())
                .bind(id.to_string())
                .execute(self.database.pool())
                .await
                .map_err(|source| StorageError::UpdateWorkflow {
                    workflow_id: id.to_string(),
                    source,
                })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::WorkflowNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Bind the shared context id once the first step reports it.
    pub async fn set_context(&self, id: Uuid, context_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE workflows SET context_id = ?, updated_at = ? WHERE id = ?")
            .bind(context_id.to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkflow {
                workflow_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Claim/update runtime ownership of a running workflow.
    pub async fn set_owner(&self, id: Uuid, owner: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE workflows SET runtime_owner = ?, updated_at = ? WHERE id = ?")
            .bind(owner)
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkflow {
                workflow_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Touch the runtime heartbeat of a running workflow.
    pub async fn heartbeat(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE workflows SET runtime_heartbeat_at = ?, updated_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateWorkflow {
                workflow_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Mark a workflow terminal with its completion time and optional error.
    pub async fn mark_completed(
        &self,
        id: Uuid,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE workflows SET status = ?, error = ?, completed_at = ?, updated_at = ? WHERE id = ?",
        )
        .bind(status)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateWorkflow {
            workflow_id: id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// The current `graph_revision` of a workflow (Phase 19).
    pub async fn graph_revision(&self, id: Uuid) -> Result<i64, StorageError> {
        let row = sqlx::query("SELECT graph_revision FROM workflows WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkflow {
                workflow_id: id.to_string(),
                source,
            })?
            .ok_or_else(|| StorageError::WorkflowNotFound(id.to_string()))?;
        Ok(row.get("graph_revision"))
    }

    /// Atomically bump the graph revision (Phase 19). The caller holds the
    /// atomic replan apply claim, so exactly one apply advances it.
    pub async fn increment_graph_revision(&self, id: Uuid) -> Result<i64, StorageError> {
        let result = sqlx::query(
            "UPDATE workflows SET graph_revision = graph_revision + 1, updated_at = ?
             WHERE id = ?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateWorkflow {
            workflow_id: id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::WorkflowNotFound(id.to_string()));
        }
        self.graph_revision(id).await
    }

    /// Every workflow that directly recovers `parent_id` (its recovery child
    /// workflows), for lineage (Phase 20 §19).
    pub async fn child_workflows(&self, parent_id: Uuid) -> Result<Vec<WorkflowRow>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM workflows WHERE parent_workflow_id = ? ORDER BY created_at")
                .bind(parent_id.to_string())
                .fetch_all(self.database.pool())
                .await
                .map_err(StorageError::ListWorkflows)?;
        rows.iter().map(row_to_workflow).collect()
    }

    /// How many recovery children a workflow already has (attempt budgeting,
    /// Phase 20 §12).
    pub async fn recovery_child_count(&self, parent_id: Uuid) -> Result<i64, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM workflows WHERE parent_workflow_id = ?")
            .bind(parent_id.to_string())
            .fetch_one(self.database.pool())
            .await
            .map_err(StorageError::ListWorkflows)?;
        Ok(row.get::<i64, _>("n"))
    }

    /// Whether any running or interrupted workflow step depends on the given
    /// agent session (Phase 14 cleanup guard).
    ///
    /// A workflow owns its step tasks; a task's session owns the workspace.
    /// Cleaning a workspace whose session is referenced by an active workflow
    /// would break that workflow, so it is refused.
    pub async fn has_active_dependency_on_session(
        &self,
        session_id: Uuid,
    ) -> Result<bool, StorageError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM workflow_steps s
             JOIN workflows w ON w.id = s.workflow_id
             JOIN tasks t ON t.id = s.task_id
             WHERE w.status IN ('running', 'interrupted')
               AND t.agent_session_id = ?
             LIMIT 1",
        )
        .bind(session_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(StorageError::ListWorkflows)?;
        Ok(row.is_some())
    }

    /// Mark every running workflow as `Interrupted`.
    ///
    /// Called by a daemon holding the singleton scope lock: any `Running`
    /// workflow was owned by a now-dead daemon. Returns the number recovered.
    pub async fn recover_interrupted(&self, reason: &str) -> Result<usize, StorageError> {
        let result = sqlx::query(
            "UPDATE workflows
             SET status = 'interrupted', error = ?, runtime_owner = NULL, updated_at = ?
             WHERE status = 'running'",
        )
        .bind(reason)
        .bind(Utc::now().to_rfc3339())
        .execute(self.database.pool())
        .await
        .map_err(StorageError::RecoverWorkflows)?;
        Ok(result.rows_affected() as usize)
    }
}

fn row_to_workflow(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRow, StorageError> {
    let id: String = row.get("id");
    let workflow_id = Uuid::parse_str(&id).map_err(|err| StorageError::LoadWorkflow {
        workflow_id: id.clone(),
        source: sqlx::Error::Decode(err.to_string().into()),
    })?;
    Ok(WorkflowRow {
        id: workflow_id,
        preset: row.get("preset"),
        goal: row.get("goal"),
        status: row.get("status"),
        context_id: row
            .get::<Option<String>, _>("context_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        options_json: row.get("options_json"),
        review_rounds: row.get("review_rounds"),
        runtime_owner: row.get("runtime_owner"),
        runtime_heartbeat_at: row.get("runtime_heartbeat_at"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
        graph_revision: row.get("graph_revision"),
        parent_workflow_id: row
            .get::<Option<String>, _>("parent_workflow_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        recovery_of_node_id: row.get("recovery_of_node_id"),
        recovery_attempt: row.get("recovery_attempt"),
        source_workspace: row.get("source_workspace"),
    })
}
