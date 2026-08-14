//! Failure-recovery proposal persistence (Phase 20): one row per Failure
//! Analyzer proposal, from generation through the atomic execute claim.

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Stable recovery lifecycle statuses (stored as strings).
pub mod recovery_status {
    /// The Failure Analyzer task is running.
    pub const GENERATING: &str = "generating";
    /// Generated + validated; ready for `recovery execute`.
    pub const READY: &str = "ready";
    /// The analyzer output could not be parsed or did not validate.
    pub const INVALID: &str = "invalid";
    /// The analyzer task itself failed or was cancelled.
    pub const FAILED: &str = "failed";
    /// An execute claimed the proposal; the child workflow is being created.
    pub const EXECUTING: &str = "executing";
    /// Executed once; a recovery child workflow now owns this proposal.
    pub const EXECUTED: &str = "executed";
    /// Rejected (e.g. recovery attempt limit reached).
    pub const REJECTED: &str = "rejected";
}

/// Outcome of an atomic [`WorkflowRecoveryRepository::claim_execute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryClaimResult {
    /// This call won the claim; it may create the child workflow.
    Claimed,
    /// Another concurrent execute is creating the child workflow.
    ExecutionInProgress,
    /// The proposal already executed.
    AlreadyExecuted,
    /// The proposal is not in a claimable state.
    NotReady,
    /// The parent workflow's recovery attempt budget is exhausted.
    RecoveryLimitReached,
}

/// SQL row shape of the `workflow_recoveries` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRecoveryRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub failed_node_id: String,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub plan_json: Option<String>,
    pub validation_error: Option<String>,
    pub recovery_workflow_id: Option<Uuid>,
    pub attempt: i64,
    pub created_at: String,
    pub executed_at: Option<String>,
}

/// Persists failure-recovery proposals.
#[derive(Clone)]
pub struct WorkflowRecoveryRepository {
    database: Database,
}

impl WorkflowRecoveryRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a proposal (must not already exist).
    pub async fn create(&self, row: &WorkflowRecoveryRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_recoveries
                (id, workflow_id, failed_node_id, status, planner_agent_id, planner_task_id, plan_json, validation_error, recovery_workflow_id, attempt, created_at, executed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.workflow_id.to_string())
        .bind(&row.failed_node_id)
        .bind(&row.status)
        .bind(&row.planner_agent_id)
        .bind(row.planner_task_id.map(|id| id.to_string()))
        .bind(&row.plan_json)
        .bind(&row.validation_error)
        .bind(row.recovery_workflow_id.map(|id| id.to_string()))
        .bind(row.attempt)
        .bind(&row.created_at)
        .bind(&row.executed_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a proposal by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<WorkflowRecoveryRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workflow_recoveries WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadPlan {
                plan_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_recovery(&row)).transpose()
    }

    /// All proposals of a workflow, newest first.
    pub async fn list_for(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowRecoveryRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workflow_recoveries WHERE workflow_id = ? ORDER BY created_at DESC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_recovery).collect()
    }

    /// All proposals, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowRecoveryRow>, StorageError> {
        let rows = sqlx::query("SELECT * FROM workflow_recoveries ORDER BY created_at DESC")
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_recovery).collect()
    }

    /// Update a proposal's status + optional validation error.
    pub async fn update_status(
        &self,
        id: Uuid,
        status: &str,
        validation_error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_recoveries SET status = ?, validation_error = ? WHERE id = ?",
        )
        .bind(status)
        .bind(validation_error)
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Record the analyzer agent + task and the validated plan (→ `ready`).
    pub async fn set_ready(
        &self,
        id: Uuid,
        planner_agent_id: &str,
        planner_task_id: Uuid,
        plan_json: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_recoveries
             SET status = ?, planner_agent_id = ?, planner_task_id = ?, plan_json = ?,
                 validation_error = NULL
             WHERE id = ?",
        )
        .bind(recovery_status::READY)
        .bind(planner_agent_id)
        .bind(planner_task_id.to_string())
        .bind(plan_json)
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Atomically claim a `ready` proposal for execution (Phase 20 §16).
    ///
    /// A single conditional `UPDATE ... WHERE status='ready'` wins; SQLite
    /// serializes writers, so exactly one concurrent caller claims. The
    /// classification read afterwards is only for error reporting.
    pub async fn claim_execute(
        &self,
        recovery_id: Uuid,
    ) -> Result<RecoveryClaimResult, StorageError> {
        let result =
            sqlx::query("UPDATE workflow_recoveries SET status = ? WHERE id = ? AND status = ?")
                .bind(recovery_status::EXECUTING)
                .bind(recovery_id.to_string())
                .bind(recovery_status::READY)
                .execute(self.database.pool())
                .await
                .map_err(|source| StorageError::UpdatePlan {
                    plan_id: recovery_id.to_string(),
                    source,
                })?;
        if result.rows_affected() > 0 {
            return Ok(RecoveryClaimResult::Claimed);
        }
        match self.get(recovery_id).await? {
            None => Err(StorageError::PlanNotFound(recovery_id.to_string())),
            Some(row) if row.status == recovery_status::EXECUTED => {
                Ok(RecoveryClaimResult::AlreadyExecuted)
            }
            Some(row) if row.status == recovery_status::EXECUTING => {
                Ok(RecoveryClaimResult::ExecutionInProgress)
            }
            Some(_) => Ok(RecoveryClaimResult::NotReady),
        }
    }

    /// Mark the claimed proposal executed, binding the child workflow.
    pub async fn mark_executed(
        &self,
        recovery_id: Uuid,
        recovery_workflow_id: Uuid,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_recoveries
             SET status = ?, recovery_workflow_id = ?, executed_at = ?
             WHERE id = ?",
        )
        .bind(recovery_status::EXECUTED)
        .bind(recovery_workflow_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(recovery_id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: recovery_id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(recovery_id.to_string()));
        }
        Ok(())
    }

    /// Recover proposals stuck mid-flight after a daemon crash (Phase 20 §20).
    ///
    /// * `generating` — the Failure Analyzer died mid-run; the proposal is
    ///   `failed` (never auto-repeat the analyzer).
    /// * `executing` + no child workflow created → retryable `ready` (nothing
    ///   irreversible happened).
    /// * `executing` + a child workflow exists → `executed` with its child
    ///   bound (never guessed).
    ///
    /// Returns `(generating_failed, ready, executed)`.
    pub async fn recover_stale_executing(&self) -> Result<(usize, usize, usize), StorageError> {
        let analyzer_failed = sqlx::query(
            "UPDATE workflow_recoveries
             SET status = ?, validation_error = ?
             WHERE status = ?",
        )
        .bind(recovery_status::FAILED)
        .bind("AgentMesh daemon terminated during failure analysis.")
        .bind(recovery_status::GENERATING)
        .execute(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        let ready = sqlx::query(
            "UPDATE workflow_recoveries
             SET status = ?, validation_error = NULL
             WHERE status = ? AND recovery_workflow_id IS NULL",
        )
        .bind(recovery_status::READY)
        .bind(recovery_status::EXECUTING)
        .execute(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        let executed = sqlx::query(
            "UPDATE workflow_recoveries
             SET status = ?, executed_at = COALESCE(executed_at, ?)
             WHERE status = ? AND recovery_workflow_id IS NOT NULL",
        )
        .bind(recovery_status::EXECUTED)
        .bind(Utc::now().to_rfc3339())
        .bind(recovery_status::EXECUTING)
        .execute(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        Ok((
            analyzer_failed.rows_affected() as usize,
            ready.rows_affected() as usize,
            executed.rows_affected() as usize,
        ))
    }
}

fn row_to_recovery(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRecoveryRow, StorageError> {
    let id: String = row.get("id");
    let workflow_id: String = row.get("workflow_id");
    Ok(WorkflowRecoveryRow {
        id: Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
            plan_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        workflow_id: Uuid::parse_str(&workflow_id).map_err(|err| StorageError::LoadPlan {
            plan_id: workflow_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        failed_node_id: row.get("failed_node_id"),
        status: row.get("status"),
        planner_agent_id: row.get("planner_agent_id"),
        planner_task_id: row
            .get::<Option<String>, _>("planner_task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        plan_json: row.get("plan_json"),
        validation_error: row.get("validation_error"),
        recovery_workflow_id: row
            .get::<Option<String>, _>("recovery_workflow_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        attempt: row.get("attempt"),
        created_at: row.get("created_at"),
        executed_at: row.get("executed_at"),
    })
}
