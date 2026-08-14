//! Replan proposal persistence (Phase 19): one row per user-triggered replan.

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Stable replan lifecycle statuses (stored as strings).
pub mod replan_status {
    /// The replan planner task is running.
    pub const GENERATING: &str = "generating";
    /// Generated + validated; ready for `replan apply`.
    pub const READY: &str = "ready";
    /// The delta could not be parsed or the candidate graph did not validate.
    pub const INVALID: &str = "invalid";
    /// The replan planner task itself failed or was cancelled.
    pub const FAILED: &str = "failed";
    /// An apply claimed the proposal; the graph is being mutated.
    pub const APPLYING: &str = "applying";
    /// Applied: the workflow's graph_revision advanced.
    pub const APPLIED: &str = "applied";
    /// Rejected (e.g. stale base graph revision) and will never apply.
    pub const REJECTED: &str = "rejected";
}

/// Outcome of an atomic [`WorkflowReplanRepository::claim_apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplanApplyResult {
    /// This call won the claim; it may now mutate the workflow graph.
    Claimed,
    /// Another concurrent apply is mutating the graph.
    ApplyInProgress,
    /// The proposal is already applied.
    AlreadyApplied,
    /// The workflow's graph_revision no longer matches the proposal's base.
    ReplanStale,
    /// The proposal is not in a claimable state.
    NotReady,
}

/// SQL row shape of the `workflow_replans` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowReplanRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub delta_json: Option<String>,
    pub validation_error: Option<String>,
    pub base_graph_revision: i64,
    pub applied_graph_revision: Option<i64>,
    pub created_at: String,
    pub applied_at: Option<String>,
}

/// Persists replan proposals.
#[derive(Clone)]
pub struct WorkflowReplanRepository {
    database: Database,
}

impl WorkflowReplanRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a proposal (must not already exist).
    pub async fn create(&self, row: &WorkflowReplanRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_replans
                (id, workflow_id, status, planner_agent_id, planner_task_id, delta_json, validation_error, base_graph_revision, applied_graph_revision, created_at, applied_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.workflow_id.to_string())
        .bind(&row.status)
        .bind(&row.planner_agent_id)
        .bind(row.planner_task_id.map(|id| id.to_string()))
        .bind(&row.delta_json)
        .bind(&row.validation_error)
        .bind(row.base_graph_revision)
        .bind(row.applied_graph_revision)
        .bind(&row.created_at)
        .bind(&row.applied_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a proposal by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<WorkflowReplanRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workflow_replans WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadPlan {
                plan_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_replan(&row)).transpose()
    }

    /// All proposals of a workflow, newest first.
    pub async fn list_for(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowReplanRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workflow_replans WHERE workflow_id = ? ORDER BY created_at DESC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_replan).collect()
    }

    /// All proposals, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowReplanRow>, StorageError> {
        let rows = sqlx::query("SELECT * FROM workflow_replans ORDER BY created_at DESC")
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_replan).collect()
    }

    /// Update a proposal's status only while it still has `expected`.
    /// Returns whether the update was applied — a concurrent caller that
    /// already claimed/applied the proposal is never overwritten (used by
    /// the stale-base rejection path so a loser cannot clobber the winner).
    pub async fn update_status_if(
        &self,
        id: Uuid,
        expected: &str,
        status: &str,
        validation_error: Option<&str>,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_replans
             SET status = ?, validation_error = ?
             WHERE id = ? AND status = ?",
        )
        .bind(status)
        .bind(validation_error)
        .bind(id.to_string())
        .bind(expected)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: id.to_string(),
            source,
        })?;
        Ok(result.rows_affected() > 0)
    }

    /// Update a proposal's status + optional validation error.
    pub async fn update_status(
        &self,
        id: Uuid,
        status: &str,
        validation_error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_replans
             SET status = ?, validation_error = ?
             WHERE id = ?",
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

    /// Record the planner agent + task and the validated delta (→ `ready`).
    pub async fn set_ready(
        &self,
        id: Uuid,
        planner_agent_id: &str,
        planner_task_id: Uuid,
        delta_json: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_replans
             SET status = ?, planner_agent_id = ?, planner_task_id = ?, delta_json = ?,
                 validation_error = NULL
             WHERE id = ?",
        )
        .bind(replan_status::READY)
        .bind(planner_agent_id)
        .bind(planner_task_id.to_string())
        .bind(delta_json)
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

    /// Atomically claim a `ready` proposal for apply (Phase 19 §12).
    ///
    /// A single conditional `UPDATE ... WHERE status='ready' AND base = current
    /// workflow graph_revision` wins; SQLite serializes writers, so exactly one
    /// concurrent caller claims. The classification read afterwards is only for
    /// error reporting, never for deciding who may proceed.
    pub async fn claim_apply(
        &self,
        replan_id: Uuid,
        workflow_id: Uuid,
    ) -> Result<ReplanApplyResult, StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_replans
             SET status = ?
             WHERE id = ?
               AND status = ?
               AND base_graph_revision = (SELECT graph_revision FROM workflows WHERE id = ?)",
        )
        .bind(replan_status::APPLYING)
        .bind(replan_id.to_string())
        .bind(replan_status::READY)
        .bind(workflow_id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: replan_id.to_string(),
            source,
        })?;
        if result.rows_affected() > 0 {
            return Ok(ReplanApplyResult::Claimed);
        }
        match self.get(replan_id).await? {
            None => Err(StorageError::PlanNotFound(replan_id.to_string())),
            Some(row) if row.status == replan_status::APPLIED => {
                Ok(ReplanApplyResult::AlreadyApplied)
            }
            Some(row) if row.status == replan_status::APPLYING => {
                Ok(ReplanApplyResult::ApplyInProgress)
            }
            Some(row) => {
                let current = self.database_current_revision(workflow_id).await?;
                if row.base_graph_revision != current {
                    Ok(ReplanApplyResult::ReplanStale)
                } else {
                    Ok(ReplanApplyResult::NotReady)
                }
            }
        }
    }

    /// Mark the claimed proposal applied with the resulting graph revision.
    pub async fn mark_applied(
        &self,
        replan_id: Uuid,
        applied_graph_revision: i64,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_replans
             SET status = ?, applied_graph_revision = ?, applied_at = ?
             WHERE id = ?",
        )
        .bind(replan_status::APPLIED)
        .bind(applied_graph_revision)
        .bind(Utc::now().to_rfc3339())
        .bind(replan_id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: replan_id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(replan_id.to_string()));
        }
        Ok(())
    }

    /// The workflow's current graph_revision (for the stale classification).
    async fn database_current_revision(&self, workflow_id: Uuid) -> Result<i64, StorageError> {
        let row = sqlx::query("SELECT graph_revision FROM workflows WHERE id = ?")
            .bind(workflow_id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadWorkflow {
                workflow_id: workflow_id.to_string(),
                source,
            })?;
        Ok(row.map(|r| r.get::<i64, _>("graph_revision")).unwrap_or(0))
    }

    /// Apply a replan atomically (Phase 20 §2 P0): replace the workflow's node
    /// rows + dependency edges, bump `graph_revision`, and mark the proposal
    /// `applied` — all in one SQLite transaction, so a crash either leaves the
    /// workflow untouched (graph_revision == base) or fully applied
    /// (graph_revision == base + 1). Nothing in between is observable.
    pub async fn apply_graph_atomic(
        &self,
        replan_id: Uuid,
        workflow_id: Uuid,
        rows: &[crate::WorkflowStepRow],
        edges: &[(String, String)],
        new_graph_revision: i64,
    ) -> Result<(), StorageError> {
        let mut tx =
            self.database
                .pool()
                .begin()
                .await
                .map_err(|source| StorageError::SetDependencies {
                    workflow_id: workflow_id.to_string(),
                    source,
                })?;
        // 1. Replace node rows.
        sqlx::query("DELETE FROM workflow_steps WHERE workflow_id = ? AND node_id IS NOT NULL")
            .bind(workflow_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::UpsertStep {
                workflow_id: workflow_id.to_string(),
                ordinal: 0,
                source,
            })?;
        for row in rows {
            sqlx::query(
                "INSERT INTO workflow_steps
                    (id, workflow_id, ordinal, node_id, role, intent, objective, status, agent_id, task_id, review_round, summary, result_json, created_at, started_at, completed_at, error)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.id.to_string())
            .bind(row.workflow_id.to_string())
            .bind(row.ordinal)
            .bind(&row.node_id)
            .bind(&row.role)
            .bind(&row.intent)
            .bind(&row.objective)
            .bind(&row.status)
            .bind(&row.agent_id)
            .bind(row.task_id.map(|id| id.to_string()))
            .bind(row.review_round)
            .bind(&row.summary)
            .bind(&row.result_json)
            .bind(&row.created_at)
            .bind(&row.started_at)
            .bind(&row.completed_at)
            .bind(&row.error)
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::UpsertStep {
                workflow_id: workflow_id.to_string(),
                ordinal: row.ordinal,
                source,
            })?;
        }
        // 2. Replace dependency edges.
        sqlx::query("DELETE FROM workflow_step_dependencies WHERE workflow_id = ?")
            .bind(workflow_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::SetDependencies {
                workflow_id: workflow_id.to_string(),
                source,
            })?;
        for (node_id, depends_on) in edges {
            sqlx::query(
                "INSERT INTO workflow_step_dependencies (id, workflow_id, node_id, depends_on_node_id)
                 VALUES (?, ?, ?, ?)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(workflow_id.to_string())
            .bind(node_id)
            .bind(depends_on)
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::SetDependencies {
                workflow_id: workflow_id.to_string(),
                source,
            })?;
        }
        // 3. Bump graph_revision.
        sqlx::query("UPDATE workflows SET graph_revision = ?, updated_at = ? WHERE id = ?")
            .bind(new_graph_revision)
            .bind(Utc::now().to_rfc3339())
            .bind(workflow_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::UpdateWorkflow {
                workflow_id: workflow_id.to_string(),
                source,
            })?;
        // 4. Mark the proposal applied.
        sqlx::query(
            "UPDATE workflow_replans
             SET status = ?, applied_graph_revision = ?, applied_at = ?
             WHERE id = ?",
        )
        .bind(replan_status::APPLIED)
        .bind(new_graph_revision)
        .bind(Utc::now().to_rfc3339())
        .bind(replan_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: replan_id.to_string(),
            source,
        })?;
        tx.commit()
            .await
            .map_err(|source| StorageError::SetDependencies {
                workflow_id: workflow_id.to_string(),
                source,
            })
    }

    /// Recover replans stuck in `applying` after a daemon crash (Phase 20 §2).
    ///
    /// Because [`Self::apply_graph_atomic`] is one transaction, the workflow's
    /// graph_revision proves what happened:
    /// * still == base → the apply never committed → `ready` (safe to retry);
    /// * advanced past base → the apply committed → `applied`;
    /// * anything else (unprovable) → `failed`, never guessed.
    ///
    /// Returns `(ready, applied, failed)`.
    pub async fn recover_stale_applying(&self) -> Result<(usize, usize, usize), StorageError> {
        let rows = sqlx::query("SELECT * FROM workflow_replans WHERE status = ?")
            .bind(replan_status::APPLYING)
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListPlans)?;
        let mut ready = 0;
        let mut applied = 0;
        let mut failed = 0;
        for row in rows {
            let row = row_to_replan(&row)?;
            let current = self.database_current_revision(row.workflow_id).await?;
            let (status, error) = if current == row.base_graph_revision {
                (replan_status::READY, None)
            } else if current > row.base_graph_revision {
                (replan_status::APPLIED, None)
            } else {
                (
                    replan_status::FAILED,
                    Some("cannot prove replan apply outcome"),
                )
            };
            sqlx::query(
                "UPDATE workflow_replans
                 SET status = ?, applied_graph_revision = ?, validation_error = ?
                 WHERE id = ?",
            )
            .bind(status)
            .bind((current > row.base_graph_revision).then_some(current))
            .bind(error)
            .bind(row.id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdatePlan {
                plan_id: row.id.to_string(),
                source,
            })?;
            match status {
                replan_status::READY => ready += 1,
                replan_status::APPLIED => applied += 1,
                _ => failed += 1,
            }
        }
        Ok((ready, applied, failed))
    }

    /// All replans for a specific workflow, oldest first.
    pub async fn list_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowReplanRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workflow_replans WHERE workflow_id = ? ORDER BY created_at ASC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_replan).collect()
    }
}

fn row_to_replan(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowReplanRow, StorageError> {
    let id: String = row.get("id");
    let workflow_id: String = row.get("workflow_id");
    Ok(WorkflowReplanRow {
        id: Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
            plan_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        workflow_id: Uuid::parse_str(&workflow_id).map_err(|err| StorageError::LoadPlan {
            plan_id: workflow_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        status: row.get("status"),
        planner_agent_id: row.get("planner_agent_id"),
        planner_task_id: row
            .get::<Option<String>, _>("planner_task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        delta_json: row.get("delta_json"),
        validation_error: row.get("validation_error"),
        base_graph_revision: row.get("base_graph_revision"),
        applied_graph_revision: row.get("applied_graph_revision"),
        created_at: row.get("created_at"),
        applied_at: row.get("applied_at"),
    })
}
