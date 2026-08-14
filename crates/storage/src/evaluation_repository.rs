//! Evaluation group + member persistence (Phase 21).

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Stable evaluation-group lifecycle statuses (stored as strings).
pub mod evaluation_status {
    /// The evaluators have not started.
    pub const PENDING: &str = "pending";
    /// At least one evaluator is running.
    pub const RUNNING: &str = "running";
    /// The consensus gate has run.
    pub const COMPLETED: &str = "completed";
    /// The evaluation could not form a consensus.
    pub const FAILED: &str = "failed";
    /// The workflow was cancelled.
    pub const CANCELLED: &str = "cancelled";
}

/// Stable evaluation-member lifecycle statuses.
pub mod member_status {
    pub const PENDING: &str = "pending";
    pub const RUNNING: &str = "running";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
}

/// SQL row shape of `evaluation_groups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationGroupRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub source_task_id: Option<Uuid>,
    pub strategy: String,
    pub quorum: i64,
    pub status: String,
    /// Serialized [`agentmesh_orchestrator::evaluation::ConsensusResult`].
    pub consensus: Option<String>,
    pub snapshot_hash: Option<String>,
    /// Which consensus fix round this group evaluates (Phase 22 §13): 0 is the
    /// initial evaluation, 1 the bounded fix round. Old rows backfill to 0.
    pub round: i64,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// SQL row shape of `evaluation_members`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationMemberRow {
    pub id: Uuid,
    pub group_id: Uuid,
    /// The DAG node id of this evaluator.
    pub node_id: String,
    pub agent_id: String,
    pub task_id: Option<Uuid>,
    pub status: String,
    /// Serialized [`agentmesh_orchestrator::evaluation::EvaluationResult`].
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// Persists evaluation groups and their members.
#[derive(Clone)]
pub struct EvaluationRepository {
    database: Database,
}

impl EvaluationRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    // ---------- groups ----------

    pub async fn create_group(&self, row: &EvaluationGroupRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO evaluation_groups
                (id, workflow_id, source_task_id, strategy, quorum, status, consensus, snapshot_hash, round, created_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.workflow_id.to_string())
        .bind(row.source_task_id.map(|id| id.to_string()))
        .bind(&row.strategy)
        .bind(row.quorum)
        .bind(&row.status)
        .bind(&row.consensus)
        .bind(&row.snapshot_hash)
        .bind(row.round)
        .bind(&row.created_at)
        .bind(&row.completed_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    pub async fn get_group(&self, id: Uuid) -> Result<Option<EvaluationGroupRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM evaluation_groups WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadPlan {
                plan_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_group(&row)).transpose()
    }

    /// Groups of a workflow, newest/latest round first. A consensus fix round
    /// group is created after its predecessor, so round order == created order;
    /// sorting by round makes "the current group" unambiguous across a resume.
    pub async fn list_groups(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<EvaluationGroupRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM evaluation_groups WHERE workflow_id = ? ORDER BY round DESC, created_at DESC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_group).collect()
    }

    pub async fn update_group_status(&self, id: Uuid, status: &str) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE evaluation_groups SET status = ? WHERE id = ?")
            .bind(status)
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

    /// Record the group's snapshot hash (before evaluators start).
    pub async fn set_group_snapshot(
        &self,
        id: Uuid,
        snapshot_hash: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE evaluation_groups SET snapshot_hash = ? WHERE id = ?")
            .bind(snapshot_hash)
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

    /// Bind the group's source task (Phase 22): round-0 groups evaluate the
    /// implementation task; round-1 groups the fixer task, whose session is
    /// the same coding workspace.
    pub async fn set_group_source_task(
        &self,
        id: Uuid,
        source_task_id: Uuid,
    ) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE evaluation_groups SET source_task_id = ? WHERE id = ?")
            .bind(source_task_id.to_string())
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

    /// Complete the group with its consensus result.
    pub async fn complete_group(
        &self,
        id: Uuid,
        status: &str,
        consensus_json: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE evaluation_groups
             SET status = ?, consensus = ?, completed_at = ?
             WHERE id = ?",
        )
        .bind(status)
        .bind(consensus_json)
        .bind(Utc::now().to_rfc3339())
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

    // ---------- members ----------

    pub async fn create_member(&self, row: &EvaluationMemberRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO evaluation_members
                (id, group_id, node_id, agent_id, task_id, status, result_json, error, created_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(row.group_id.to_string())
        .bind(&row.node_id)
        .bind(&row.agent_id)
        .bind(row.task_id.map(|id| id.to_string()))
        .bind(&row.status)
        .bind(&row.result_json)
        .bind(&row.error)
        .bind(&row.created_at)
        .bind(&row.completed_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    pub async fn list_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<EvaluationMemberRow>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM evaluation_members WHERE group_id = ? ORDER BY created_at")
                .bind(group_id.to_string())
                .fetch_all(self.database.pool())
                .await
                .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_member).collect()
    }

    /// The member for a group's evaluator node.
    pub async fn member_for_node(
        &self,
        group_id: Uuid,
        node_id: &str,
    ) -> Result<Option<EvaluationMemberRow>, StorageError> {
        let row =
            sqlx::query("SELECT * FROM evaluation_members WHERE group_id = ? AND node_id = ?")
                .bind(group_id.to_string())
                .bind(node_id)
                .fetch_optional(self.database.pool())
                .await
                .map_err(StorageError::ListPlans)?;
        row.map(|row| row_to_member(&row)).transpose()
    }

    /// Update a member's status + optional result/error.
    pub async fn update_member(
        &self,
        id: Uuid,
        status: &str,
        result_json: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE evaluation_members
             SET status = ?, result_json = ?, error = ?, completed_at = ?
             WHERE id = ?",
        )
        .bind(status)
        .bind(result_json)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
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

    /// Bind an evaluator's agent + task (Phase 21 §10) once it dispatches.
    pub async fn set_member_agent(
        &self,
        id: Uuid,
        agent_id: &str,
        task_id: Option<Uuid>,
    ) -> Result<(), StorageError> {
        let result =
            sqlx::query("UPDATE evaluation_members SET agent_id = ?, task_id = ? WHERE id = ?")
                .bind(agent_id)
                .bind(task_id.map(|id| id.to_string()))
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
}

fn row_to_group(row: &sqlx::sqlite::SqliteRow) -> Result<EvaluationGroupRow, StorageError> {
    let id: String = row.get("id");
    Ok(EvaluationGroupRow {
        id: Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
            plan_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        workflow_id: row
            .get::<String, _>("workflow_id")
            .parse::<Uuid>()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        source_task_id: row
            .get::<Option<String>, _>("source_task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        strategy: row.get("strategy"),
        quorum: row.get("quorum"),
        status: row.get("status"),
        consensus: row.get("consensus"),
        snapshot_hash: row.get("snapshot_hash"),
        round: row.get("round"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
    })
}

fn row_to_member(row: &sqlx::sqlite::SqliteRow) -> Result<EvaluationMemberRow, StorageError> {
    let id: String = row.get("id");
    Ok(EvaluationMemberRow {
        id: Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
            plan_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        group_id: row
            .get::<String, _>("group_id")
            .parse::<Uuid>()
            .map_err(|err| StorageError::LoadPlan {
                plan_id: id.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        node_id: row.get("node_id"),
        agent_id: row.get("agent_id"),
        task_id: row
            .get::<Option<String>, _>("task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        status: row.get("status"),
        result_json: row.get("result_json"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
    })
}
