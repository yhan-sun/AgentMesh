//! Workflow step persistence: one row per step/node of a daemon-owned
//! workflow, plus the DAG dependency edges (Phase 16).

use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// SQL row shape of the `workflow_steps` table. The `result_json` column
/// carries a serialized [`agentmesh_orchestrator::workflow::PersistedStepResult`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowStepRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub ordinal: i64,
    /// The stable node slug (DAG presets) — `None` for legacy sequential steps.
    pub node_id: Option<String>,
    pub role: String,
    pub intent: String,
    /// Untrusted planner objective (Phase 17); persisted since Phase 19 so a
    /// replanned DAG survives a crash without the original plan.
    pub objective: Option<String>,
    pub status: String,
    pub agent_id: Option<String>,
    pub task_id: Option<Uuid>,
    pub review_round: i64,
    pub summary: Option<String>,
    pub result_json: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error: Option<String>,
}

/// One directed dependency edge of a DAG workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowStepDependencyRow {
    pub workflow_id: Uuid,
    /// The dependent node.
    pub node_id: String,
    /// A node this node depends on.
    pub depends_on_node_id: String,
}

/// Persists workflow steps and their DAG dependency edges.
#[derive(Clone)]
pub struct WorkflowStepRepository {
    database: Database,
}

impl WorkflowStepRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert or update a step row keyed by `(workflow_id, ordinal)`.
    pub async fn upsert(&self, row: &WorkflowStepRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_steps
                (id, workflow_id, ordinal, node_id, role, intent, objective, status, agent_id, task_id, review_round, summary, result_json, created_at, started_at, completed_at, error)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workflow_id, ordinal) DO UPDATE SET
                id = excluded.id,
                node_id = excluded.node_id,
                role = excluded.role,
                intent = excluded.intent,
                objective = excluded.objective,
                status = excluded.status,
                agent_id = excluded.agent_id,
                task_id = excluded.task_id,
                review_round = excluded.review_round,
                summary = excluded.summary,
                result_json = excluded.result_json,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                error = excluded.error",
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
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpsertStep {
            workflow_id: row.workflow_id.to_string(),
            ordinal: row.ordinal,
            source,
        })?;
        Ok(())
    }

    /// All steps of a workflow, ordered by ordinal.
    pub async fn list_for(&self, workflow_id: Uuid) -> Result<Vec<WorkflowStepRow>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM workflow_steps WHERE workflow_id = ? ORDER BY ordinal")
                .bind(workflow_id.to_string())
                .fetch_all(self.database.pool())
                .await
                .map_err(|source| StorageError::ListSteps {
                    workflow_id: workflow_id.to_string(),
                    source,
                })?;
        rows.iter().map(row_to_step).collect()
    }

    // ---------- Phase 16: DAG dependency edges ----------

    /// Replace the dependency edges of a workflow (delete + insert in one call).
    pub async fn set_dependencies(
        &self,
        workflow_id: Uuid,
        edges: &[(String, String)],
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
        tx.commit()
            .await
            .map_err(|source| StorageError::SetDependencies {
                workflow_id: workflow_id.to_string(),
                source,
            })
    }

    /// All dependency edges of a workflow.
    pub async fn list_dependencies(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowStepDependencyRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT workflow_id, node_id, depends_on_node_id
             FROM workflow_step_dependencies WHERE workflow_id = ?",
        )
        .bind(workflow_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(|source| StorageError::ListDependencies {
            workflow_id: workflow_id.to_string(),
            source,
        })?;
        rows.iter()
            .map(|row| {
                Ok(WorkflowStepDependencyRow {
                    workflow_id: row
                        .get::<String, _>("workflow_id")
                        .parse::<Uuid>()
                        .map_err(|err| StorageError::LoadStep {
                            step_id: "".to_string(),
                            source: sqlx::Error::Decode(err.to_string().into()),
                        })?,
                    node_id: row.get("node_id"),
                    depends_on_node_id: row.get("depends_on_node_id"),
                })
            })
            .collect()
    }

    /// Atomically replace a workflow's whole DAG: node rows and dependency
    /// edges (Phase 19). The caller holds the replan apply claim and the
    /// per-workflow graph lock, so no live scheduler write interleaves.
    ///
    /// `rows` is the complete new node set (existing immutable rows carry their
    /// preserved status/result); `edges` is the full new dependency set. Rows
    /// for nodes no longer in the graph are deleted.
    pub async fn replace_graph(
        &self,
        workflow_id: Uuid,
        rows: &[WorkflowStepRow],
        edges: &[(String, String)],
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
        tx.commit()
            .await
            .map_err(|source| StorageError::SetDependencies {
                workflow_id: workflow_id.to_string(),
                source,
            })
    }

    /// Mark every running step of the given workflows as `Interrupted`.
    ///
    /// Companion to [`crate::WorkflowRepository::recover_interrupted`].
    pub async fn recover_interrupted_for(
        &self,
        workflow_ids: &[Uuid],
        reason: &str,
    ) -> Result<usize, StorageError> {
        if workflow_ids.is_empty() {
            return Ok(0);
        }
        let mut query = String::from(
            "UPDATE workflow_steps
             SET status = 'interrupted', error = ?
             WHERE status = 'running' AND workflow_id IN (",
        );
        let placeholders: Vec<String> = (0..workflow_ids.len()).map(|_| "?".to_string()).collect();
        query.push_str(&placeholders.join(", "));
        query.push(')');
        let mut q = sqlx::query(&query).bind(reason);
        for id in workflow_ids {
            q = q.bind(id.to_string());
        }
        let result = q
            .execute(self.database.pool())
            .await
            .map_err(StorageError::RecoverSteps)?;
        Ok(result.rows_affected() as usize)
    }
}

fn row_to_step(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowStepRow, StorageError> {
    let id: String = row.get("id");
    let step_id = Uuid::parse_str(&id).map_err(|err| StorageError::LoadStep {
        step_id: id.clone(),
        source: sqlx::Error::Decode(err.to_string().into()),
    })?;
    Ok(WorkflowStepRow {
        id: step_id,
        workflow_id: row
            .get::<String, _>("workflow_id")
            .parse::<Uuid>()
            .map_err(|err| StorageError::LoadStep {
                step_id: id.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
        ordinal: row.get("ordinal"),
        node_id: row.get("node_id"),
        role: row.get("role"),
        intent: row.get("intent"),
        objective: row.get("objective"),
        status: row.get("status"),
        agent_id: row.get("agent_id"),
        task_id: row
            .get::<Option<String>, _>("task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        review_round: row.get("review_round"),
        summary: row.get("summary"),
        result_json: row.get("result_json"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        error: row.get("error"),
    })
}
