//! Workflow plan persistence (Phase 17 + Phase 18): one row per AI-planner
//! plan, plus its revision history, edit metadata and atomic execution claim.

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Stable plan lifecycle statuses (stored as strings; the daemon maps them
/// onto its own [`agentmesh_daemon::planner::PlanStatus`]).
pub mod plan_status {
    /// The planner task is running; nothing is persisted yet.
    pub const GENERATING: &str = "generating";
    /// Generated + validated; ready for `plan execute`.
    pub const READY: &str = "ready";
    /// Planner output could not be parsed or did not validate.
    pub const INVALID: &str = "invalid";
    /// The planner task itself failed or was cancelled.
    pub const FAILED: &str = "failed";
    /// An execute claimed the plan; the workflow is being created. Set by the
    /// atomic [`WorkflowPlanRepository::claim_execution`] so concurrent
    /// executes can never both see `ready`.
    pub const EXECUTING: &str = "executing";
    /// Executed once; a workflow now owns the plan.
    pub const EXECUTED: &str = "executed";
}

/// Stable `workflow_plan_revisions.source` values (Phase 18).
pub mod plan_revision_source {
    /// The original AI-planner output (always revision 1).
    pub const PLANNER: &str = "planner";
    /// A user edit through `plan edit` (never overwrites the planner output).
    pub const USER_EDIT: &str = "user_edit";
}

/// Outcome of an atomic [`WorkflowPlanRepository::claim_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanClaimResult {
    /// This call won the claim; it may now create the workflow.
    Claimed,
    /// The plan is already `executed` — it runs at most once.
    AlreadyExecuted,
    /// Another concurrent execute won the claim and is creating the workflow.
    ExecutionInProgress,
    /// The plan is not claimable (not `ready`).
    NotReady,
}

/// One immutable revision of a plan's JSON (Phase 18). Revision 1 is the
/// original planner output; later revisions are user edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRevisionRow {
    pub id: Uuid,
    pub plan_id: Uuid,
    pub revision: i64,
    pub plan_json: String,
    pub source: String,
    pub created_at: String,
}

/// SQL row shape of the `workflow_plans` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowPlanRow {
    pub id: Uuid,
    pub goal: String,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub plan_json: Option<String>,
    pub validation_error: Option<String>,
    pub workflow_id: Option<Uuid>,
    pub created_at: String,
    pub updated_at: String,
    pub executed_at: Option<String>,
    /// The active revision (mirrors `workflow_plan_revisions`); `None` when
    /// the plan has never stored JSON (Phase 18).
    pub current_revision: Option<i64>,
    /// When an execute won the atomic claim (Phase 18); `None` until claimed.
    pub execution_claimed_at: Option<String>,
    /// Which revision actually executed (audit, Phase 18); `None` until done.
    pub executed_revision: Option<i64>,
}

/// Persists AI-planner workflow plans.
#[derive(Clone)]
pub struct WorkflowPlanRepository {
    database: Database,
}

impl WorkflowPlanRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a plan (must not already exist).
    pub async fn create(&self, row: &WorkflowPlanRow) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_plans
                (id, goal, status, planner_agent_id, planner_task_id, plan_json, validation_error, workflow_id, created_at, updated_at, executed_at, current_revision, execution_claimed_at, executed_revision)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id.to_string())
        .bind(&row.goal)
        .bind(&row.status)
        .bind(&row.planner_agent_id)
        .bind(row.planner_task_id.map(|id| id.to_string()))
        .bind(&row.plan_json)
        .bind(&row.validation_error)
        .bind(row.workflow_id.map(|id| id.to_string()))
        .bind(&row.created_at)
        .bind(&row.updated_at)
        .bind(&row.executed_at)
        .bind(row.current_revision)
        .bind(&row.execution_claimed_at)
        .bind(row.executed_revision)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a plan by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<WorkflowPlanRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workflow_plans WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadPlan {
                plan_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_plan(&row)).transpose()
    }

    /// The plan that produced a workflow (for crash resume), if any.
    pub async fn by_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<WorkflowPlanRow>, StorageError> {
        let row = sqlx::query("SELECT * FROM workflow_plans WHERE workflow_id = ?")
            .bind(workflow_id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(StorageError::ListPlans)?;
        row.map(|row| row_to_plan(&row)).transpose()
    }

    /// All plans, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowPlanRow>, StorageError> {
        let rows = sqlx::query("SELECT * FROM workflow_plans ORDER BY created_at DESC")
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_plan).collect()
    }

    /// Update a plan's status and optional validation error.
    pub async fn update_status(
        &self,
        id: Uuid,
        status: &str,
        validation_error: Option<&str>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, validation_error = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(status)
        .bind(validation_error)
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

    /// Mark a generated plan `ready`, recording the planner agent + task and
    /// the validated plan JSON.
    pub async fn mark_ready(
        &self,
        id: Uuid,
        planner_agent_id: &str,
        planner_task_id: Uuid,
        plan_json: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, planner_agent_id = ?, planner_task_id = ?, plan_json = ?,
                 validation_error = NULL, updated_at = ?
             WHERE id = ?",
        )
        .bind(plan_status::READY)
        .bind(planner_agent_id)
        .bind(planner_task_id.to_string())
        .bind(plan_json)
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

    /// Mark a plan executed: bind the persisted workflow it created. Enforced
    /// by the daemon — a plan may be executed only once (`PlanAlreadyExecuted`).
    pub async fn mark_executed(&self, id: Uuid, workflow_id: Uuid) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, workflow_id = ?, executed_at = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(plan_status::EXECUTED)
        .bind(workflow_id.to_string())
        .bind(Utc::now().to_rfc3339())
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

    // ---------- Phase 18: revisions ----------

    /// Append one immutable revision. `revision` must be strictly greater than
    /// the plan's highest stored revision (`UNIQUE (plan_id, revision)`).
    pub async fn add_revision(
        &self,
        plan_id: Uuid,
        revision: i64,
        plan_json: &str,
        source: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_plan_revisions (id, plan_id, revision, plan_json, source, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(plan_id.to_string())
        .bind(revision)
        .bind(plan_json)
        .bind(source)
        .bind(Utc::now().to_rfc3339())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreatePlan {
            plan_id: plan_id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Make `revision` the active one, mirroring its JSON onto the plan row.
    pub async fn set_current_revision(
        &self,
        plan_id: Uuid,
        revision: i64,
        plan_json: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET current_revision = ?, plan_json = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(revision)
        .bind(plan_json)
        .bind(Utc::now().to_rfc3339())
        .bind(plan_id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: plan_id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(plan_id.to_string()));
        }
        Ok(())
    }

    /// All revisions of a plan, ascending (revision 1 first).
    pub async fn list_revisions(
        &self,
        plan_id: Uuid,
    ) -> Result<Vec<PlanRevisionRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workflow_plan_revisions WHERE plan_id = ? ORDER BY revision",
        )
        .bind(plan_id.to_string())
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        rows.iter().map(row_to_revision).collect()
    }

    /// The highest-numbered revision, if any.
    pub async fn latest_revision(
        &self,
        plan_id: Uuid,
    ) -> Result<Option<PlanRevisionRow>, StorageError> {
        let row = sqlx::query(
            "SELECT * FROM workflow_plan_revisions WHERE plan_id = ?
             ORDER BY revision DESC LIMIT 1",
        )
        .bind(plan_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        row.map(|row| row_to_revision(&row)).transpose()
    }

    /// The original planner output (revision 1 if it exists, else the earliest
    /// revision), used as the `plan diff` baseline.
    pub async fn planner_revision(
        &self,
        plan_id: Uuid,
    ) -> Result<Option<PlanRevisionRow>, StorageError> {
        let row = sqlx::query(
            "SELECT * FROM workflow_plan_revisions WHERE plan_id = ?
             ORDER BY revision ASC LIMIT 1",
        )
        .bind(plan_id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        row.map(|row| row_to_revision(&row)).transpose()
    }

    // ---------- Phase 18: atomic execution claim ----------

    /// Atomically claim a `ready` plan for execution (Phase 18).
    ///
    /// A single conditional `UPDATE ... WHERE status = 'ready'` transitions the
    /// plan to `executing`; SQLite serializes writers, so exactly one concurrent
    /// caller wins. The loser classifies the plan from its current row — this
    /// read is only for error reporting, never for deciding who may proceed.
    ///
    /// The caller must NOT do `get → if ready → update`; that is the
    /// application-layer race this method exists to close.
    pub async fn claim_execution(&self, plan_id: Uuid) -> Result<PlanClaimResult, StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, execution_claimed_at = ?, updated_at = ?
             WHERE id = ? AND status = ?",
        )
        .bind(plan_status::EXECUTING)
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(plan_id.to_string())
        .bind(plan_status::READY)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: plan_id.to_string(),
            source,
        })?;
        if result.rows_affected() > 0 {
            return Ok(PlanClaimResult::Claimed);
        }
        match self.get(plan_id).await? {
            None => Err(StorageError::PlanNotFound(plan_id.to_string())),
            Some(row) if row.status == plan_status::EXECUTED => {
                Ok(PlanClaimResult::AlreadyExecuted)
            }
            Some(row) if row.status == plan_status::EXECUTING => {
                Ok(PlanClaimResult::ExecutionInProgress)
            }
            Some(_) => Ok(PlanClaimResult::NotReady),
        }
    }

    /// Mark the claimed plan executed, recording which revision ran (audit).
    pub async fn mark_executed_with_revision(
        &self,
        plan_id: Uuid,
        workflow_id: Uuid,
        revision: i64,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, workflow_id = ?, executed_at = ?, executed_revision = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(plan_status::EXECUTED)
        .bind(workflow_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(revision)
        .bind(Utc::now().to_rfc3339())
        .bind(plan_id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdatePlan {
            plan_id: plan_id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::PlanNotFound(plan_id.to_string()));
        }
        Ok(())
    }

    /// Recover plans stuck in `executing` after a daemon crash (Phase 19 §1).
    ///
    /// * `executing` + no workflow was ever created → `failed` (the daemon died
    ///   during execution setup, before the claim produced a workflow).
    /// * `executing` + a workflow exists → the claim produced a workflow, so the
    ///   plan *did* execute; correct it to `executed` (never mislabel `failed`).
    ///   The executed revision is the plan's current revision.
    ///
    /// Returns `(failed, corrected_to_executed)`.
    pub async fn recover_stale_executing(
        &self,
        error_message: &str,
    ) -> Result<(usize, usize), StorageError> {
        let failed = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?, validation_error = ?, updated_at = ?
             WHERE status = ? AND workflow_id IS NULL",
        )
        .bind(plan_status::FAILED)
        .bind(error_message)
        .bind(Utc::now().to_rfc3339())
        .bind(plan_status::EXECUTING)
        .execute(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        let executed = sqlx::query(
            "UPDATE workflow_plans
             SET status = ?,
                 executed_revision = COALESCE(executed_revision, current_revision, 1),
                 executed_at = COALESCE(executed_at, ?),
                 updated_at = ?
             WHERE status = ? AND workflow_id IS NOT NULL",
        )
        .bind(plan_status::EXECUTED)
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(plan_status::EXECUTING)
        .execute(self.database.pool())
        .await
        .map_err(StorageError::ListPlans)?;
        Ok((
            failed.rows_affected() as usize,
            executed.rows_affected() as usize,
        ))
    }
}

fn row_to_plan(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowPlanRow, StorageError> {
    let id: String = row.get("id");
    let plan_id = Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
        plan_id: id.clone(),
        source: sqlx::Error::Decode(err.to_string().into()),
    })?;
    Ok(WorkflowPlanRow {
        id: plan_id,
        goal: row.get("goal"),
        status: row.get("status"),
        planner_agent_id: row.get("planner_agent_id"),
        planner_task_id: row
            .get::<Option<String>, _>("planner_task_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        plan_json: row.get("plan_json"),
        validation_error: row.get("validation_error"),
        workflow_id: row
            .get::<Option<String>, _>("workflow_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        executed_at: row.get("executed_at"),
        current_revision: row.get("current_revision"),
        execution_claimed_at: row.get("execution_claimed_at"),
        executed_revision: row.get("executed_revision"),
    })
}

fn row_to_revision(row: &sqlx::sqlite::SqliteRow) -> Result<PlanRevisionRow, StorageError> {
    let id: String = row.get("id");
    let plan_id: String = row.get("plan_id");
    Ok(PlanRevisionRow {
        id: Uuid::parse_str(&id).map_err(|err| StorageError::LoadPlan {
            plan_id: id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        plan_id: Uuid::parse_str(&plan_id).map_err(|err| StorageError::LoadPlan {
            plan_id: plan_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        revision: row.get("revision"),
        plan_json: row.get("plan_json"),
        source: row.get("source"),
        created_at: row.get("created_at"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: Uuid) -> WorkflowPlanRow {
        let now = Utc::now().to_rfc3339();
        WorkflowPlanRow {
            id,
            goal: "Refactor auth".to_string(),
            status: plan_status::GENERATING.to_string(),
            planner_agent_id: None,
            planner_task_id: None,
            plan_json: None,
            validation_error: None,
            workflow_id: None,
            created_at: now.clone(),
            updated_at: now,
            executed_at: None,
            current_revision: None,
            execution_claimed_at: None,
            executed_revision: None,
        }
    }

    /// Mark a plan `ready` with the given JSON (the common test setup).
    async fn ready_with_json(repo: &WorkflowPlanRepository, id: Uuid, plan_json: &str) {
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");
        repo.set_current_revision(id, 1, plan_json)
            .await
            .expect("current");
        repo.add_revision(id, 1, plan_json, plan_revision_source::PLANNER)
            .await
            .expect("revision 1");
    }

    #[tokio::test]
    async fn plan_roundtrips_through_the_repository() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        repo.update_status(id, plan_status::INVALID, Some("unknown role"))
            .await
            .expect("update");
        let loaded = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(loaded.status, plan_status::INVALID);
        assert_eq!(loaded.validation_error.as_deref(), Some("unknown role"));
        assert_eq!(repo.get(Uuid::new_v4()).await.expect("get"), None);
    }

    #[tokio::test]
    async fn mark_ready_and_mark_executed_bind_the_workflow() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");

        let task_id = Uuid::new_v4();
        repo.mark_ready(id, "claude", task_id, r#"{"version":1}"#)
            .await
            .expect("ready");
        let ready = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(ready.status, plan_status::READY);
        assert_eq!(ready.planner_agent_id.as_deref(), Some("claude"));
        assert_eq!(ready.planner_task_id, Some(task_id));
        assert_eq!(ready.plan_json.as_deref(), Some(r#"{"version":1}"#));

        let workflow_id = Uuid::new_v4();
        repo.mark_executed(id, workflow_id).await.expect("executed");
        let executed = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(executed.status, plan_status::EXECUTED);
        assert_eq!(executed.workflow_id, Some(workflow_id));
        assert!(executed.executed_at.is_some());

        // The plan that produced a workflow is findable for crash resume.
        let by_workflow = repo
            .by_workflow(workflow_id)
            .await
            .expect("by workflow")
            .expect("exists");
        assert_eq!(by_workflow.id, id);
    }

    #[tokio::test]
    async fn plans_list_newest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut row_a = row(a);
        let mut row_b = row(b);
        row_a.created_at = "2026-08-12T00:00:00+00:00".to_string();
        row_b.created_at = "2026-08-12T00:00:01+00:00".to_string();
        repo.create(&row_a).await.expect("create a");
        repo.create(&row_b).await.expect("create b");
        let all = repo.list().await.expect("list");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, b, "newest first");
        assert_eq!(all[1].id, a);
    }

    #[tokio::test]
    async fn unknown_plan_is_reported() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let err = repo
            .update_status(Uuid::new_v4(), plan_status::READY, None)
            .await
            .expect_err("missing");
        assert!(matches!(err, StorageError::PlanNotFound(_)));
    }

    // ---------- Phase 18 ----------

    #[tokio::test]
    async fn revisions_append_in_order_and_latest_wins() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        ready_with_json(&repo, id, r#"{"version":1}"#).await;

        repo.add_revision(id, 2, r#"{"version":2}"#, plan_revision_source::USER_EDIT)
            .await
            .expect("revision 2");
        repo.add_revision(id, 3, r#"{"version":3}"#, plan_revision_source::USER_EDIT)
            .await
            .expect("revision 3");

        let all = repo.list_revisions(id).await.expect("list");
        assert_eq!(all.len(), 3, "planner + two user edits");
        assert_eq!(all[0].revision, 1);
        assert_eq!(all[0].source, plan_revision_source::PLANNER);
        assert_eq!(all[1].revision, 2);
        assert_eq!(all[2].revision, 3);
        let revs: Vec<i64> = all.iter().map(|r| r.revision).collect();
        assert_eq!(revs, vec![1, 2, 3], "ascending order");

        let latest = repo
            .latest_revision(id)
            .await
            .expect("latest")
            .expect("some");
        assert_eq!(latest.revision, 3);
        assert_eq!(latest.plan_json, r#"{"version":3}"#);
        let planner = repo
            .planner_revision(id)
            .await
            .expect("planner")
            .expect("some");
        assert_eq!(planner.revision, 1, "planner output stays the baseline");
    }

    #[tokio::test]
    async fn set_current_revision_mirrors_json_onto_the_plan_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");
        repo.set_current_revision(id, 3, r#"{"current":3}"#)
            .await
            .expect("set");
        let loaded = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(loaded.current_revision, Some(3));
        assert_eq!(loaded.plan_json.as_deref(), Some(r#"{"current":3}"#));
    }

    #[tokio::test]
    async fn claim_execution_is_atomic_ready_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");

        // Not ready yet → NotReady.
        assert_eq!(
            repo.claim_execution(id).await.expect("claim"),
            PlanClaimResult::NotReady
        );

        // Ready → Claimed, then the second claim sees ExecutionInProgress.
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");
        assert_eq!(
            repo.claim_execution(id).await.expect("claim"),
            PlanClaimResult::Claimed
        );
        assert_eq!(
            repo.claim_execution(id).await.expect("second claim"),
            PlanClaimResult::ExecutionInProgress
        );
        let claimed = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(claimed.status, plan_status::EXECUTING);
        assert!(claimed.execution_claimed_at.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_claims_only_one_wins() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let repo = repo.clone();
            set.spawn(async move { repo.claim_execution(id).await });
        }
        let mut results = Vec::new();
        while let Some(res) = set.join_next().await {
            results.push(res.expect("claim future"));
        }
        let claimed = results
            .iter()
            .filter(|r| matches!(r, Ok(PlanClaimResult::Claimed)))
            .count();
        assert_eq!(claimed, 1, "exactly one concurrent claim may win");
        for r in results {
            let r = r.expect("claim future");
            assert!(
                matches!(
                    r,
                    PlanClaimResult::Claimed | PlanClaimResult::ExecutionInProgress
                ),
                "losers observe the in-progress execution, never ready"
            );
        }
    }

    #[tokio::test]
    async fn executed_plan_is_never_claimable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");
        repo.mark_executed(id, Uuid::new_v4())
            .await
            .expect("executed");
        assert_eq!(
            repo.claim_execution(id).await.expect("claim"),
            PlanClaimResult::AlreadyExecuted
        );
    }

    #[tokio::test]
    async fn mark_executed_with_revision_records_the_audit_revision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let repo = WorkflowPlanRepository::new(db.clone());
        let id = Uuid::new_v4();
        repo.create(&row(id)).await.expect("create");
        repo.update_status(id, plan_status::READY, None)
            .await
            .expect("ready");
        repo.set_current_revision(id, 2, r#"{"v":2}"#)
            .await
            .expect("current");

        let workflow_id = Uuid::new_v4();
        repo.mark_executed_with_revision(id, workflow_id, 2)
            .await
            .expect("executed");
        let executed = repo.get(id).await.expect("get").expect("exists");
        assert_eq!(executed.status, plan_status::EXECUTED);
        assert_eq!(executed.workflow_id, Some(workflow_id));
        assert_eq!(executed.executed_revision, Some(2));
        assert!(executed.executed_at.is_some());
    }
}
