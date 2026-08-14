//! Phase 17 + Phase 18: AI Planner service.
//!
//! The planner is an ordinary A2A agent. Generating a plan is a single A2A
//! task routed with [`TaskIntent::Architecture`] (or an explicit `--agent`
//! override — still over A2A, never an adapter):
//!
//! ```text
//! goal
//!   → RuleRouter(Architecture)
//!   → A2AClient
//!   → Planner Agent (any capable agent)
//!   → WorkflowPlan (structure + dependencies only)
//!   → PlanValidator
//!   → persist + preview
//!   → explicit `plan execute`
//!   → WorkflowGraph → DagScheduler
//! ```
//!
//! Phase 18 adds the edit + policy + budget + atomic-claim layer between the
//! validator and the scheduler:
//!
//! ```text
//! Planner → revision 1 (source `planner`)
//! User    → `plan edit` → revision 2+ (source `user_edit`; the planner's
//!           revision 1 is never overwritten)
//! Schema Validation → DAG Validation → Policy Validation
//! `plan execute --check` → preview (no claim, no workflow)
//! `plan execute --yes`   → atomic claim → WorkflowGraph → DagScheduler
//! ```
//!
//! Invariant: the Planner proposes structure, the User may edit it, the
//! Validator checks correctness, the Policy checks allowed scope, the Budget
//! explains structural cost, the Router chooses agents, the Scheduler controls
//! concurrency, the Daemon owns runtime. The Planner/User can never bypass the
//! Policy; a plan is never trusted on `status = ready` alone — every path
//! re-parses and re-validates.

use std::sync::Arc;

use agentmesh_a2a::types::Message;
use agentmesh_core::TaskIntent;
use agentmesh_orchestrator::budget::PlanBudget;
use agentmesh_orchestrator::delegate::pick_agent;
use agentmesh_orchestrator::diff::PlanDiff;
use agentmesh_orchestrator::plan::{
    PlannerArtifact, WorkflowPlan, build_planner_prompt, parse_planner_output,
};
use agentmesh_orchestrator::policy::{PlanPolicy, PlanPolicyEngine};
use agentmesh_orchestrator::workflow::{
    MAX_PARALLEL_CAP, NoopObserver, StepOutcome, WorkflowOptions, stream_a2a_step,
};
use agentmesh_storage::{
    PlanClaimResult, WorkflowPlanRepository, WorkflowPlanRow, plan_revision_source, plan_status,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::protocol::{
    PlanDetail, PlanInfo, PlanNodeInfo, PlanPolicyUsage, PlanPreview, PlanRevisionInfo,
};
use crate::workflow_service::{WorkflowError, WorkflowService};

/// Lifecycle status of a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanStatus {
    Generating,
    Ready,
    Invalid,
    Failed,
    Executing,
    Executed,
}

impl PlanStatus {
    /// Stable snake_case string used for persistence and the wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanStatus::Generating => plan_status::GENERATING,
            PlanStatus::Ready => plan_status::READY,
            PlanStatus::Invalid => plan_status::INVALID,
            PlanStatus::Failed => plan_status::FAILED,
            PlanStatus::Executing => plan_status::EXECUTING,
            PlanStatus::Executed => plan_status::EXECUTED,
        }
    }

    /// Parse a stable [`Self::as_str`] value; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            plan_status::GENERATING => PlanStatus::Generating,
            plan_status::READY => PlanStatus::Ready,
            plan_status::INVALID => PlanStatus::Invalid,
            plan_status::FAILED => PlanStatus::Failed,
            plan_status::EXECUTING => PlanStatus::Executing,
            plan_status::EXECUTED => PlanStatus::Executed,
            _ => return None,
        })
    }
}

/// Errors produced by the plan service.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("plan `{0}` not found")]
    NotFound(Uuid),

    #[error("plan `{0}` is not ready (status `{1}`); only ready plans execute")]
    NotReady(Uuid, String),

    #[error("plan `{0}` has already been executed; a plan executes at most once")]
    AlreadyExecuted(Uuid),

    #[error("plan `{0}` execution is already in progress by another caller")]
    ExecutionInProgress(Uuid),

    #[error("plan `{0}` is not editable (status `{1}`); only ready or invalid plans can be edited")]
    NotEditable(Uuid, String),

    #[error("plan `{0}` violates policy: {1}")]
    PolicyViolation(Uuid, agentmesh_orchestrator::policy::PolicyViolation),

    #[error("agent directory is not initialized")]
    DirectoryUninitialized,

    #[error("invalid planner output: {0}")]
    InvalidPlannerOutput(String),

    #[error("invalid plan: {0}")]
    InvalidPlan(String),

    #[error("planner task failed: {0}")]
    PlannerTaskFailed(String),

    #[error("A2A error: {0}")]
    A2A(String),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("workflow error: {0}")]
    Workflow(#[from] WorkflowError),

    #[error("orchestrator error: {0}")]
    Orchestrator(#[from] agentmesh_orchestrator::OrchestratorError),
}

/// Daemon-owned AI planner: generate, edit, preview and atomically execute
/// plans under a deterministic policy.
pub struct PlanService {
    workflows: Arc<WorkflowService>,
    plans: WorkflowPlanRepository,
    policy: PlanPolicyEngine,
}

impl PlanService {
    /// A service under the safe default policy (used by tests).
    pub fn new(workflows: Arc<WorkflowService>, plans: WorkflowPlanRepository) -> Arc<Self> {
        Self::with_policy(workflows, plans, PlanPolicy::default())
    }

    /// A service under an explicit policy (the daemon builds it from config).
    pub fn with_policy(
        workflows: Arc<WorkflowService>,
        plans: WorkflowPlanRepository,
        policy: PlanPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            workflows,
            plans,
            policy: PlanPolicyEngine::new(policy),
        })
    }

    /// Generate a plan: route the goal to a planner agent over A2A, parse and
    /// validate the output, and persist the result. Returns the plan id.
    pub async fn create_plan(
        &self,
        goal: &str,
        agent_override: Option<&str>,
    ) -> Result<Uuid, PlanError> {
        let directory = self.workflows.directory()?;
        let plan_id = Uuid::new_v4();

        // Persist `generating` up front so a failure is never silent.
        self.plans
            .create(&WorkflowPlanRow {
                id: plan_id,
                goal: goal.to_string(),
                status: PlanStatus::Generating.as_str().to_string(),
                planner_agent_id: None,
                planner_task_id: None,
                plan_json: None,
                validation_error: None,
                workflow_id: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                updated_at: chrono::Utc::now().to_rfc3339(),
                executed_at: None,
                current_revision: None,
                execution_claimed_at: None,
                executed_revision: None,
            })
            .await?;

        // 1. Pick the planner agent: explicit override (still A2A) or routed
        //    by the Architecture intent. The Router decides WHO.
        let router = self.workflows.router();
        let delegation = pick_agent(
            &directory,
            &router,
            Some(TaskIntent::Architecture),
            agent_override.map(str::to_string),
        )?;
        let agent_id = delegation.agent_id.clone();

        // 2. Start the planner task over A2A and stream to a terminal state.
        let message = Message::user_text(build_planner_prompt(goal));
        let cancel = Notify::new();
        let streaming = match delegation.client.send_streaming_message(&message).await {
            Ok(streaming) => streaming,
            Err(err) => {
                let reason = format!("failed to start planner task: {err}");
                self.plans
                    .update_status(plan_id, PlanStatus::Failed.as_str(), Some(&reason))
                    .await?;
                return Err(PlanError::PlannerTaskFailed(reason));
            }
        };
        let task_id = streaming.task.id;
        let outcome = stream_a2a_step(
            &cancel,
            &agent_id,
            task_id,
            &delegation.client,
            streaming.events,
            &NoopObserver,
        )
        .await;

        match outcome {
            StepOutcome::Completed { summary, artifacts } => {
                let artifacts: Vec<PlannerArtifact> =
                    artifacts.iter().map(PlannerArtifact::from).collect();

                // 3. Parse the output (JSON artifact preferred; final-message
                //    JSON as fallback) and strictly validate it. A planning
                //    outcome that is unusable persists as `invalid` — it is a
                //    status, not a hard error, so the caller can inspect it.
                //
                //    When the output *parses* but fails validation, the JSON is
                //    still stored (as revision 1) so the user can read it and
                //    fix it via `plan edit` — never discarded.
                let plan = match parse_planner_output(summary.as_deref(), &artifacts) {
                    Ok(plan) => plan,
                    Err(err) => {
                        let reason = err.to_string();
                        self.plans
                            .update_status(plan_id, PlanStatus::Invalid.as_str(), Some(&reason))
                            .await?;
                        return Ok(plan_id);
                    }
                };
                let plan_json = serde_json::to_string(&plan)
                    .map_err(|err| PlanError::InvalidPlan(err.to_string()))?;
                if let Err(err) = plan.validate() {
                    let reason = err.to_string();
                    self.plans
                        .update_status(plan_id, PlanStatus::Invalid.as_str(), Some(&reason))
                        .await?;
                    self.plans
                        .add_revision(plan_id, 1, &plan_json, plan_revision_source::PLANNER)
                        .await?;
                    self.plans
                        .set_current_revision(plan_id, 1, &plan_json)
                        .await?;
                    return Ok(plan_id);
                }

                self.plans
                    .mark_ready(plan_id, &agent_id, task_id, &plan_json)
                    .await?;
                self.plans
                    .add_revision(plan_id, 1, &plan_json, plan_revision_source::PLANNER)
                    .await?;
                self.plans
                    .set_current_revision(plan_id, 1, &plan_json)
                    .await?;
                Ok(plan_id)
            }
            StepOutcome::Failed(message) => {
                self.plans
                    .update_status(plan_id, PlanStatus::Failed.as_str(), Some(&message))
                    .await?;
                Ok(plan_id)
            }
            StepOutcome::Cancelled => {
                let reason = "planner task was cancelled".to_string();
                self.plans
                    .update_status(plan_id, PlanStatus::Failed.as_str(), Some(&reason))
                    .await?;
                Ok(plan_id)
            }
        }
    }

    /// All plans, newest first.
    pub async fn list(&self) -> Result<Vec<PlanInfo>, PlanError> {
        let rows = self.plans.list().await?;
        Ok(rows.iter().map(plan_info).collect())
    }

    /// Full detail of one plan, including its preview nodes.
    pub async fn get(&self, plan_id: Uuid) -> Result<Option<PlanDetail>, PlanError> {
        let Some(row) = self.plans.get(plan_id).await? else {
            return Ok(None);
        };
        Ok(Some(self.detail(&row).await?))
    }

    /// The raw stored plan JSON, for diagnostics / audit (e.g. verifying a
    /// generated plan carries no agent/provider/control fields).
    pub async fn stored_plan_json(&self, plan_id: Uuid) -> Result<Option<String>, PlanError> {
        let row = self.plans.get(plan_id).await?;
        Ok(row.and_then(|r| r.plan_json))
    }

    /// Recover plans stuck in `executing` after a daemon crash (Phase 19 §1).
    /// Called by a fresh daemon holding the scope lock. Returns
    /// `(failed, corrected_to_executed)`.
    pub async fn recover_stale_executing(&self) -> Result<(usize, usize), PlanError> {
        let reason = "AgentMesh daemon terminated during plan execution setup.";
        Ok(self.plans.recover_stale_executing(reason).await?)
    }

    /// Replace the current revision with a user-edited plan (Phase 18).
    ///
    /// Only `ready` or `invalid` plans are editable. The user JSON is treated
    /// as untrusted exactly like planner JSON: strict parse (the same closed
    /// [`WorkflowPlan`] schema — no second editable DTO), full validation
    /// (including the DAG cycle/missing-dependency check) and then the policy.
    /// A saved edit always validates, so the plan returns to `ready`; a broken
    /// edit is rejected without touching the stored revision.
    ///
    /// Returns the new revision number.
    pub async fn edit(&self, plan_id: Uuid, plan_json: &str) -> Result<i64, PlanError> {
        let row = self
            .plans
            .get(plan_id)
            .await?
            .ok_or(PlanError::NotFound(plan_id))?;
        if !matches!(
            row.status.as_str(),
            plan_status::READY | plan_status::INVALID
        ) {
            return Err(PlanError::NotEditable(plan_id, row.status));
        }
        let plan = WorkflowPlan::from_json(plan_json)
            .map_err(|err| PlanError::InvalidPlan(format!("user plan JSON invalid: {err}")))?;
        plan.validate()
            .map_err(|err| PlanError::InvalidPlan(format!("user plan invalid: {err}")))?;
        self.policy
            .check_plan(&plan)
            .map_err(|violation| PlanError::PolicyViolation(plan_id, violation))?;

        let next = row.current_revision.unwrap_or(0) + 1;
        self.plans
            .add_revision(plan_id, next, plan_json, plan_revision_source::USER_EDIT)
            .await?;
        self.plans
            .set_current_revision(plan_id, next, plan_json)
            .await?;
        self.plans
            .update_status(plan_id, plan_status::READY, None)
            .await?;
        Ok(next)
    }

    /// Structural execution preview (`plan execute --check`). Re-parses and
    /// re-validates, checks the policy, computes the budget — and never claims
    /// the plan or creates a workflow.
    pub async fn preview(
        &self,
        plan_id: Uuid,
        max_parallel: usize,
    ) -> Result<PlanPreview, PlanError> {
        let row = self
            .plans
            .get(plan_id)
            .await?
            .ok_or(PlanError::NotFound(plan_id))?;
        self.gate_executable(&row, plan_id)?;
        let (revision, plan, graph) = self.current_plan(&row, plan_id).await?;
        self.policy
            .check_plan(&plan)
            .map_err(|violation| PlanError::PolicyViolation(plan_id, violation))?;
        self.policy
            .check_parallel(max_parallel)
            .map_err(|violation| PlanError::PolicyViolation(plan_id, violation))?;

        let budget = PlanBudget::new(&plan, &graph, max_parallel);
        let policy = self.policy.policy();
        Ok(PlanPreview {
            plan_id,
            status: row.status.clone(),
            revision: Some(revision),
            node_count: budget.node_count,
            estimated_agent_calls: budget.estimated_agent_calls,
            evaluation_agent_calls: budget.evaluation_agent_calls,
            root_count: budget.root_count,
            terminal_count: budget.terminal_count,
            planning_calls: budget.planning_calls,
            max_parallel_requested: max_parallel,
            effective_max_parallel: effective_max_parallel(max_parallel, policy),
            policy: PlanPolicyUsage {
                max_nodes: policy.max_nodes,
                max_agent_calls: policy.max_agent_calls,
                max_parallel: policy.max_parallel,
            },
        })
    }

    /// Execute a plan (Phase 18): load latest revision → parse → validate →
    /// policy check → atomic claim → WorkflowGraph → persisted Workflow →
    /// DagScheduler. Exactly one concurrent caller wins the claim; the audit
    /// records which revision ran. `max_parallel` comes from the CLI/config,
    /// never from the plan. `source_workspace` (Phase 22 §4) is explicit
    /// execution input — it never lives in the plan JSON.
    pub async fn execute(
        &self,
        plan_id: Uuid,
        max_parallel: usize,
        source_workspace: Option<String>,
    ) -> Result<Uuid, PlanError> {
        let row = self
            .plans
            .get(plan_id)
            .await?
            .ok_or(PlanError::NotFound(plan_id))?;
        // Never trust status=ready alone: re-parse and re-validate.
        let (revision, plan, graph) = self.current_plan(&row, plan_id).await?;
        self.policy
            .check_plan(&plan)
            .map_err(|violation| PlanError::PolicyViolation(plan_id, violation))?;
        self.policy
            .check_parallel(max_parallel)
            .map_err(|violation| PlanError::PolicyViolation(plan_id, violation))?;

        // Atomic claim — the only authority on who may execute. No
        // `get → if ready → update` application-layer race.
        match self.plans.claim_execution(plan_id).await? {
            PlanClaimResult::Claimed => {}
            PlanClaimResult::AlreadyExecuted => return Err(PlanError::AlreadyExecuted(plan_id)),
            PlanClaimResult::ExecutionInProgress => {
                return Err(PlanError::ExecutionInProgress(plan_id));
            }
            PlanClaimResult::NotReady => {
                let status = self
                    .plans
                    .get(plan_id)
                    .await?
                    .map(|r| r.status)
                    .unwrap_or_default();
                return Err(PlanError::NotReady(plan_id, status));
            }
        }

        let options = WorkflowOptions {
            max_review_rounds: 0,
            max_parallel: effective_max_parallel(max_parallel, self.policy.policy()),
        };
        let workflow_id = match self
            .workflows
            .start_from_graph(&row.goal, graph, options, source_workspace)
            .await
        {
            Ok(id) => id,
            Err(err) => {
                // The claim is spent; surface the failure so the plan is
                // terminal and auditable instead of stuck `executing`.
                let reason = err.to_string();
                let _ = self
                    .plans
                    .update_status(plan_id, PlanStatus::Failed.as_str(), Some(&reason))
                    .await;
                return Err(PlanError::Workflow(err));
            }
        };
        self.plans
            .mark_executed_with_revision(plan_id, workflow_id, revision)
            .await?;
        Ok(workflow_id)
    }

    /// Structural diff between the original planner output (revision 1) and
    /// the current revision (Phase 18). `Ok(None)` when the plan has no
    /// revisions at all.
    pub async fn diff(&self, plan_id: Uuid) -> Result<Option<PlanDiff>, PlanError> {
        let _ = self
            .plans
            .get(plan_id)
            .await?
            .ok_or(PlanError::NotFound(plan_id))?;
        let Some(planner) = self.plans.planner_revision(plan_id).await? else {
            return Ok(None);
        };
        let Some(current) = self.plans.latest_revision(plan_id).await? else {
            return Ok(None);
        };
        let before: WorkflowPlan = serde_json::from_str(&planner.plan_json)
            .map_err(|err| PlanError::InvalidPlan(format!("planner revision corrupt: {err}")))?;
        let after: WorkflowPlan = serde_json::from_str(&current.plan_json)
            .map_err(|err| PlanError::InvalidPlan(format!("current revision corrupt: {err}")))?;
        Ok(Some(PlanDiff::new(&before, &after)))
    }

    /// Revision history, oldest first (Phase 18).
    pub async fn revisions(&self, plan_id: Uuid) -> Result<Vec<PlanRevisionInfo>, PlanError> {
        let rows = self.plans.list_revisions(plan_id).await?;
        Ok(rows
            .into_iter()
            .map(|r| PlanRevisionInfo {
                revision: r.revision,
                source: r.source,
                created_at: r.created_at,
            })
            .collect())
    }

    /// Reject a plan that cannot be previewed/executed, using the row's own
    /// status. The authoritative gate for *execution* is still the atomic
    /// claim — this only produces the right error early.
    fn gate_executable(&self, row: &WorkflowPlanRow, plan_id: Uuid) -> Result<(), PlanError> {
        match row.status.as_str() {
            plan_status::READY => Ok(()),
            plan_status::EXECUTED => Err(PlanError::AlreadyExecuted(plan_id)),
            plan_status::EXECUTING => Err(PlanError::ExecutionInProgress(plan_id)),
            other => Err(PlanError::NotReady(plan_id, other.to_string())),
        }
    }

    /// The latest revision's parsed + validated plan. Falls back to the plan
    /// row's own JSON for pre-revision rows (e.g. plans inserted before the
    /// Phase 18 migration); the fallback is still re-validated, never trusted.
    async fn current_plan(
        &self,
        row: &WorkflowPlanRow,
        plan_id: Uuid,
    ) -> Result<(i64, WorkflowPlan, agentmesh_orchestrator::WorkflowGraph), PlanError> {
        let (revision, plan_json) = match self.plans.latest_revision(plan_id).await? {
            Some(revision) => (revision.revision, revision.plan_json),
            None => {
                let json = row
                    .plan_json
                    .clone()
                    .ok_or_else(|| PlanError::InvalidPlan("plan has no stored JSON".to_string()))?;
                (row.current_revision.unwrap_or(1), json)
            }
        };
        let plan: WorkflowPlan = serde_json::from_str(&plan_json)
            .map_err(|err| PlanError::InvalidPlan(format!("stored plan JSON corrupt: {err}")))?;
        let graph = plan
            .validate()
            .map_err(|err| PlanError::InvalidPlan(err.to_string()))?;
        Ok((revision, plan, graph))
    }

    async fn detail(&self, row: &WorkflowPlanRow) -> Result<PlanDetail, PlanError> {
        // Re-validate on read (spec §13): a stored plan is never trusted on
        // its row status alone. Reads never mutate the row.
        let mut status = row.status.clone();
        let mut validation_error = row.validation_error.clone();
        let (summary, nodes) = match &row.plan_json {
            Some(json) => match serde_json::from_str::<WorkflowPlan>(json) {
                Ok(plan) => {
                    let nodes = plan
                        .nodes
                        .iter()
                        .map(|n| PlanNodeInfo {
                            id: n.id.clone(),
                            role: n.role.clone(),
                            intent: n.intent.clone(),
                            objective: n.objective.clone(),
                            depends_on: n.depends_on.clone(),
                        })
                        .collect();
                    match plan.validate() {
                        Ok(_) => (Some(plan.summary.clone()), nodes),
                        Err(err) => {
                            status = plan_status::INVALID.to_string();
                            validation_error = Some(err.to_string());
                            (Some(plan.summary.clone()), nodes)
                        }
                    }
                }
                Err(_) => {
                    status = plan_status::INVALID.to_string();
                    validation_error = Some("stored plan JSON is corrupt".to_string());
                    (None, Vec::new())
                }
            },
            None => (None, Vec::new()),
        };
        Ok(PlanDetail {
            plan_id: row.id,
            goal: row.goal.clone(),
            status,
            summary,
            nodes,
            planner_agent_id: row.planner_agent_id.clone(),
            planner_task_id: row.planner_task_id,
            workflow_id: row.workflow_id,
            validation_error,
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            executed_at: row.executed_at.clone(),
            current_revision: row.current_revision,
            executed_revision: row.executed_revision,
            plan_json: row.plan_json.clone(),
        })
    }
}

/// The effective DAG parallelism for an execute: CLI request bounded by the
/// policy limit and the scheduler hard cap. An over-policy request is rejected
/// up front by [`PlanPolicyEngine::check_parallel`], never silently clamped.
fn effective_max_parallel(requested: usize, policy: &PlanPolicy) -> usize {
    requested.min(policy.max_parallel).min(MAX_PARALLEL_CAP)
}

fn plan_info(row: &WorkflowPlanRow) -> PlanInfo {
    PlanInfo {
        plan_id: row.id,
        goal: row.goal.clone(),
        status: row.status.clone(),
        planner_agent_id: row.planner_agent_id.clone(),
        planner_task_id: row.planner_task_id,
        workflow_id: row.workflow_id,
        validation_error: row.validation_error.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        executed_at: row.executed_at.clone(),
    }
}
