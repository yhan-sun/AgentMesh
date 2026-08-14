//! Failure recovery + bounded self-healing (Phase 20).
//!
//! A failed workflow stays Failed; it is never reopened. Recovery runs as a NEW
//! child workflow:
//!
//! ```text
//! Workflow A → node failed → Failed (history immutable)
//!   → Failure Analyzer (ordinary A2A agent, TaskIntent::Debug)
//!   → Recovery Proposal (a WorkflowPlan, validated + policy + budget)
//!   → user approves (`recovery execute --yes`)
//!   → atomic claim → Workflow B (child, same context)
//! ```
//!
//! The child reuses the parent's context, so existing agent sessions and
//! worktrees are reused (same agent) or isolated (different agent). Attempts
//! are bounded by `[recovery] max_attempts` (hard cap 2) and
//! `max_recovery_agent_calls` across the whole recovery chain; a proposal is
//! never auto-executed unless `auto_execute` is explicitly true.

use std::sync::Arc;

use agentmesh_a2a::types::Message;
use agentmesh_core::TaskIntent;
use agentmesh_core::config::RecoveryConfig;
use agentmesh_orchestrator::budget::PlanBudget;
use agentmesh_orchestrator::delegate::pick_agent;
use agentmesh_orchestrator::plan::{WorkflowPlan, parse_planner_output};
use agentmesh_orchestrator::policy::{PlanPolicy, PlanPolicyEngine, PolicyViolation};
use agentmesh_orchestrator::workflow::{
    NoopObserver, StepOutcome, WorkflowOptions, stream_a2a_step,
};
use agentmesh_storage::{
    RecoveryClaimResult, WorkflowRecoveryRepository, WorkflowRecoveryRow, recovery_status,
};
use agentmesh_workspace::WorkspaceManager;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::workflow_service::{RecoveryInputs, WorkflowError, WorkflowService};

/// Bounded recovery policy (from `[recovery]` config, defaults safe).
#[derive(Debug, Clone)]
pub struct RecoveryPolicy {
    pub max_attempts: usize,
    pub auto_generate: bool,
    pub auto_execute: bool,
    pub max_recovery_agent_calls: usize,
}

/// Hard cap on recovery attempts (Phase 20 §12): no unbounded self-healing.
pub const MAX_ATTEMPTS_CAP: usize = 2;

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            auto_generate: true,
            auto_execute: false,
            max_recovery_agent_calls: 6,
        }
    }
}

impl RecoveryPolicy {
    /// Build from `[recovery]`; absent fields use defaults; max_attempts is
    /// hard-capped at [`MAX_ATTEMPTS_CAP`].
    pub fn from_config(config: &RecoveryConfig) -> Self {
        let default = Self::default();
        Self {
            max_attempts: config
                .max_attempts
                .unwrap_or(default.max_attempts)
                .min(MAX_ATTEMPTS_CAP),
            auto_generate: config.auto_generate.unwrap_or(default.auto_generate),
            auto_execute: config.auto_execute.unwrap_or(default.auto_execute),
            max_recovery_agent_calls: config
                .max_recovery_agent_calls
                .unwrap_or(default.max_recovery_agent_calls),
        }
    }
}

/// Errors produced by the recovery service.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("recovery `{0}` not found")]
    NotFound(Uuid),

    #[error("workflow `{0}` not found")]
    WorkflowNotFound(Uuid),

    #[error("workflow `{0}` cannot be recovered (status `{1}`); only failed workflows recover")]
    WorkflowNotFailed(Uuid, String),

    #[error("workflow `{0}` has no failed node to recover")]
    NoFailedNode(Uuid),

    #[error(
        "recovery limit reached for workflow `{workflow_id}` (attempt {attempt} > max {max_attempts})"
    )]
    RecoveryLimitReached {
        workflow_id: Uuid,
        attempt: usize,
        max_attempts: usize,
    },

    #[error(
        "recovery agent-call budget exceeded for workflow `{workflow_id}` ({used} used + {requested} > max {max})"
    )]
    RecoveryBudgetExceeded {
        workflow_id: Uuid,
        used: usize,
        requested: usize,
        max: usize,
    },

    #[error("recovery `{0}` is not ready (status `{1}`); only ready proposals execute")]
    NotReady(Uuid, String),

    #[error("recovery `{0}` has already been executed")]
    AlreadyExecuted(Uuid),

    #[error(
        "workflow `{workflow_id}` already has a pending recovery proposal `{recovery_id}`; run `recovery execute {recovery_id}` instead of generating a competing one"
    )]
    AlreadyPending {
        workflow_id: Uuid,
        recovery_id: Uuid,
    },

    #[error("recovery `{0}` execution is already in progress by another caller")]
    ExecutionInProgress(Uuid),

    #[error("agent directory is not initialized")]
    DirectoryUninitialized,

    #[error("failure analyzer task failed: {0}")]
    AnalyzerTaskFailed(String),

    #[error("invalid recovery plan: {0}")]
    InvalidPlan(String),

    #[error("recovery `{0}` violates policy: {1}")]
    PolicyViolation(Uuid, PolicyViolation),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("workflow error: {0}")]
    Workflow(#[from] WorkflowError),

    #[error("orchestrator error: {0}")]
    Orchestrator(#[from] agentmesh_orchestrator::OrchestratorError),
}

/// Daemon-owned failure recovery: analyze, propose, preview and atomically
/// execute recovery child workflows under bounded policy.
pub struct RecoveryService {
    workflows: Arc<WorkflowService>,
    recoveries: WorkflowRecoveryRepository,
    workspaces: Arc<WorkspaceManager>,
    plan_policy: PlanPolicyEngine,
    policy: RecoveryPolicy,
}

impl RecoveryService {
    pub fn new(
        workflows: Arc<WorkflowService>,
        recoveries: WorkflowRecoveryRepository,
        workspaces: Arc<WorkspaceManager>,
    ) -> Arc<Self> {
        Self::with_policy(
            workflows,
            recoveries,
            workspaces,
            PlanPolicy::default(),
            RecoveryPolicy::default(),
        )
    }

    pub fn with_policy(
        workflows: Arc<WorkflowService>,
        recoveries: WorkflowRecoveryRepository,
        workspaces: Arc<WorkspaceManager>,
        plan_policy: PlanPolicy,
        policy: RecoveryPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            workflows,
            recoveries,
            workspaces,
            plan_policy: PlanPolicyEngine::new(plan_policy),
            policy,
        })
    }

    /// The recovery policy (for the daemon's auto-generate decision).
    pub fn policy(&self) -> &RecoveryPolicy {
        &self.policy
    }

    /// Generate a recovery proposal for a failed workflow (Phase 20 §8). The
    /// failed workflow stays Failed; nothing executes. Returns the recovery id.
    pub async fn propose(
        &self,
        workflow_id: Uuid,
        agent_override: Option<&str>,
    ) -> Result<Uuid, RecoveryError> {
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(RecoveryError::WorkflowNotFound(workflow_id))?;
        if row.status != agentmesh_orchestrator::WorkflowStatus::Failed {
            return Err(RecoveryError::WorkflowNotFailed(
                workflow_id,
                row.status.as_str().to_string(),
            ));
        }
        let inputs = self
            .workflows
            .recovery_inputs(workflow_id)
            .await?
            .ok_or(RecoveryError::NoFailedNode(workflow_id))?;

        // Phase 21 §1: never generate a competing proposal — if a proposal is
        // already generating / ready / executing, return it instead.
        for existing in self.recoveries.list_for(workflow_id).await? {
            if matches!(
                existing.status.as_str(),
                recovery_status::GENERATING | recovery_status::READY | recovery_status::EXECUTING
            ) {
                return Err(RecoveryError::AlreadyPending {
                    workflow_id,
                    recovery_id: existing.id,
                });
            }
        }

        // Attempt budget: one attempt per executed recovery child.
        let attempt = (self.workflows.recovery_child_count(workflow_id).await? + 1) as usize;
        if attempt > self.policy.max_attempts {
            return Err(RecoveryError::RecoveryLimitReached {
                workflow_id,
                attempt,
                max_attempts: self.policy.max_attempts,
            });
        }

        let recovery_id = Uuid::new_v4();
        self.recoveries
            .create(&WorkflowRecoveryRow {
                id: recovery_id,
                workflow_id,
                failed_node_id: inputs.failed_node_id.clone(),
                status: recovery_status::GENERATING.to_string(),
                planner_agent_id: None,
                planner_task_id: None,
                plan_json: None,
                validation_error: None,
                recovery_workflow_id: None,
                attempt: attempt as i64,
                created_at: chrono::Utc::now().to_rfc3339(),
                executed_at: None,
            })
            .await?;

        // Run the Failure Analyzer over A2A (TaskIntent::Debug), never an
        // adapter directly.
        let directory = self.workflows.directory()?;
        let router = self.workflows.router();
        let delegation = pick_agent(
            &directory,
            &router,
            Some(TaskIntent::Debug),
            agent_override.map(str::to_string),
        )?;
        let agent_id = delegation.agent_id.clone();

        let diff = match self.workspace_diff(inputs.failed_session_id).await {
            Ok(diff) => diff,
            Err(err) => {
                tracing::warn!(workflow_id = %workflow_id, error = %err, "recovery analyzer: no workspace diff");
                None
            }
        };
        let prompt = build_recovery_prompt(&inputs, diff.as_ref(), attempt);
        let message = Message::user_text(prompt);
        let cancel = Notify::new();
        let streaming = match delegation.client.send_streaming_message(&message).await {
            Ok(streaming) => streaming,
            Err(err) => {
                let reason = format!("failed to start failure analyzer task: {err}");
                self.recoveries
                    .update_status(recovery_id, recovery_status::FAILED, Some(&reason))
                    .await?;
                return Err(RecoveryError::AnalyzerTaskFailed(reason));
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
                let artifacts: Vec<_> = artifacts.iter().map(Into::into).collect();
                let plan = match parse_planner_output(summary.as_deref(), &artifacts) {
                    Ok(plan) => plan,
                    Err(err) => {
                        let reason = err.to_string();
                        self.recoveries
                            .update_status(recovery_id, recovery_status::INVALID, Some(&reason))
                            .await?;
                        return Ok(recovery_id);
                    }
                };
                let plan_json = match serde_json::to_string(&plan) {
                    Ok(json) => json,
                    Err(err) => {
                        let reason = err.to_string();
                        self.recoveries
                            .update_status(recovery_id, recovery_status::INVALID, Some(&reason))
                            .await?;
                        return Ok(recovery_id);
                    }
                };
                // Validator → Policy → Budget (the recovery agent proposes;
                // limits are local code).
                if let Err(err) = self.validate_recovery_plan(workflow_id, &plan).await {
                    let reason = err.to_string();
                    self.recoveries
                        .update_status(recovery_id, recovery_status::INVALID, Some(&reason))
                        .await?;
                    return Ok(recovery_id);
                }
                self.recoveries
                    .set_ready(recovery_id, &agent_id, task_id, &plan_json)
                    .await?;
                Ok(recovery_id)
            }
            StepOutcome::Failed(message) => {
                self.recoveries
                    .update_status(recovery_id, recovery_status::FAILED, Some(&message))
                    .await?;
                Ok(recovery_id)
            }
            StepOutcome::Cancelled => {
                let reason = "failure analyzer task was cancelled".to_string();
                self.recoveries
                    .update_status(recovery_id, recovery_status::FAILED, Some(&reason))
                    .await?;
                Ok(recovery_id)
            }
        }
    }

    /// One proposal.
    pub async fn get(
        &self,
        recovery_id: Uuid,
    ) -> Result<Option<WorkflowRecoveryRow>, RecoveryError> {
        Ok(self.recoveries.get(recovery_id).await?)
    }

    /// All proposals of a workflow, newest first.
    pub async fn list_for(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowRecoveryRow>, RecoveryError> {
        Ok(self.recoveries.list_for(workflow_id).await?)
    }

    /// All proposals, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowRecoveryRow>, RecoveryError> {
        Ok(self.recoveries.list().await?)
    }

    /// Re-parse + re-validate a proposal's plan (`recovery execute --check`).
    /// Never mutates anything. Returns the validated plan + graph.
    pub async fn preview(
        &self,
        recovery_id: Uuid,
    ) -> Result<(WorkflowPlan, agentmesh_orchestrator::WorkflowGraph), RecoveryError> {
        let row = self
            .recoveries
            .get(recovery_id)
            .await?
            .ok_or(RecoveryError::NotFound(recovery_id))?;
        if row.status != recovery_status::READY {
            return Err(RecoveryError::NotReady(recovery_id, row.status));
        }
        self.validated_plan(&row).await
    }

    /// Execute a recovery proposal: re-validate → policy → budget → attempt
    /// limit → atomic claim → create the child workflow → mark executed.
    /// Returns the child workflow id.
    pub async fn execute(&self, recovery_id: Uuid) -> Result<Uuid, RecoveryError> {
        let row = self
            .recoveries
            .get(recovery_id)
            .await?
            .ok_or(RecoveryError::NotFound(recovery_id))?;
        match row.status.as_str() {
            recovery_status::READY => {}
            recovery_status::EXECUTED => return Err(RecoveryError::AlreadyExecuted(recovery_id)),
            recovery_status::EXECUTING => {
                return Err(RecoveryError::ExecutionInProgress(recovery_id));
            }
            other => return Err(RecoveryError::NotReady(recovery_id, other.to_string())),
        }

        // Re-validate + policy + budget against the current state.
        let (plan, graph) = self.validated_plan(&row).await?;
        self.plan_policy
            .check_plan(&plan)
            .map_err(|violation| RecoveryError::PolicyViolation(recovery_id, violation))?;
        let used = self.recovery_calls_used(row.workflow_id).await?;
        let requested = plan.nodes.len();
        if used + requested > self.policy.max_recovery_agent_calls {
            return Err(RecoveryError::RecoveryBudgetExceeded {
                workflow_id: row.workflow_id,
                used,
                requested,
                max: self.policy.max_recovery_agent_calls,
            });
        }

        // Attempt limit (defensive; `propose` already gated generation).
        if row.attempt as usize > self.policy.max_attempts {
            let _ = self
                .recoveries
                .update_status(
                    recovery_id,
                    recovery_status::REJECTED,
                    Some("attempt limit reached"),
                )
                .await;
            return Err(RecoveryError::RecoveryLimitReached {
                workflow_id: row.workflow_id,
                attempt: row.attempt as usize,
                max_attempts: self.policy.max_attempts,
            });
        }

        // Atomic claim — exactly one concurrent `--yes` creates the child.
        match self.recoveries.claim_execute(recovery_id).await? {
            RecoveryClaimResult::Claimed => {}
            RecoveryClaimResult::AlreadyExecuted => {
                return Err(RecoveryError::AlreadyExecuted(recovery_id));
            }
            RecoveryClaimResult::ExecutionInProgress => {
                return Err(RecoveryError::ExecutionInProgress(recovery_id));
            }
            RecoveryClaimResult::NotReady => {
                let status = self
                    .recoveries
                    .get(recovery_id)
                    .await?
                    .map(|r| r.status)
                    .unwrap_or_default();
                return Err(RecoveryError::NotReady(recovery_id, status));
            }
            RecoveryClaimResult::RecoveryLimitReached => {
                return Err(RecoveryError::RecoveryLimitReached {
                    workflow_id: row.workflow_id,
                    attempt: row.attempt as usize,
                    max_attempts: self.policy.max_attempts,
                });
            }
        }

        // Create the child workflow reusing the parent's context.
        let inputs = self
            .workflows
            .recovery_inputs(row.workflow_id)
            .await?
            .ok_or(RecoveryError::NoFailedNode(row.workflow_id))?;
        let goal = build_recovery_goal(&inputs, row.failed_node_id.clone(), row.attempt as usize);
        let options = WorkflowOptions {
            max_review_rounds: 0,
            max_parallel: agentmesh_orchestrator::workflow::DEFAULT_MAX_PARALLEL,
        };
        let child_id = self
            .workflows
            .start_recovery_workflow(
                &goal,
                graph,
                options,
                row.workflow_id,
                &row.failed_node_id,
                row.attempt,
            )
            .await?;
        // Broadcast the recovery lifecycle on the child's stream (Phase 20 §23).
        self.workflows
            .broadcast_to_workflow(
                child_id,
                crate::protocol::WorkflowStreamEvent::RecoveryStarted {
                    workflow_id: child_id,
                    recovery_workflow_id: child_id,
                    attempt: row.attempt as usize,
                },
            )
            .await;
        self.recoveries.mark_executed(recovery_id, child_id).await?;
        Ok(child_id)
    }

    /// The recovery agent-call budget already spent by this workflow's chain.
    async fn recovery_calls_used(&self, workflow_id: Uuid) -> Result<usize, RecoveryError> {
        let rows = self.recoveries.list_for(workflow_id).await?;
        let mut total = 0;
        for row in rows {
            if row.status == recovery_status::EXECUTED
                && let Some(json) = &row.plan_json
                && let Ok(plan) = serde_json::from_str::<WorkflowPlan>(json)
            {
                total += plan.nodes.len();
            }
        }
        Ok(total)
    }

    /// The full execution preview (`recovery execute --check`): the validated
    /// plan, its budget and the chain budget. Never mutates anything.
    pub async fn preview_detail(
        &self,
        recovery_id: Uuid,
    ) -> Result<crate::protocol::RecoveryPreview, RecoveryError> {
        let row = self
            .recoveries
            .get(recovery_id)
            .await?
            .ok_or(RecoveryError::NotFound(recovery_id))?;
        if row.status != recovery_status::READY {
            return Err(RecoveryError::NotReady(recovery_id, row.status));
        }
        let (plan, graph) = self.validated_plan(&row).await?;
        let budget = recovery_budget(&plan, &graph);
        let plan_policy = self.plan_policy.policy();
        let used = self.recovery_calls_used(row.workflow_id).await?;
        Ok(crate::protocol::RecoveryPreview {
            recovery_id: row.id,
            workflow_id: row.workflow_id,
            status: row.status,
            failed_node_id: row.failed_node_id,
            attempt: row.attempt,
            node_count: budget.node_count,
            estimated_agent_calls: budget.estimated_agent_calls,
            evaluation_agent_calls: budget.evaluation_agent_calls,
            policy_max_nodes: plan_policy.max_nodes,
            policy_max_agent_calls: plan_policy.max_agent_calls,
            chain_calls_used: used,
            chain_calls_max: self.policy.max_recovery_agent_calls,
        })
    }

    /// Recover proposals stuck mid-flight after a daemon crash (Phase 20 §20):
    /// `generating` → `failed` (analyzer died), `executing` + no child →
    /// retryable `ready`, `executing` + child → `executed`.
    pub async fn recover_stale_executing(&self) -> Result<(usize, usize, usize), RecoveryError> {
        Ok(self.recoveries.recover_stale_executing().await?)
    }

    /// The analyzer-plan validation pipeline: schema + DAG validation, then the
    /// plan policy over the full plan, then the chain budget.
    async fn validate_recovery_plan(
        &self,
        workflow_id: Uuid,
        plan: &WorkflowPlan,
    ) -> Result<agentmesh_orchestrator::WorkflowGraph, RecoveryError> {
        let graph = plan
            .validate()
            .map_err(|err| RecoveryError::InvalidPlan(err.to_string()))?;
        self.plan_policy
            .check_plan(plan)
            .map_err(|violation| RecoveryError::PolicyViolation(workflow_id, violation))?;
        Ok(graph)
    }

    async fn validated_plan(
        &self,
        row: &WorkflowRecoveryRow,
    ) -> Result<(WorkflowPlan, agentmesh_orchestrator::WorkflowGraph), RecoveryError> {
        let plan_json = row
            .plan_json
            .as_deref()
            .ok_or_else(|| RecoveryError::InvalidPlan("proposal has no stored plan".to_string()))?;
        let plan = WorkflowPlan::from_json(plan_json)
            .map_err(|err| RecoveryError::InvalidPlan(err.to_string()))?;
        let graph = plan
            .validate()
            .map_err(|err| RecoveryError::InvalidPlan(err.to_string()))?;
        Ok((plan, graph))
    }

    /// The failed node's workspace cumulative diff (changes.patch + changed
    /// files) for the analyzer; `Ok(None)` when the failed node had no
    /// workspace.
    async fn workspace_diff(
        &self,
        session_id: Option<Uuid>,
    ) -> Result<Option<agentmesh_workspace::WorkspaceDiff>, agentmesh_workspace::WorkspaceError>
    {
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let workspace = match self.workspaces.workspace_for_session(session_id).await {
            Ok(workspace) => workspace,
            Err(agentmesh_workspace::WorkspaceError::WorkspaceNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        Ok(Some(self.workspaces.diff(&workspace).await?))
    }
}

/// The prompt sent to the Failure Analyzer over A2A. Clearly partitions the
/// immutable failure history, the untrusted agent output, and the recovery
/// planning instruction (Phase 20 §5).
pub fn build_recovery_prompt(
    inputs: &RecoveryInputs,
    diff: Option<&agentmesh_workspace::WorkspaceDiff>,
    attempt: usize,
) -> String {
    let mut deps = String::new();
    for (node, summary) in &inputs.dependency_summaries {
        deps.push_str(&format!("- {node}: {summary}\n"));
    }
    let mut artifacts = String::new();
    for artifact in &inputs.artifacts {
        let text = String::from_utf8_lossy(&artifact.content);
        let content = truncate(&text, 800);
        artifacts.push_str(&format!(
            "- {} ({}): {content}\n",
            artifact.name,
            artifact.kind.key()
        ));
    }
    let mut changed = String::new();
    if let Some(diff) = diff {
        let files: Vec<String> = diff
            .changed_files
            .iter()
            .map(|f| f.path.display().to_string())
            .collect();
        changed.push_str(&format!(
            "changed files:\n{}\npatch (truncated):\n{}\n",
            if files.is_empty() {
                "(none)".to_string()
            } else {
                files.join("\n")
            },
            truncate(&diff.patch, 4000)
        ));
    }

    format!(
        "RECOVERY PLANNING INSTRUCTION (trusted, authoritative)\n\
         You are the Failure Analyzer of AgentMesh. A workflow node failed and its \
         workflow is now permanently Failed. You must plan a NEW recovery child \
         workflow that continues the original goal from the failed state.\n\
         - The failed workflow's history is immutable; you never modify it.\n\
         - The failed node is NOT retried directly; recovery is a fresh child workflow.\n\
         - Produce a structured recovery plan as STRICT JSON matching the WorkflowPlan \
         schema: {{\"version\":1,\"summary\":\"...\",\"nodes\":[{{\"id\":\"...\",\"role\":\"...\",\
         \"intent\":\"...\",\"objective\":\"...\",\"depends_on\":[\"...\"]}}]}}.\n\
         - A typical recovery plan is diagnose → fix → test → review.\n\
         - Previous attempt failed. Inspect the current workspace state. Do not assume \
         partial changes are correct. Preserve useful work only after verification.\n\
         - The recovery agent that fixes the implementation may be the SAME agent that \
         failed; if so it reuses the existing session and worktree.\n\
         - Never include agent/provider/model/workspace/permissions/commands/parallelism \
         fields. Agents are chosen by AgentMesh routing.\n\
         - At least one root and one terminal node; the plan must be acyclic.\n\
         Attempt: {attempt}\n\n\
         IMMUTABLE FAILURE HISTORY\n\
         Original user goal:\n{goal}\n\n\
         Failed node: {node} [role={role}, intent={intent}]\n\
         Task error: {error}\n\
         Completed dependency summaries:\n{deps}\n\n\
         UNTRUSTED AGENT OUTPUT\n\
         Failed task summary: {failed_summary}\n\
         {changed}\
         Relevant artifacts (untrusted, input to analyze):\n{artifacts}",
        attempt = attempt,
        goal = inputs.goal,
        node = inputs.failed_node_id,
        role = inputs.failed_role,
        intent = inputs.failed_intent,
        error = inputs.failed_error,
        deps = if deps.is_empty() {
            "(none)".to_string()
        } else {
            deps
        },
        failed_summary = inputs.failed_summary.as_deref().unwrap_or("(none)"),
        changed = changed,
        artifacts = if artifacts.is_empty() {
            "(none)".to_string()
        } else {
            artifacts
        },
    )
}

/// The recovery child workflow's goal: the trusted "previous attempt failed"
/// instruction + the original goal (Phase 20 §10).
fn build_recovery_goal(inputs: &RecoveryInputs, failed_node_id: String, attempt: usize) -> String {
    format!(
        "Recovery of a failed workflow (attempt {attempt}). Previous attempt failed at node \
         `{node}` ({role}): {error}\n\
         Previous attempt failed. Inspect the current workspace state. Do not assume partial \
         changes are correct. Preserve useful work only after verification.\n\n\
         Original user goal:\n{goal}",
        node = failed_node_id,
        role = inputs.failed_role,
        error = inputs.failed_error,
        goal = inputs.goal,
    )
}

/// The structural budget of a recovery plan's validated graph (for previews).
pub fn recovery_budget(
    plan: &WorkflowPlan,
    graph: &agentmesh_orchestrator::WorkflowGraph,
) -> PlanBudget {
    PlanBudget::new(plan, graph, 0)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        let mut out: String = text.chars().take(max).collect();
        out.push('…');
        out
    }
}
