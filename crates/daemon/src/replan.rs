//! Runtime replanning service (Phase 19).
//!
//! A replan is user-triggered: the user runs
//! `agentmesh workflow replan <workflow> "request"`, the daemon asks an
//! ordinary A2A agent (routed by the Architecture intent) for a strict
//! [`WorkflowPlanDelta`], the proposal is validated + policy-checked + budgeted
//! and persisted. It never mutates the live workflow by itself: the user must
//! explicitly apply it (`replan apply --yes`), which re-validates, atomically
//! claims, and only then swaps the pending part of the graph.
//!
//! ```text
//! user request + current DAG + statuses + summaries
//!   → RuleRouter(Architecture) → A2A Planner → WorkflowPlanDelta
//!   → apply_delta(candidate) → Policy → Budget → persisted proposal
//!   → preview (no mutation)
//!   → explicit apply --yes
//!   → atomic claim (base graph_revision) → replace persisted graph → reload scheduler
//! ```

use std::sync::Arc;

use agentmesh_a2a::types::Message;
use agentmesh_core::TaskIntent;
use agentmesh_orchestrator::budget::PlanBudget;
use agentmesh_orchestrator::delegate::pick_agent;
use agentmesh_orchestrator::plan::PlannerArtifact;
use agentmesh_orchestrator::policy::{PlanPolicy, PlanPolicyEngine, PolicyViolation};
use agentmesh_orchestrator::replan::{
    ReplanError as DeltaError, WorkflowPlanDelta, apply_delta, build_replan_prompt,
};
use agentmesh_orchestrator::workflow::{NoopObserver, StepOutcome, stream_a2a_step};
use agentmesh_storage::{
    ReplanApplyResult, WorkflowReplanRepository, WorkflowReplanRow, replan_status,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::workflow_service::{WorkflowError, WorkflowService};

/// Errors produced by the replan service.
#[derive(Debug, thiserror::Error)]
pub enum ReplanError {
    #[error("replan `{0}` not found")]
    NotFound(Uuid),

    #[error("workflow `{0}` not found")]
    WorkflowNotFound(Uuid),

    #[error("workflow `{0}` has no DAG to replan")]
    NotDag(Uuid),

    #[error(
        "workflow `{0}` cannot be replanned (status `{1}`); only running or interrupted workflows replan"
    )]
    NotReplannable(Uuid, String),

    #[error("replan `{0}` is not ready (status `{1}`); only ready proposals apply")]
    NotReady(Uuid, String),

    #[error("replan `{0}` has already been applied")]
    AlreadyApplied(Uuid),

    #[error("replan `{0}` apply is already in progress by another caller")]
    ApplyInProgress(Uuid),

    #[error(
        "replan `{replan_id}` is stale (base graph revision {base} != current {current}); re-plan from the current graph"
    )]
    ReplanStale {
        replan_id: Uuid,
        base: i64,
        current: i64,
    },

    #[error("invalid replan delta: {0}")]
    InvalidDelta(String),

    #[error("replan candidate invalid: {0}")]
    InvalidCandidate(String),

    #[error("replan `{0}` violates policy: {1}")]
    PolicyViolation(Uuid, PolicyViolation),

    #[error("agent directory is not initialized")]
    DirectoryUninitialized,

    #[error("replan planner task failed: {0}")]
    PlannerTaskFailed(String),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("workflow error: {0}")]
    Workflow(#[from] WorkflowError),

    #[error("orchestrator error: {0}")]
    Orchestrator(#[from] agentmesh_orchestrator::OrchestratorError),
}

impl From<DeltaError> for ReplanError {
    fn from(err: DeltaError) -> Self {
        ReplanError::InvalidCandidate(err.to_string())
    }
}

/// Daemon-owned replan service: generate, preview and atomically apply
/// user-approved DAG deltas.
pub struct ReplanService {
    workflows: Arc<WorkflowService>,
    replans: WorkflowReplanRepository,
    policy: PlanPolicyEngine,
}

impl ReplanService {
    pub fn new(workflows: Arc<WorkflowService>, replans: WorkflowReplanRepository) -> Arc<Self> {
        Self::with_policy(workflows, replans, PlanPolicy::default())
    }

    pub fn with_policy(
        workflows: Arc<WorkflowService>,
        replans: WorkflowReplanRepository,
        policy: PlanPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            workflows,
            replans,
            policy: PlanPolicyEngine::new(policy),
        })
    }

    /// Generate a replan proposal: build the planner prompt from the current
    /// DAG + statuses + summaries + user request, route to a replan planner
    /// over A2A, validate the delta against the current graph, and persist.
    /// A proposal never mutates the workflow.
    pub async fn create_proposal(
        &self,
        workflow_id: Uuid,
        user_request: &str,
        agent_override: Option<&str>,
    ) -> Result<Uuid, ReplanError> {
        let current = self.replannable_current(workflow_id).await?;
        let replan_id = Uuid::new_v4();

        self.replans
            .create(&WorkflowReplanRow {
                id: replan_id,
                workflow_id,
                status: replan_status::GENERATING.to_string(),
                planner_agent_id: None,
                planner_task_id: None,
                delta_json: None,
                validation_error: None,
                base_graph_revision: current.graph_revision,
                applied_graph_revision: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                applied_at: None,
            })
            .await?;

        // 1. Pick the replan planner: explicit override or routed Architecture.
        let directory = self.workflows.directory()?;
        let router = self.workflows.router();
        let delegation = pick_agent(
            &directory,
            &router,
            Some(TaskIntent::Architecture),
            agent_override.map(str::to_string),
        )?;
        let agent_id = delegation.agent_id.clone();

        // 2. Ask the planner for a delta over A2A.
        let prompt = build_replan_prompt(
            &self
                .workflows
                .get(workflow_id)
                .await?
                .ok_or(ReplanError::WorkflowNotFound(workflow_id))?
                .goal,
            &current.graph,
            &current.statuses,
            &current.summaries,
            user_request,
        );
        let message = Message::user_text(prompt);
        let cancel = Notify::new();
        let streaming = match delegation.client.send_streaming_message(&message).await {
            Ok(streaming) => streaming,
            Err(err) => {
                let reason = format!("failed to start replan planner task: {err}");
                self.replans
                    .update_status(replan_id, replan_status::FAILED, Some(&reason))
                    .await?;
                return Err(ReplanError::PlannerTaskFailed(reason));
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
                let delta = match parse_replan_delta(summary.as_deref(), &artifacts) {
                    Ok(delta) => delta,
                    Err(err) => {
                        let reason = err.to_string();
                        self.replans
                            .update_status(replan_id, replan_status::INVALID, Some(&reason))
                            .await?;
                        return Ok(replan_id);
                    }
                };
                let delta_json = serde_json::to_string(&delta)
                    .map_err(|err| ReplanError::InvalidDelta(err.to_string()))?;

                // Validate the delta's candidate against the *current* graph +
                // statuses, then the policy. Any failure persists as `invalid`
                // and never touches the live workflow.
                match self.candidate_graph(workflow_id, &delta).await {
                    Ok(_) => {
                        self.replans
                            .set_ready(replan_id, &agent_id, task_id, &delta_json)
                            .await?;
                    }
                    Err(err) => {
                        let reason = err.to_string();
                        self.replans
                            .update_status(replan_id, replan_status::INVALID, Some(&reason))
                            .await?;
                    }
                }
                Ok(replan_id)
            }
            StepOutcome::Failed(message) => {
                self.replans
                    .update_status(replan_id, replan_status::FAILED, Some(&message))
                    .await?;
                Ok(replan_id)
            }
            StepOutcome::Cancelled => {
                let reason = "replan planner task was cancelled".to_string();
                self.replans
                    .update_status(replan_id, replan_status::FAILED, Some(&reason))
                    .await?;
                Ok(replan_id)
            }
        }
    }

    /// One proposal.
    pub async fn get(&self, replan_id: Uuid) -> Result<Option<WorkflowReplanRow>, ReplanError> {
        Ok(self.replans.get(replan_id).await?)
    }

    /// All proposals of a workflow, newest first.
    pub async fn list_for(&self, workflow_id: Uuid) -> Result<Vec<WorkflowReplanRow>, ReplanError> {
        Ok(self.replans.list_for(workflow_id).await?)
    }

    /// All proposals, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowReplanRow>, ReplanError> {
        Ok(self.replans.list().await?)
    }

    /// Re-parse + re-validate a proposal against the workflow's current
    /// persisted state (spec §11 `--check`). Never mutates anything. Returns
    /// the parsed delta and its validated candidate graph.
    pub async fn preview(
        &self,
        replan_id: Uuid,
    ) -> Result<(WorkflowPlanDelta, agentmesh_orchestrator::WorkflowGraph), ReplanError> {
        let row = self
            .replans
            .get(replan_id)
            .await?
            .ok_or(ReplanError::NotFound(replan_id))?;
        self.preview_row(&row).await
    }

    async fn preview_row(
        &self,
        row: &WorkflowReplanRow,
    ) -> Result<(WorkflowPlanDelta, agentmesh_orchestrator::WorkflowGraph), ReplanError> {
        if row.status != replan_status::READY {
            return Err(ReplanError::NotReady(row.id, row.status.clone()));
        }
        self.candidate_for(row).await
    }

    /// Apply a proposal: status gate → stale base check → re-parse → atomic
    /// claim (the only authority) → re-validate the candidate + policy against
    /// the now-stable persisted state → replace the persisted graph + bump
    /// graph_revision + hot-reload the live scheduler → mark applied. The
    /// candidate is never trusted from the proposal row; it is recomputed
    /// against the current persisted state. Returns the workflow's new revision.
    pub async fn apply(&self, replan_id: Uuid) -> Result<i64, ReplanError> {
        let row = self
            .replans
            .get(replan_id)
            .await?
            .ok_or(ReplanError::NotFound(replan_id))?;
        match row.status.as_str() {
            replan_status::READY => {}
            replan_status::APPLIED => return Err(ReplanError::AlreadyApplied(replan_id)),
            replan_status::APPLYING => return Err(ReplanError::ApplyInProgress(replan_id)),
            other => return Err(ReplanError::NotReady(replan_id, other.to_string())),
        }

        // Stale base check first: a proposal generated against an older graph
        // is rejected before its (now-stale) candidate is even considered.
        let current_revision = self.workflows.graph_revision(row.workflow_id).await?;
        if row.base_graph_revision != current_revision {
            // Conditional: a concurrent winner may have claimed or applied the
            // proposal between our read and this write — never overwrite it.
            let _ = self
                .replans
                .update_status_if(
                    replan_id,
                    replan_status::READY,
                    replan_status::REJECTED,
                    Some("stale base graph revision; re-plan from the current graph"),
                )
                .await;
            return Err(ReplanError::ReplanStale {
                replan_id,
                base: row.base_graph_revision,
                current: current_revision,
            });
        }

        // Re-parse the stored delta (not trusted from the row).
        let delta_json = row
            .delta_json
            .as_deref()
            .ok_or_else(|| ReplanError::InvalidDelta("proposal has no stored delta".to_string()))?;
        let delta = WorkflowPlanDelta::from_json(delta_json)
            .map_err(|err| ReplanError::InvalidDelta(err.to_string()))?;

        // Atomic claim — the only authority on who may apply. The candidate is
        // validated *after* the claim, never before: validating before the
        // claim would read the current graph in its own snapshot and race a
        // concurrent winner (a loser could observe a candidate that no longer
        // fits the graph the winner committed — misreported as invalid — or
        // worse, overwrite the winner's `applied` state). Holding the claim
        // guarantees no other apply is mutating the graph, so the candidate is
        // validated against a stable graph.
        match self.replans.claim_apply(replan_id, row.workflow_id).await? {
            ReplanApplyResult::Claimed => {}
            ReplanApplyResult::AlreadyApplied => {
                return Err(ReplanError::AlreadyApplied(replan_id));
            }
            ReplanApplyResult::ApplyInProgress => {
                return Err(ReplanError::ApplyInProgress(replan_id));
            }
            ReplanApplyResult::ReplanStale => {
                let current = self.workflows.graph_revision(row.workflow_id).await?;
                return Err(ReplanError::ReplanStale {
                    replan_id,
                    base: row.base_graph_revision,
                    current,
                });
            }
            ReplanApplyResult::NotReady => {
                let status = self
                    .replans
                    .get(replan_id)
                    .await?
                    .map(|r| r.status)
                    .unwrap_or_default();
                return Err(ReplanError::NotReady(replan_id, status));
            }
        }

        // Re-validate the delta's candidate against the current persisted state
        // (now stable: this caller holds the claim). A failure rolls the claim
        // back to `ready` so the proposal stays retryable and never hangs in
        // `applying`.
        let graph = match self.candidate_graph(row.workflow_id, &delta).await {
            Ok(graph) => graph,
            Err(err) => {
                let _ = self
                    .replans
                    .update_status(replan_id, replan_status::READY, None)
                    .await;
                return Err(err);
            }
        };

        // Claimed: apply the candidate graph — replace rows + deps, bump
        // graph_revision and mark applied in one transaction, then hot-reload
        // the scheduler (Phase 20 §2 P0).
        let new_revision = self
            .workflows
            .apply_replan_graph_atomic(replan_id, row.workflow_id, &graph)
            .await?;
        Ok(new_revision)
    }

    /// Recover replans stuck in `applying` after a daemon crash (Phase 20 §2):
    /// `ready` (graph_revision == base), `applied` (advanced), or `failed`
    /// (unprovable). Returns `(ready, applied, failed)`.
    pub async fn recover_stale_applying(&self) -> Result<(usize, usize, usize), ReplanError> {
        Ok(self.replans.recover_stale_applying().await?)
    }

    /// The full execution preview (`replan apply --check`): the parsed delta,
    /// its validated candidate and the budget + policy usage, with the
    /// graph-revision gate. Never mutates anything.
    pub async fn preview_detail(
        &self,
        replan_id: Uuid,
    ) -> Result<crate::protocol::ReplanPreview, ReplanError> {
        let row = self
            .replans
            .get(replan_id)
            .await?
            .ok_or(ReplanError::NotFound(replan_id))?;
        let (delta, graph) = self.preview_row(&row).await?;
        let current_revision = self.workflows.graph_revision(row.workflow_id).await?;
        let budget = PlanBudget::from_graph(&graph, 0);
        let policy = self.policy.policy();
        Ok(crate::protocol::ReplanPreview {
            replan_id: row.id,
            workflow_id: row.workflow_id,
            status: row.status.clone(),
            base_graph_revision: row.base_graph_revision,
            current_graph_revision: current_revision,
            add_nodes: delta.add_nodes.iter().map(|n| n.id.clone()).collect(),
            update_nodes: delta.update_nodes.iter().map(|u| u.id.clone()).collect(),
            remove_nodes: delta.remove_nodes.clone(),
            node_count: budget.node_count,
            estimated_agent_calls: budget.estimated_agent_calls,
            evaluation_agent_calls: budget.evaluation_agent_calls,
            root_count: budget.root_count,
            terminal_count: budget.terminal_count,
            policy_max_nodes: policy.max_nodes,
            policy_max_agent_calls: policy.max_agent_calls,
        })
    }

    /// The workflow must be a DAG that is Running or Interrupted (Phase 19 §14).
    async fn replannable_current(
        &self,
        workflow_id: Uuid,
    ) -> Result<crate::workflow_service::CurrentDag, ReplanError> {
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(ReplanError::WorkflowNotFound(workflow_id))?;
        match row.status {
            agentmesh_orchestrator::WorkflowStatus::Running
            | agentmesh_orchestrator::WorkflowStatus::Interrupted => {}
            other => {
                return Err(ReplanError::NotReplannable(
                    workflow_id,
                    other.as_str().to_string(),
                ));
            }
        }
        let current = self
            .workflows
            .current_dag(workflow_id)
            .await?
            .ok_or(ReplanError::NotDag(workflow_id))?;
        Ok(current)
    }

    /// Validate a delta against the workflow's current persisted graph +
    /// statuses (immutable rules + cycle/missing-dep via `apply_delta`) and the
    /// policy over the full candidate.
    async fn candidate_graph(
        &self,
        workflow_id: Uuid,
        delta: &WorkflowPlanDelta,
    ) -> Result<agentmesh_orchestrator::WorkflowGraph, ReplanError> {
        let current = self
            .workflows
            .current_dag(workflow_id)
            .await?
            .ok_or(ReplanError::NotDag(workflow_id))?;
        let graph = apply_delta(&current.graph, &current.statuses, delta)?;
        self.policy
            .check_graph(&graph)
            .map_err(|violation| ReplanError::PolicyViolation(workflow_id, violation))?;
        Ok(graph)
    }

    /// Load the delta of a row and validate it against the current state.
    async fn candidate_for(
        &self,
        row: &WorkflowReplanRow,
    ) -> Result<(WorkflowPlanDelta, agentmesh_orchestrator::WorkflowGraph), ReplanError> {
        let delta_json = row
            .delta_json
            .as_deref()
            .ok_or_else(|| ReplanError::InvalidDelta("proposal has no stored delta".to_string()))?;
        let delta = WorkflowPlanDelta::from_json(delta_json)
            .map_err(|err| ReplanError::InvalidDelta(err.to_string()))?;
        let graph = self.candidate_graph(row.workflow_id, &delta).await?;
        Ok((delta, graph))
    }

    /// The structural budget of a proposal's candidate (for the preview).
    pub async fn budget(
        &self,
        replan_id: Uuid,
    ) -> Result<(PlanBudget, agentmesh_orchestrator::WorkflowGraph), ReplanError> {
        let row = self
            .replans
            .get(replan_id)
            .await?
            .ok_or(ReplanError::NotFound(replan_id))?;
        let (_, graph) = self.preview_row(&row).await?;
        Ok((PlanBudget::from_graph(&graph, 0), graph))
    }
}

/// Extract a [`WorkflowPlanDelta`] from the replan planner's final message +
/// artifacts (mirrors the Phase 17 plan parsing: JSON artifact preferred,
/// final-message JSON fallback, markdown fences rejected).
fn parse_replan_delta(
    summary: Option<&str>,
    artifacts: &[PlannerArtifact],
) -> Result<WorkflowPlanDelta, agentmesh_orchestrator::PlanParseError> {
    use agentmesh_core::ArtifactKind;
    let json_artifacts: Vec<&PlannerArtifact> = artifacts
        .iter()
        .filter(|a| a.kind == ArtifactKind::Json || a.name.to_ascii_lowercase().ends_with(".json"))
        .collect();
    if let Some(artifact) = json_artifacts.first() {
        let text = artifact.content.as_deref().ok_or_else(|| {
            agentmesh_orchestrator::PlanParseError::NoJsonOutput(format!(
                "json artifact `{}` has no inline content",
                artifact.name
            ))
        })?;
        if looks_markdown(text) {
            return Err(agentmesh_orchestrator::PlanParseError::MarkdownFenced);
        }
        return WorkflowPlanDelta::from_json(text);
    }
    if let Some(message) = summary {
        if looks_markdown(message) {
            return Err(agentmesh_orchestrator::PlanParseError::MarkdownFenced);
        }
        return WorkflowPlanDelta::from_json(message);
    }
    Err(agentmesh_orchestrator::PlanParseError::NoJsonOutput(
        "replan planner produced no JSON artifact and no final message".to_string(),
    ))
}

fn looks_markdown(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("```") || trimmed.starts_with('`')
}
