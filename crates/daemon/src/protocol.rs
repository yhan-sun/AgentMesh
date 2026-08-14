//! Daemon protocol types shared by server and client.

use agentmesh_core::AgentEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Protocol version between CLI and daemon.
pub const DAEMON_PROTOCOL_VERSION: u32 = 1;

/// Metadata written after a daemon binds successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonMeta {
    pub protocol_version: u32,
    pub instance_id: String,
    pub pid: u32,
    pub address: String,
    pub started_at: String,
}

/// Health endpoint payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub protocol_version: u32,
    pub instance_id: String,
    pub status: String,
}

/// Request to run a fresh task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub agent_id: String,
    pub prompt: String,
    /// Source project/repository location; `null` uses the daemon cwd scope.
    #[serde(default)]
    pub source_workspace: Option<String>,
}

/// Request to resume a previous task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeRequest {
    pub source_task_id: Uuid,
    pub prompt: String,
}

/// Task start response: the daemon now owns the live runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResponse {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub agent_session_id: Uuid,
    pub agent_id: String,
}

/// One live task as reported by the runtime endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTaskInfo {
    pub task_id: Uuid,
    pub agent_id: String,
    pub agent_session_id: Uuid,
    pub status: String,
}

/// Runtime endpoint payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeResponse {
    pub instance_id: String,
    pub live_tasks: Vec<LiveTaskInfo>,
}

/// Uniform error payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            error: ApiErrorBody {
                code: code.to_string(),
                message: message.into(),
                details: None,
            },
        }
    }
}

/// Events streamed to clients over SSE.
///
/// Vendor-neutral: adapters stay behind the AgentEvent boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonStreamEvent {
    /// Initial metadata, sent once when attaching.
    TaskInfo {
        task_id: Uuid,
        context_id: Uuid,
        agent_session_id: Uuid,
        agent_id: String,
    },
    /// A forwarded agent event.
    Agent { event: AgentEvent },
    /// Requested replay position is older than the buffer; continuing from
    /// the oldest available sequence.
    ReplayGap { oldest_available: u64 },
}

/// Shutdown request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResponse {
    pub cancelled_tasks: usize,
}

// ---------- Phase 12: workflow persistence ----------

use agentmesh_orchestrator::{ReviewVerdict, WorkflowRole, WorkflowStatus, WorkflowStepStatus};

// ---------- Phase 13: safe apply ----------

/// Request to plan (`check`) or execute an apply for a task or workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyRequest {
    /// `true` runs the preflight only and never modifies the source
    /// repository (the CLI `--check` path).
    pub check: bool,
}

/// Response of an apply endpoint: either the plan (preview) or the outcome
/// (execution).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApplyResponse {
    Plan {
        plan: agentmesh_apply::ApplyPlan,
    },
    Applied {
        outcome: agentmesh_apply::ApplyOutcome,
    },
}

// ---------- Phase 14: workspace lifecycle + apply history ----------

/// One apply row for the `agentmesh applies` history listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyInfo {
    pub apply_id: Uuid,
    pub task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub workspace_id: Uuid,
    pub source_repository: String,
    pub base_revision: String,
    pub status: String,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub workspace_snapshot_hash: Option<String>,
}

impl From<agentmesh_storage::ApplyRow> for ApplyInfo {
    fn from(row: agentmesh_storage::ApplyRow) -> Self {
        Self {
            apply_id: row.id,
            task_id: row.task_id,
            workflow_id: row.workflow_id,
            workspace_id: row.workspace_id,
            source_repository: row.source_repository.display().to_string(),
            base_revision: row.base_revision,
            status: row.status.as_str().to_string(),
            error: row.error,
            created_at: row.created_at,
            completed_at: row.completed_at,
            workspace_snapshot_hash: row.workspace_snapshot_hash,
        }
    }
}

/// One workspace for the `agentmesh workspaces` listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceInfo {
    pub id: Uuid,
    pub agent_id: String,
    pub state: String,
    pub repository: String,
    pub branch: String,
    pub base_revision: String,
    pub created_at: String,
}

/// Request to plan (`check`) or execute a cleanup for a task/workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupRequest {
    /// `true` runs the preflight only and never deletes anything.
    pub check: bool,
}

/// Response of a cleanup endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CleanupResponse {
    Plan {
        plan: agentmesh_workspace::CleanupPlan,
    },
    Plans {
        plans: Vec<agentmesh_workspace::CleanupPlan>,
    },
    Removed {
        outcome: agentmesh_workspace::CleanupOutcome,
    },
    RemovedAll {
        outcomes: Vec<agentmesh_workspace::CleanupOutcome>,
    },
}

/// Artifact prune request (`agentmesh artifacts prune`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneRequest {
    pub older_than_days: u64,
    pub check: bool,
}

/// Artifact prune response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResponse {
    /// Number of file-backed artifacts that qualified.
    pub candidates: usize,
    /// Number actually pruned (0 for a `--check` preview).
    pub pruned: usize,
}

/// Request to start a daemon-owned workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStartRequest {
    pub preset: String,
    pub goal: String,
    #[serde(default)]
    pub max_review_rounds: usize,
    /// Maximum concurrent DAG nodes (Phase 16); ignored for sequential presets.
    #[serde(default)]
    pub max_parallel: usize,
    /// The explicit source project/repository the workflow operates on
    /// (Phase 22); `None` keeps the legacy daemon-cwd behavior.
    #[serde(default)]
    pub source_workspace: Option<String>,
}

/// Response to starting a workflow: the daemon owns the background run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStartResponse {
    pub workflow_id: Uuid,
}

/// One workflow as reported when listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub workflow_id: Uuid,
    pub preset: String,
    pub goal: String,
    pub status: WorkflowStatus,
    pub context_id: Option<Uuid>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub error: Option<String>,
    /// Bumped on every successful replan apply (Phase 19).
    pub graph_revision: i64,
    /// The failed workflow this workflow recovers (Phase 20); `None` otherwise.
    pub parent_workflow_id: Option<Uuid>,
    /// The parent node whose failure this workflow recovers.
    pub recovery_of_node_id: Option<String>,
    /// Which recovery attempt this is for the parent (1 = first).
    pub recovery_attempt: i64,
    /// The explicit source project/repository (Phase 22); `None` = legacy.
    pub source_workspace: Option<String>,
}

/// One step as reported in a workflow detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStepInfo {
    pub ordinal: usize,
    /// The stable node slug for DAG workflows; `None` for sequential steps.
    pub node_id: Option<String>,
    pub role: WorkflowRole,
    pub status: WorkflowStepStatus,
    pub agent_id: Option<String>,
    pub task_id: Option<Uuid>,
    pub review_round: usize,
    pub summary: Option<String>,
    pub error: Option<String>,
}

/// Full detail of one workflow, including its steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDetail {
    pub workflow_id: Uuid,
    pub preset: String,
    pub goal: String,
    pub status: WorkflowStatus,
    pub context_id: Option<Uuid>,
    pub max_review_rounds: usize,
    /// Maximum concurrent DAG nodes (Phase 16).
    pub max_parallel: usize,
    pub final_review_verdict: Option<ReviewVerdict>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    /// Bumped on every successful replan apply (Phase 19).
    pub graph_revision: i64,
    /// The failed workflow this workflow recovers (Phase 20); `None` otherwise.
    pub parent_workflow_id: Option<Uuid>,
    /// The parent node whose failure this workflow recovers.
    pub recovery_of_node_id: Option<String>,
    /// Which recovery attempt this is for the parent (1 = first).
    pub recovery_attempt: i64,
    /// The explicit source project/repository (Phase 22); `None` = legacy.
    pub source_workspace: Option<String>,
    pub steps: Vec<WorkflowStepInfo>,
}

/// Events streamed to workflow attach/start clients over SSE.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowStreamEvent {
    WorkflowStarted {
        workflow_id: Uuid,
        preset: String,
        goal: String,
    },
    StepStarted {
        workflow_id: Uuid,
        ordinal: usize,
        role: WorkflowRole,
        agent_id: String,
    },
    StepCompleted {
        workflow_id: Uuid,
        ordinal: usize,
        role: WorkflowRole,
    },
    StepFailed {
        workflow_id: Uuid,
        ordinal: usize,
        role: WorkflowRole,
        error: String,
    },
    StepCancelled {
        workflow_id: Uuid,
        ordinal: usize,
        role: WorkflowRole,
    },
    StepSkipped {
        workflow_id: Uuid,
        ordinal: usize,
        role: WorkflowRole,
    },
    // ---------- Phase 16: DAG node events ----------
    NodeReady {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
    },
    NodeStarted {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
        agent_id: String,
    },
    NodeCompleted {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
    },
    NodeFailed {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
        error: String,
    },
    NodeSkipped {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
    },
    NodeCancelled {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
    },
    NodeInterrupted {
        workflow_id: Uuid,
        node_id: String,
        role: WorkflowRole,
    },
    AgentMessage {
        workflow_id: Uuid,
        agent_id: String,
        message: String,
    },
    WorkflowCompleted {
        workflow_id: Uuid,
        final_review_verdict: Option<ReviewVerdict>,
    },
    WorkflowFailed {
        workflow_id: Uuid,
        error: Option<String>,
    },
    WorkflowCancelled {
        workflow_id: Uuid,
    },
    WorkflowInterrupted {
        workflow_id: Uuid,
        reason: String,
    },
    // ---------- Phase 20: failure recovery events ----------
    RecoveryPlanningStarted {
        workflow_id: Uuid,
        failed_node_id: String,
    },
    RecoveryProposalReady {
        workflow_id: Uuid,
        recovery_id: Uuid,
        attempt: usize,
    },
    RecoveryStarted {
        workflow_id: Uuid,
        recovery_workflow_id: Uuid,
        attempt: usize,
    },
    RecoveryCompleted {
        workflow_id: Uuid,
        recovery_workflow_id: Uuid,
    },
    RecoveryFailed {
        workflow_id: Uuid,
        recovery_workflow_id: Uuid,
        error: Option<String>,
    },
    RecoveryLimitReached {
        workflow_id: Uuid,
    },
    // ---------- Phase 22/23: Evaluation & Best-of-N competition events ----------
    EvaluationSnapshotChanged {
        workflow_id: Uuid,
        node_id: String,
    },
    CandidateStarted {
        workflow_id: Uuid,
        candidate_id: String,
        agent_id: String,
    },
    CandidateCompleted {
        workflow_id: Uuid,
        candidate_id: String,
        snapshot_hash: Option<String>,
    },
    CandidateFailed {
        workflow_id: Uuid,
        candidate_id: String,
        error: String,
    },
    CandidateSnapshotChanged {
        workflow_id: Uuid,
        candidate_id: String,
    },
    CandidateConsensusReady {
        workflow_id: Uuid,
        candidate_id: String,
        outcome: String,
    },
    WinnerSelected {
        workflow_id: Uuid,
        candidate_id: String,
        agent_id: String,
    },
    NoAcceptableCandidate {
        workflow_id: Uuid,
    },
}

// ---------- Phase 23: Best-of-N competitions ----------

/// Detailed view of a competition group and its candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitionGroupInfo {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub source_workspace: Option<String>,
    pub base_revision: String,
    pub candidate_count: i64,
    pub status: String,
    pub winner_candidate_id: Option<String>,
    pub winner_task_id: Option<Uuid>,
    pub winner_workspace_id: Option<Uuid>,
    pub winner_snapshot_hash: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub candidates: Vec<CompetitionCandidateInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitionCandidateInfo {
    pub id: Uuid,
    pub group_id: Uuid,
    pub candidate_id: String,
    pub agent_id: String,
    pub session_lane: String,
    pub task_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub snapshot_hash: Option<String>,
    pub evaluation_group_id: Option<Uuid>,
    pub status: String,
    pub summary: Option<String>,
    pub patch_path: Option<String>,
    pub consensus: Option<String>,
    pub approved_count: Option<usize>,
    pub valid_count: Option<usize>,
    pub created_at: String,
    pub updated_at: String,
}

// ---------- Phase 19: runtime replanning ----------

/// Request to generate a replan proposal for a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanCreateRequest {
    pub prompt: String,
    /// Explicit replan planner agent id; bypasses routing but still goes
    /// through A2A.
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanCreateResponse {
    pub replan_id: Uuid,
}

/// One replan proposal as reported when listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanInfo {
    pub replan_id: Uuid,
    pub workflow_id: Uuid,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub validation_error: Option<String>,
    pub base_graph_revision: i64,
    pub applied_graph_revision: Option<i64>,
    pub created_at: String,
    pub applied_at: Option<String>,
}

impl From<agentmesh_storage::WorkflowReplanRow> for ReplanInfo {
    fn from(row: agentmesh_storage::WorkflowReplanRow) -> Self {
        Self {
            replan_id: row.id,
            workflow_id: row.workflow_id,
            status: row.status,
            planner_agent_id: row.planner_agent_id,
            planner_task_id: row.planner_task_id,
            validation_error: row.validation_error,
            base_graph_revision: row.base_graph_revision,
            applied_graph_revision: row.applied_graph_revision,
            created_at: row.created_at,
            applied_at: row.applied_at,
        }
    }
}

/// One replan proposal with its parsed delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanDetail {
    pub replan_id: Uuid,
    pub workflow_id: Uuid,
    pub status: String,
    pub summary: Option<String>,
    pub delta: Option<agentmesh_orchestrator::replan::WorkflowPlanDelta>,
    pub validation_error: Option<String>,
    pub base_graph_revision: i64,
    pub applied_graph_revision: Option<i64>,
    pub created_at: String,
    pub applied_at: Option<String>,
}

/// Request to apply (`check = false`) or preview (`check = true`) a proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanApplyRequest {
    /// Preview only; never claims the proposal or mutates the workflow.
    #[serde(default)]
    pub check: bool,
}

/// The `replan apply --check` preview: the delta's added/updated/removed
/// nodes, the resulting budget and the graph-revision gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplanPreview {
    pub replan_id: Uuid,
    pub workflow_id: Uuid,
    pub status: String,
    pub base_graph_revision: i64,
    pub current_graph_revision: i64,
    pub add_nodes: Vec<String>,
    pub update_nodes: Vec<String>,
    pub remove_nodes: Vec<String>,
    pub node_count: usize,
    pub estimated_agent_calls: usize,
    /// Evaluator agent calls across all consensus fix rounds (Phase 22 §15).
    pub evaluation_agent_calls: usize,
    pub root_count: usize,
    pub terminal_count: usize,
    pub policy_max_nodes: usize,
    pub policy_max_agent_calls: usize,
}

/// Response to a replan apply request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplanApplyResponse {
    Preview { preview: ReplanPreview },
    Applied { applied_graph_revision: i64 },
}

// ---------- Phase 20: failure recovery + lineage ----------

/// Request to generate a recovery proposal for a failed workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryCreateRequest {
    /// Explicit Failure Analyzer agent id; bypasses routing but still goes
    /// through A2A.
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryCreateResponse {
    pub recovery_id: Uuid,
}

/// One recovery proposal as reported when listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryInfo {
    pub recovery_id: Uuid,
    pub workflow_id: Uuid,
    pub failed_node_id: String,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub validation_error: Option<String>,
    pub recovery_workflow_id: Option<Uuid>,
    pub attempt: i64,
    pub created_at: String,
    pub executed_at: Option<String>,
}

impl From<agentmesh_storage::WorkflowRecoveryRow> for RecoveryInfo {
    fn from(row: agentmesh_storage::WorkflowRecoveryRow) -> Self {
        Self {
            recovery_id: row.id,
            workflow_id: row.workflow_id,
            failed_node_id: row.failed_node_id,
            status: row.status,
            planner_agent_id: row.planner_agent_id,
            validation_error: row.validation_error,
            recovery_workflow_id: row.recovery_workflow_id,
            attempt: row.attempt,
            created_at: row.created_at,
            executed_at: row.executed_at,
        }
    }
}

/// One recovery proposal with its parsed plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryDetail {
    pub recovery_id: Uuid,
    pub workflow_id: Uuid,
    pub failed_node_id: String,
    pub status: String,
    pub summary: Option<String>,
    pub plan: Option<agentmesh_orchestrator::plan::WorkflowPlan>,
    pub validation_error: Option<String>,
    pub recovery_workflow_id: Option<Uuid>,
    pub attempt: i64,
    pub created_at: String,
    pub executed_at: Option<String>,
}

/// Request to preview (`check = true`) or execute (`check = false`) a recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryApplyRequest {
    /// Preview only; never claims the proposal or creates a child workflow.
    #[serde(default)]
    pub check: bool,
}

/// The `recovery execute --check` preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryPreview {
    pub recovery_id: Uuid,
    pub workflow_id: Uuid,
    pub status: String,
    pub failed_node_id: String,
    pub attempt: i64,
    pub node_count: usize,
    pub estimated_agent_calls: usize,
    /// Evaluator agent calls across all consensus fix rounds (Phase 22 §15).
    pub evaluation_agent_calls: usize,
    pub policy_max_nodes: usize,
    pub policy_max_agent_calls: usize,
    pub chain_calls_used: usize,
    pub chain_calls_max: usize,
}

/// Response to a recovery apply request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecoveryApplyResponse {
    Preview { preview: RecoveryPreview },
    Executed { recovery_workflow_id: Uuid },
}

/// One node of a workflow lineage chain (Phase 20 §19).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    pub workflow_id: Uuid,
    pub preset: String,
    pub status: WorkflowStatus,
    pub parent_workflow_id: Option<Uuid>,
    pub recovery_of_node_id: Option<String>,
    pub recovery_attempt: i64,
    pub created_at: String,
}

/// A workflow's lineage: itself, its parent (if it is a recovery child) and its
/// recovery children.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowLineage {
    pub workflow_id: Uuid,
    pub parent: Option<Box<LineageNode>>,
    pub recovery_children: Vec<LineageNode>,
}

// ---------- Phase 21: multi-agent evaluation ----------

/// One evaluation group as reported when listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationInfo {
    pub group_id: Uuid,
    pub workflow_id: Uuid,
    pub source_task_id: Option<Uuid>,
    pub strategy: String,
    pub quorum: usize,
    pub status: String,
    pub consensus: Option<agentmesh_orchestrator::evaluation::ConsensusResult>,
    pub snapshot_hash: Option<String>,
    /// Which consensus fix round this group evaluates (Phase 22 §13): 0 = the
    /// initial evaluation, 1 = the bounded fix round.
    pub round: usize,
    pub created_at: String,
    pub completed_at: Option<String>,
}

impl From<agentmesh_storage::EvaluationGroupRow> for EvaluationInfo {
    fn from(row: agentmesh_storage::EvaluationGroupRow) -> Self {
        Self {
            group_id: row.id,
            workflow_id: row.workflow_id,
            source_task_id: row.source_task_id,
            strategy: row.strategy,
            quorum: row.quorum as usize,
            status: row.status,
            consensus: row
                .consensus
                .and_then(|json| serde_json::from_str(&json).ok()),
            snapshot_hash: row.snapshot_hash,
            round: row.round as usize,
            created_at: row.created_at,
            completed_at: row.completed_at,
        }
    }
}

/// One evaluator member as reported in a group detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationMemberInfo {
    pub member_id: Uuid,
    pub node_id: String,
    pub agent_id: String,
    pub task_id: Option<Uuid>,
    pub status: String,
    pub result: Option<agentmesh_orchestrator::evaluation::EvaluationResult>,
    pub error: Option<String>,
}

impl From<agentmesh_storage::EvaluationMemberRow> for EvaluationMemberInfo {
    fn from(row: agentmesh_storage::EvaluationMemberRow) -> Self {
        Self {
            member_id: row.id,
            node_id: row.node_id,
            agent_id: row.agent_id,
            task_id: row.task_id,
            status: row.status,
            result: row
                .result_json
                .and_then(|json| serde_json::from_str(&json).ok()),
            error: row.error,
        }
    }
}

/// Full detail of an evaluation group with its members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationDetail {
    pub group_id: Uuid,
    pub workflow_id: Uuid,
    pub strategy: String,
    pub quorum: usize,
    pub status: String,
    pub consensus: Option<agentmesh_orchestrator::evaluation::ConsensusResult>,
    pub snapshot_hash: Option<String>,
    /// Which consensus fix round this group evaluates (Phase 22 §13).
    pub round: usize,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub members: Vec<EvaluationMemberInfo>,
}

/// One evaluation group in the compact `workflow evaluations` listing
/// (Phase 22 §18): the round, group, valid-vote count, consensus outcome and
/// snapshot hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationRoundInfo {
    pub round: usize,
    pub group_id: Uuid,
    pub valid_count: usize,
    pub total_count: usize,
    pub result: Option<agentmesh_orchestrator::evaluation::ConsensusOutcome>,
    pub snapshot_hash: Option<String>,
    pub status: String,
}

impl From<agentmesh_storage::EvaluationGroupRow> for EvaluationRoundInfo {
    fn from(row: agentmesh_storage::EvaluationGroupRow) -> Self {
        let (valid_count, total_count, result) = match &row.consensus {
            Some(json) => {
                serde_json::from_str::<agentmesh_orchestrator::evaluation::ConsensusResult>(json)
                    .map(|c| (c.valid_count, c.total_count, Some(c.outcome)))
                    .unwrap_or((0, 0, None))
            }
            None => (0, 0, None),
        };
        Self {
            round: row.round as usize,
            group_id: row.id,
            valid_count,
            total_count,
            result,
            snapshot_hash: row.snapshot_hash,
            status: row.status,
        }
    }
}

/// Request to run a standalone evaluation of a workflow's latest implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationStartRequest {
    /// Evaluators 1..=5 (default 3).
    #[serde(default)]
    pub evaluators: Option<usize>,
    /// `majority` | `unanimous` (default `majority`).
    #[serde(default)]
    pub strategy: Option<String>,
    /// Minimum valid results (default 2).
    #[serde(default)]
    pub quorum: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationStartResponse {
    pub workflow_id: Uuid,
    pub group_id: Uuid,
}

// ---------- Phase 17: AI planner plans ----------

/// Request to generate a plan from a natural-language goal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCreateRequest {
    pub goal: String,
    /// Explicit planner agent id; bypasses routing but still goes through A2A.
    #[serde(default)]
    pub agent: Option<String>,
}

/// Response to creating a plan: the daemon owns the planning task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCreateResponse {
    pub plan_id: Uuid,
}

/// One plan as reported when listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanInfo {
    pub plan_id: Uuid,
    pub goal: String,
    pub status: String,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub validation_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub executed_at: Option<String>,
}

/// One planned node as reported in a plan detail (preview).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNodeInfo {
    pub id: String,
    pub role: String,
    pub intent: String,
    pub objective: String,
    pub depends_on: Vec<String>,
}

/// Full detail of one plan, including its preview nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDetail {
    pub plan_id: Uuid,
    pub goal: String,
    pub status: String,
    pub summary: Option<String>,
    pub nodes: Vec<PlanNodeInfo>,
    pub planner_agent_id: Option<String>,
    pub planner_task_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    pub validation_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub executed_at: Option<String>,
    /// The active revision (Phase 18).
    pub current_revision: Option<i64>,
    /// Which revision actually executed (audit, Phase 18).
    pub executed_revision: Option<i64>,
    /// The current revision's raw plan JSON (for `plan show --json`, the edit
    /// round-trip and audit).
    pub plan_json: Option<String>,
}

/// Request to execute a ready plan (`max_parallel` comes from the CLI/config,
/// never from the plan itself). `check = true` previews without claiming or
/// creating a workflow (the CLI `--check` path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanExecuteRequest {
    #[serde(default)]
    pub max_parallel: usize,
    /// Preview only; never claims the plan or creates a workflow.
    #[serde(default)]
    pub check: bool,
    /// The explicit source project/repository the executed workflow operates
    /// on (Phase 22 §4). Never part of the plan JSON — it is execution
    /// control-plane input.
    #[serde(default)]
    pub source_workspace: Option<String>,
}

/// Response to executing a plan: either the execution preview (`--check`) or
/// the claimed workflow the daemon now owns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanExecuteResponse {
    Preview { preview: PlanPreview },
    Workflow { workflow_id: Uuid },
}

// ---------- Phase 18: plan edit, policy, budget, history ----------

/// Request to replace the current revision with an edited plan JSON. The edit
/// uses the exact same [`agentmesh_orchestrator::WorkflowPlan`] schema as the
/// planner output — there is no second, editable DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEditRequest {
    pub plan_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEditResponse {
    pub plan_id: Uuid,
    pub revision: i64,
}

/// One plan revision as reported by `plan revisions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRevisionInfo {
    pub revision: i64,
    pub source: String,
    pub created_at: String,
}

/// The active policy limits, for rendering the execution preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPolicyUsage {
    pub max_nodes: usize,
    pub max_agent_calls: usize,
    pub max_parallel: usize,
}

/// The `plan execute --check` preview: budget + policy usage, never a claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPreview {
    pub plan_id: Uuid,
    pub status: String,
    pub revision: Option<i64>,
    pub node_count: usize,
    pub estimated_agent_calls: usize,
    /// Evaluator agent calls across all consensus fix rounds (Phase 22 §15).
    pub evaluation_agent_calls: usize,
    pub root_count: usize,
    pub terminal_count: usize,
    pub planning_calls: usize,
    pub max_parallel_requested: usize,
    pub effective_max_parallel: usize,
    pub policy: PlanPolicyUsage,
}

// ---------- Phase 24: Provenance, Audit, Replay & Lineage ----------

use agentmesh_core::provenance::ProvenanceEvent;

/// Workflow audit response containing recorded provenance events and integrity status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowAuditResponse {
    pub workflow_id: Uuid,
    pub schema_version: u32,
    pub is_legacy: bool,
    pub integrity_valid: bool,
    pub events_count: usize,
    pub events: Vec<ProvenanceEvent>,
    pub details: Vec<String>,
}

/// Request to run deterministic decision replay.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowReplayRequest {
    #[serde(default)]
    pub verify_only: bool,
}

/// Response of deterministic decision replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowReplayResponse {
    pub workflow_id: Uuid,
    pub passed: bool,
    pub is_legacy: bool,
    pub integrity_passed: bool,
    pub consensus_passed: bool,
    pub selection_passed: bool,
    pub apply_passed: bool,
    pub policy_passed: bool,
    pub mismatches: Vec<String>,
    pub details: Vec<String>,
}

/// Workflow lineage response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowLineageResponse {
    pub workflow_id: Uuid,
    pub preset: String,
    pub goal: String,
    pub status: String,
    pub parent_workflow_id: Option<Uuid>,
    pub recovery_workflows: Vec<Uuid>,
    pub plan_id: Option<Uuid>,
    pub graph_revision: i64,
    pub replans_count: usize,
    pub competition_group_id: Option<Uuid>,
    pub winner_candidate_id: Option<String>,
    pub evaluation_groups_count: usize,
    pub apply_id: Option<Uuid>,
    pub provenance_events_count: usize,
}
