//! Daemon-owned workflow runtime (Phase 12) + Phase 16 DAG workflows.
//!
//! The daemon persists every workflow and drives it in the background, so a
//! CLI disconnect never stops the workflow. Crash recovery marks interrupted
//! runs; an explicit resume continues them without rerunning completed
//! steps/nodes. Sequential presets and DAG presets share the same service,
//! persistence tables and event channel; only the executor differs.

use std::collections::HashMap;
use std::sync::Arc;

use crate::protocol::{WorkflowDetail, WorkflowInfo, WorkflowStepInfo, WorkflowStreamEvent};
use agentmesh_a2a::mapping::to_artifact;
use agentmesh_a2a::types::A2AArtifact;
use agentmesh_core::provenance::{
    CandidateCompletedPayload, ConsensusComputedPayload, EvaluationCompletedPayload,
    PolicySnapshot, RecoveryWorkflowCreatedPayload, WinnerSelectedPayload,
    WorkflowCancelledPayload, WorkflowCompletedPayload, WorkflowFailedPayload,
    WorkflowResumedPayload, WorkflowStartedPayload, actor_type, entity_type, event_type,
};
use agentmesh_orchestrator::dag::preset_graph;
use agentmesh_orchestrator::dag_scheduler::{
    DagPersister, DagResumeSeed, DagRun, EvaluationConfig, NodeStatus,
};
use agentmesh_orchestrator::evaluation::{ConsensusOutcome, ConsensusStrategy};
use agentmesh_orchestrator::policy::{PlanPolicy, PlanPolicyEngine};
use agentmesh_orchestrator::workflow::{
    WorkflowEngine, WorkflowPersister, WorkflowResumeSeed, WorkflowRun,
};
use agentmesh_orchestrator::{
    AgentDirectory, HandoffPackage, PersistedStepResult, ReviewVerdict, RuleRouter, WorkflowGraph,
    WorkflowNode, WorkflowOptions, WorkflowPlan, WorkflowResult, WorkflowRole, WorkflowStatus,
    WorkflowStep, WorkflowStepResult, WorkflowStepStatus, build_handoff,
};
use agentmesh_storage::{
    WorkflowPlanRepository, WorkflowReplanRepository, WorkflowRepository, WorkflowRow,
    WorkflowStepRepository, WorkflowStepRow,
};
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

/// The persisted DAG of a workflow plus its live state (Phase 19), used as the
/// replan planner's input and the delta's base.
pub struct CurrentDag {
    pub graph: WorkflowGraph,
    /// node id → current scheduling status (from persisted step rows).
    pub statuses: std::collections::HashMap<String, NodeStatus>,
    /// node id → completed summary (from step rows), for the planner.
    pub summaries: std::collections::HashMap<String, String>,
    pub graph_revision: i64,
}

/// The immutable failure history handed to the Failure Analyzer (Phase 20 §5).
pub struct RecoveryInputs {
    pub goal: String,
    pub failed_node_id: String,
    pub failed_role: String,
    pub failed_intent: String,
    pub failed_error: String,
    pub failed_summary: Option<String>,
    /// (node/role, summary) of every completed step.
    pub dependency_summaries: Vec<(String, String)>,
    pub failed_task_id: Option<Uuid>,
    pub failed_session_id: Option<Uuid>,
    /// The failed task's artifacts (relevant inputs, not the raw stream).
    pub artifacts: Vec<agentmesh_core::Artifact>,
}

/// Errors produced by the workflow service.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("workflow `{0}` not found")]
    NotFound(Uuid),

    #[error("workflow `{0}` is not running")]
    NotRunning(Uuid),

    #[error("workflow `{0}` is not resumable (status `{1}`); only interrupted workflows resume")]
    NotResumable(Uuid, String),

    #[error("agent directory is not initialized")]
    DirectoryUninitialized,

    #[error("InvalidSourceWorkspace `{0}`: {1}")]
    InvalidSourceWorkspace(String, String),

    #[error(
        "EvaluationBudgetExceeded: the consensus graph needs {0} evaluator calls but the limit is {1}"
    )]
    EvaluationBudgetExceeded(usize, usize),

    #[error(
        "CompetitionBudgetExceeded: the competition needs {0} candidate calls but the limit is {1}"
    )]
    CompetitionBudgetExceeded(usize, usize),

    #[error("InsufficientCandidates: need {0} distinct candidate agents, only {1} available")]
    InsufficientCandidates(usize, usize),

    #[error("InsufficientEvaluationPanel: need {0} distinct evaluator agents, only {1} available")]
    InsufficientEvaluationPanel(usize, usize),

    #[error("consensus fix round rejected by policy: {0}")]
    FixRoundPolicy(String),

    #[error("invalid persisted workflow state: {0}")]
    InvalidState(String),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("orchestrator error: {0}")]
    Orchestrator(#[from] agentmesh_orchestrator::OrchestratorError),

    #[error("task error: {0}")]
    Task(#[from] agentmesh_tasks::TaskError),
}

/// One live (in-memory) workflow executor being driven by this daemon.
enum LiveRun {
    Sequential(Arc<WorkflowRun>),
    Dag(Arc<DagRun>),
}

impl LiveRun {
    fn workflow_id(&self) -> Uuid {
        match self {
            LiveRun::Sequential(run) => run.workflow_id(),
            LiveRun::Dag(run) => run.workflow_id(),
        }
    }
}

/// One live (in-memory) workflow being driven by this daemon.
struct LiveWorkflow {
    run: LiveRun,
    events: broadcast::Sender<WorkflowStreamEvent>,
}

/// Daemon-owned workflow runtime: persistence + background execution.
pub struct WorkflowService {
    instance_id: Uuid,
    task_manager: agentmesh_tasks::TaskManager,
    workflows: WorkflowRepository,
    steps: WorkflowStepRepository,
    /// AI-planner plans whose workflows this daemon owns (Phase 17); used to
    /// rebuild a plan's graph when resuming a plan-executed workflow.
    plans: WorkflowPlanRepository,
    /// Replan proposals (Phase 19): the atomic graph apply is owned here.
    replans: WorkflowReplanRepository,
    /// Evaluation groups + members (Phase 21): consensus-review persistence.
    evaluations: agentmesh_storage::EvaluationRepository,
    /// Competition groups + candidates (Phase 23): best-of-n competition persistence.
    competitions: agentmesh_storage::CompetitionRepository,
    /// Workspace lifecycle (Phase 21 §14 snapshot verification).
    workspaces: Arc<WorkspaceManager>,
    directory: std::sync::RwLock<Option<AgentDirectory>>,
    router: RuleRouter,
    live: RwLock<HashMap<Uuid, Arc<LiveWorkflow>>>,
    /// Per-workflow async mutex serializing DAG persistence (node rows written
    /// by the live scheduler) with a replan apply's graph replacement (Phase
    /// 19 §19). Never held across a blocking lock; pure tokio::sync.
    graph_locks: RwLock<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>,
    /// Best-effort sink for workflow-failure notifications (Phase 20 §8): the
    /// daemon feeds it to the RecoveryService so `[recovery] auto_generate`
    /// can propose a recovery child. `None` disables auto-generation.
    failure_sink: RwLock<Option<tokio::sync::mpsc::Sender<Uuid>>>,
    /// Optional override of `[evaluation]` control-plane limits (Phase 22
    /// §16/§17). Injected by tests — the config files are process-global and
    /// unsafe to mutate under parallel tests.
    evaluation_override: std::sync::RwLock<Option<EvaluationOverride>>,
    /// Optional override of `[competition]` control-plane limits (Phase 23).
    competition_override: std::sync::RwLock<Option<CompetitionOverride>>,
    provenance: agentmesh_storage::ProvenanceRepository,
}

/// Test/embedded override of the evaluation control-plane limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvaluationOverride {
    pub max_total_evaluator_calls: Option<usize>,
    pub default_evaluators: Option<usize>,
}

/// Test/embedded override of the competition control-plane limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompetitionOverride {
    pub default_candidates: Option<usize>,
    pub default_evaluators: Option<usize>,
    pub max_candidates: Option<usize>,
    pub max_total_candidate_calls: Option<usize>,
}

impl WorkflowService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_id: Uuid,
        task_manager: agentmesh_tasks::TaskManager,
        workflows: WorkflowRepository,
        steps: WorkflowStepRepository,
        plans: WorkflowPlanRepository,
        replans: WorkflowReplanRepository,
        evaluations: agentmesh_storage::EvaluationRepository,
        competitions: agentmesh_storage::CompetitionRepository,
        workspaces: Arc<WorkspaceManager>,
        router: RuleRouter,
    ) -> Arc<Self> {
        let provenance = agentmesh_storage::ProvenanceRepository::new(workflows.database().clone());
        Arc::new(Self {
            instance_id,
            task_manager,
            workflows,
            steps,
            plans,
            replans,
            evaluations,
            competitions,
            workspaces,
            directory: std::sync::RwLock::new(None),
            router,
            live: RwLock::new(HashMap::new()),
            graph_locks: RwLock::new(HashMap::new()),
            failure_sink: RwLock::new(None),
            evaluation_override: std::sync::RwLock::new(None),
            competition_override: std::sync::RwLock::new(None),
            provenance,
        })
    }

    pub fn provenance(&self) -> &agentmesh_storage::ProvenanceRepository {
        &self.provenance
    }

    /// Inject a competition control-plane override (Phase 23; tests).
    pub fn set_competition_override(&self, override_: CompetitionOverride) {
        *self.competition_override.write().unwrap() = Some(override_);
    }

    /// Inject an evaluation control-plane override (Phase 22; tests). `None`
    /// restores the `[evaluation]` config.
    pub fn set_evaluation_override(&self, override_: EvaluationOverride) {
        *self.evaluation_override.write().unwrap() = Some(override_);
    }

    /// Inject the agent directory (built after the A2A listeners start).
    pub fn set_directory(&self, directory: AgentDirectory) {
        *self.directory.write().unwrap() = Some(directory);
    }

    /// The agent directory used for routing (shared with the planner).
    pub fn directory(&self) -> Result<AgentDirectory, WorkflowError> {
        self.directory
            .read()
            .unwrap()
            .clone()
            .ok_or(WorkflowError::DirectoryUninitialized)
    }

    /// The router used to resolve every step/node's agent (shared with the
    /// planner, which routes its own single Architecture task).
    pub fn router(&self) -> RuleRouter {
        self.router.clone()
    }

    /// The per-workflow async mutex serializing DAG graph mutation (Phase 19).
    /// The live scheduler's node-row writes and a replan apply both take it, so
    /// a graph replacement never interleaves with a node persistence write.
    async fn node_write_guard(&self, workflow_id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.graph_locks.write().await;
        map.entry(workflow_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    // ---------- Phase 19: replan graph swap ----------

    /// The persisted DAG of a workflow (rebuild from node rows + dependency
    /// edges), its current node statuses and completed summaries — the planner
    /// input and the delta's base. `None` when the workflow is not a DAG or
    /// has no persisted rows.
    pub async fn current_dag(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<CurrentDag>, WorkflowError> {
        let Some(row) = self.workflows.get(workflow_id).await? else {
            return Ok(None);
        };
        let step_rows = self.steps.list_for(workflow_id).await?;
        let deps = self.steps.list_dependencies(workflow_id).await?;
        let mut nodes = Vec::new();
        let mut statuses = HashMap::new();
        let mut summaries = HashMap::new();
        for step in &step_rows {
            let Some(node_id) = step.node_id.clone() else {
                continue; // legacy sequential step
            };
            let role = WorkflowRole::from_str(&step.role).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown role {}", step.role))
            })?;
            let intent = agentmesh_core::TaskIntent::from_key(&step.intent).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown intent {}", step.intent))
            })?;
            let mut node_deps: Vec<String> = deps
                .iter()
                .filter(|d| d.node_id == node_id)
                .map(|d| d.depends_on_node_id.clone())
                .collect();
            node_deps.sort();
            node_deps.dedup();
            nodes.push(WorkflowNode {
                node_id: node_id.clone(),
                role,
                intent,
                dependencies: node_deps,
                objective: step.objective.clone(),
            });
            statuses.insert(
                node_id.clone(),
                NodeStatus::from_str(&step.status).unwrap_or(NodeStatus::Pending),
            );
            if let Some(summary) = &step.summary {
                summaries.insert(node_id, summary.clone());
            }
        }
        if nodes.is_empty() {
            return Ok(None);
        }
        let graph = WorkflowGraph::new(nodes)?;
        Ok(Some(CurrentDag {
            graph,
            statuses,
            summaries,
            graph_revision: row.graph_revision,
        }))
    }

    /// Build the complete node-row + edge set for a candidate graph, preserving
    /// every surviving row's identity (status / result / task) so a Completed
    /// or Running node is never reset; new nodes start Pending.
    pub async fn build_graph_rows(
        &self,
        workflow_id: Uuid,
        graph: &WorkflowGraph,
    ) -> Result<(Vec<WorkflowStepRow>, Vec<(String, String)>), WorkflowError> {
        let existing = self.steps.list_for(workflow_id).await?;
        let mut rows = Vec::new();
        let mut edges = Vec::new();
        for (ordinal, node) in graph.nodes.iter().enumerate() {
            let prior = existing
                .iter()
                .find(|r| r.node_id.as_deref() == Some(node.node_id.as_str()));
            let step = node.to_step();
            let status = prior
                .and_then(|p| NodeStatus::from_str(&p.status))
                .unwrap_or(NodeStatus::Pending);
            let mut row = self.node_row(
                workflow_id,
                ordinal,
                &node.node_id,
                &step,
                status,
                None,
                prior.and_then(|p| p.error.as_deref()),
            );
            if let Some(p) = prior {
                row.agent_id = p.agent_id.clone();
                row.task_id = p.task_id;
                row.summary = p.summary.clone();
                row.result_json = p.result_json.clone();
                row.started_at = p.started_at.clone();
                row.completed_at = p.completed_at.clone();
                row.error = p.error.clone();
                row.review_round = p.review_round;
                row.created_at = p.created_at.clone();
            }
            rows.push(row);
            for dep in &node.dependencies {
                edges.push((node.node_id.clone(), dep.clone()));
            }
        }
        Ok((rows, edges))
    }

    /// Apply a replanned candidate graph to a workflow (Phase 20 §2 P0): replace
    /// the persisted node rows + dependency edges, bump `graph_revision`, and
    /// mark the replan `applied` in ONE SQLite transaction, then hot-reload the
    /// live scheduler if the workflow is running. The per-workflow graph lock
    /// serializes this with the scheduler's own node-row writes, so a Running
    /// node's task is never restarted and its result row is never clobbered.
    /// Returns the new graph revision.
    pub async fn apply_replan_graph_atomic(
        &self,
        replan_id: Uuid,
        workflow_id: Uuid,
        graph: &WorkflowGraph,
    ) -> Result<i64, WorkflowError> {
        let lock = self.node_write_guard(workflow_id).await;
        let _guard = lock.lock().await;

        let (rows, edges) = self.build_graph_rows(workflow_id, graph).await?;
        let current = self.workflows.graph_revision(workflow_id).await?;
        let new_revision = current + 1;
        self.replans
            .apply_graph_atomic(replan_id, workflow_id, &rows, &edges, new_revision)
            .await?;

        // Hot-reload the live run (if any): Running nodes keep their tasks and
        // statuses; new nodes start Pending; the scheduler re-promotes on its
        // next iteration.
        if let Some(live) = self.live.read().await.get(&workflow_id).cloned()
            && let LiveRun::Dag(run) = &live.run
        {
            run.reload_graph(graph.clone());
        }
        Ok(new_revision)
    }

    /// The current `graph_revision` of a workflow (Phase 19 §7).
    pub async fn graph_revision(&self, workflow_id: Uuid) -> Result<i64, WorkflowError> {
        Ok(self.workflows.graph_revision(workflow_id).await?)
    }

    /// Wire the workflow-failure notification sink (Phase 20 §8). The daemon
    /// feeds it to the RecoveryService for `[recovery] auto_generate`.
    pub async fn set_failure_sink(&self, sink: tokio::sync::mpsc::Sender<Uuid>) {
        *self.failure_sink.write().await = Some(sink);
    }

    /// Best-effort notification that a workflow reached `Failed`. Sent on the
    /// persister path; a dropped receiver (no sink / shutting down) is fine.
    async fn notify_failed(&self, workflow_id: Uuid) {
        let sink = self.failure_sink.read().await.clone();
        if let Some(sink) = sink {
            let _ = sink.send(workflow_id).await;
        }
    }

    /// Broadcast the recovery-child terminal event on the child's stream
    /// (Phase 20 §23). No-op for non-recovery workflows.
    async fn notify_recovery_terminal(
        &self,
        workflow_id: Uuid,
        status: WorkflowStatus,
        error: Option<String>,
    ) {
        let is_recovery_child = self
            .workflows
            .get(workflow_id)
            .await
            .ok()
            .flatten()
            .map(|row| row.parent_workflow_id.is_some())
            .unwrap_or(false);
        if !is_recovery_child {
            return;
        }
        let event = match status {
            WorkflowStatus::Completed => WorkflowStreamEvent::RecoveryCompleted {
                workflow_id,
                recovery_workflow_id: workflow_id,
            },
            WorkflowStatus::Failed => WorkflowStreamEvent::RecoveryFailed {
                workflow_id,
                recovery_workflow_id: workflow_id,
                error,
            },
            _ => return,
        };
        self.broadcast_to_workflow(workflow_id, event).await;
    }

    /// The child workflows that recover `workflow_id` (Phase 20 §19 lineage).
    pub async fn child_workflows(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<agentmesh_storage::WorkflowRow>, WorkflowError> {
        Ok(self.workflows.child_workflows(workflow_id).await?)
    }

    /// How many recovery children `workflow_id` already has (attempt budget).
    pub async fn recovery_child_count(&self, workflow_id: Uuid) -> Result<i64, WorkflowError> {
        Ok(self.workflows.recovery_child_count(workflow_id).await?)
    }

    /// Best-effort broadcast of a stream event to a workflow's live channel
    /// (Phase 20 §23). No-op when the workflow is not live (e.g. a finished
    /// parent).
    pub async fn broadcast_to_workflow(&self, workflow_id: Uuid, event: WorkflowStreamEvent) {
        if let Some(live) = self.live.read().await.get(&workflow_id).cloned() {
            let _ = live.events.send(event);
        }
    }

    /// The lineage of a workflow: its parent (if a recovery child) and its
    /// recovery children (Phase 20 §19).
    pub async fn lineage(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<crate::protocol::WorkflowLineage>, WorkflowError> {
        let Some(row) = self.workflows.get(workflow_id).await? else {
            return Ok(None);
        };
        let parent = match row.parent_workflow_id {
            Some(parent_id) => {
                let parent_row =
                    self.workflows
                        .get(parent_id)
                        .await?
                        .ok_or(WorkflowError::InvalidState(format!(
                            "lineage parent {parent_id} of {workflow_id} missing"
                        )))?;
                Some(Box::new(crate::protocol::LineageNode {
                    workflow_id: parent_id,
                    preset: parent_row.preset.clone(),
                    status: WorkflowStatus::from_str(&parent_row.status)
                        .unwrap_or(WorkflowStatus::Failed),
                    parent_workflow_id: parent_row.parent_workflow_id,
                    recovery_of_node_id: parent_row.recovery_of_node_id.clone(),
                    recovery_attempt: parent_row.recovery_attempt,
                    created_at: parent_row.created_at.clone(),
                }))
            }
            None => None,
        };
        let children = self.workflows.child_workflows(workflow_id).await?;
        let recovery_children = children
            .iter()
            .map(|c| crate::protocol::LineageNode {
                workflow_id: c.id,
                preset: c.preset.clone(),
                status: WorkflowStatus::from_str(&c.status).unwrap_or(WorkflowStatus::Pending),
                parent_workflow_id: c.parent_workflow_id,
                recovery_of_node_id: c.recovery_of_node_id.clone(),
                recovery_attempt: c.recovery_attempt,
                created_at: c.created_at.clone(),
            })
            .collect();
        Ok(Some(crate::protocol::WorkflowLineage {
            workflow_id,
            parent,
            recovery_children,
        }))
    }

    /// The first `Failed` step row of a workflow, if any (Phase 20 §21: only
    /// task/node failures generate a recovery).
    pub async fn failed_node(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<WorkflowStepRow>, WorkflowError> {
        let steps = self.steps.list_for(workflow_id).await?;
        Ok(steps.into_iter().find(|s| s.status == "failed"))
    }

    /// The inputs the Failure Analyzer sees (Phase 20 §5): the immutable
    /// failure history (goal, failed node, completed dependency summaries, the
    /// failed task's summary + artifacts + session) — never the reasoning/raw
    /// event stream.
    pub async fn recovery_inputs(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<RecoveryInputs>, WorkflowError> {
        let Some(row) = self.workflows.get(workflow_id).await? else {
            return Ok(None);
        };
        let steps = self.steps.list_for(workflow_id).await?;
        let Some(failed) = steps.iter().find(|s| s.status == "failed") else {
            return Ok(None);
        };
        let mut dependency_summaries = Vec::new();
        for step in &steps {
            if step.status == WorkflowStepStatus::Completed.as_str()
                && let Some(summary) = &step.summary
            {
                dependency_summaries.push((
                    step.node_id.clone().unwrap_or(step.role.clone()),
                    summary.clone(),
                ));
            }
        }
        let mut artifacts = Vec::new();
        let mut failed_task_id = None;
        let mut failed_session_id = None;
        if let Some(task_id) = failed.task_id {
            failed_task_id = Some(task_id);
            if let Ok(Some(task)) = self.task_manager.get_task(task_id).await {
                failed_session_id = task.agent_session_id;
                if let Ok(list) = self.task_manager.list_artifacts(task_id).await {
                    artifacts = list;
                }
            }
        }
        Ok(Some(RecoveryInputs {
            goal: row.goal.clone(),
            failed_node_id: failed.node_id.clone().unwrap_or(failed.role.clone()),
            failed_role: failed.role.clone(),
            failed_intent: failed.intent.clone(),
            failed_error: failed
                .error
                .clone()
                .unwrap_or_else(|| "node failed".to_string()),
            failed_summary: failed.summary.clone(),
            dependency_summaries,
            failed_task_id,
            failed_session_id,
            artifacts,
        }))
    }

    /// Start a recovery child workflow (Phase 20 §9/§17): a new DAG workflow
    /// that reuses the failed parent's context (so existing agent sessions and
    /// worktrees are reused for the same agent, isolated for a different one)
    /// and records the parent lineage. Returns the child workflow id.
    pub async fn start_recovery_workflow(
        self: &Arc<Self>,
        goal: &str,
        graph: WorkflowGraph,
        options: WorkflowOptions,
        parent_workflow_id: Uuid,
        failed_node_id: &str,
        attempt: i64,
    ) -> Result<Uuid, WorkflowError> {
        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory, self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();
        let parent = self.workflows.get(parent_workflow_id).await?;
        let parent_context = parent.as_ref().and_then(|p| p.context_id);
        // Phase 22 §3/§20: a recovery child inherits the parent's immutable
        // source workspace — never the daemon cwd, never the planner.
        let source_workspace = parent.as_ref().and_then(|p| p.source_workspace.clone());

        let row = WorkflowRow {
            id: workflow_id,
            preset: "recovery".to_string(),
            goal: goal.to_string(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: parent_context,
            options_json: serde_json::to_string(&options).unwrap_or_else(|_| "{}".to_string()),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now,
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: Some(parent_workflow_id),
            recovery_of_node_id: Some(failed_node_id.to_string()),
            recovery_attempt: attempt,
            source_workspace: source_workspace.clone(),
        };
        self.workflows.create(&row).await?;

        let r_payload = serde_json::to_value(RecoveryWorkflowCreatedPayload {
            parent_workflow_id,
            child_workflow_id: workflow_id,
            recovery_of_node_id: failed_node_id.to_string(),
            attempt: attempt as usize,
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::RECOVERY_WORKFLOW_CREATED,
                entity_type::RECOVERY,
                failed_node_id,
                None,
                actor_type::SYSTEM,
                Some("RecoveryService"),
                &r_payload,
            )
            .await;

        let run = engine.start_dag_with_graph("recovery", goal, graph, options, workflow_id)?;
        if let Some(context_id) = parent_context {
            run.set_context_id(context_id);
        }
        run.set_source_workspace(source_workspace.map(std::path::PathBuf::from));
        self.persist_dag_plan(&run).await?;
        self.spawn_run(LiveRun::Dag(run), None, None).await;
        Ok(workflow_id)
    }

    // ---------- start / resume / cancel ----------

    /// Start a fresh workflow: persist it immediately, then run it in the
    /// background. Returns the workflow id. DAG presets (e.g.
    /// `parallel-review`) are scheduled in parallel with `max_parallel`.
    pub async fn start(
        self: &Arc<Self>,
        preset: &str,
        goal: &str,
        options: WorkflowOptions,
    ) -> Result<Uuid, WorkflowError> {
        self.start_with_source(preset, goal, options, None).await
    }

    /// [`Self::start`] with an explicit source workspace (Phase 22 §1): the
    /// source project the user wants AgentMesh to operate on, canonicalized
    /// and validated here — never guessed from the daemon cwd later. The
    /// execution agents still work in isolated worktrees.
    pub async fn start_with_source(
        self: &Arc<Self>,
        preset: &str,
        goal: &str,
        options: WorkflowOptions,
        source_workspace: Option<String>,
    ) -> Result<Uuid, WorkflowError> {
        let source = self.validate_source_workspace(source_workspace).await?;
        if preset == agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW {
            return self.start_consensus_review(goal, options, source).await;
        }
        if preset == agentmesh_orchestrator::dag::PRESET_BEST_OF_N {
            return self.start_best_of_n(goal, options, source).await;
        }
        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory, self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();

        let row = WorkflowRow {
            id: workflow_id,
            preset: preset.to_string(),
            goal: goal.to_string(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: None,
            options_json: serde_json::to_string(&options).unwrap_or_else(|_| "{}".to_string()),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now,
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: source.clone(),
        };
        self.workflows.create(&row).await?;

        let p_payload = serde_json::to_value(WorkflowStartedPayload {
            workflow_id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            source_workspace: row.source_workspace.clone(),
            base_revision: None,
            policy: PolicySnapshot::default(),
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_STARTED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::SYSTEM,
                Some("WorkflowService"),
                &p_payload,
            )
            .await;

        if preset_graph(preset).is_some() {
            let run = engine.start_dag_with_id(preset, goal, options, workflow_id)?;
            run.set_source_workspace(source.map(std::path::PathBuf::from));
            self.persist_dag_plan(&run).await?;
            self.spawn_run(LiveRun::Dag(run), None, None).await;
        } else {
            let run = engine.start_with_id(preset, goal, options, workflow_id)?;
            run.set_source_workspace(source.map(std::path::PathBuf::from));
            for (ordinal, step) in run.workflow.steps.iter().enumerate() {
                let step_row = self.step_row(
                    workflow_id,
                    ordinal,
                    None,
                    step,
                    WorkflowStepStatus::Pending,
                    None,
                    None,
                );
                self.steps.upsert(&step_row).await?;
            }
            self.spawn_run(LiveRun::Sequential(run), None, None).await;
        }
        Ok(workflow_id)
    }

    /// Start a `consensus-review` workflow (Phase 21 §11): Architecture →
    /// Implementation → {N parallel evaluators} → deterministic ConsensusGate.
    /// Creates the persisted evaluation group + members and seeds the run's
    /// evaluation config from `[evaluation]` (control plane, never the planner).
    /// Phase 22 §16: the initial evaluator count is checked against
    /// `max_total_evaluator_calls` before anything runs.
    async fn start_consensus_review(
        self: &Arc<Self>,
        goal: &str,
        options: WorkflowOptions,
        source_workspace: Option<String>,
    ) -> Result<Uuid, WorkflowError> {
        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory, self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();
        let (evaluators, quorum, strategy) = self.evaluation_settings();
        // Phase 22 §16: the initial graph's evaluator calls must fit the total
        // evaluation budget (default 3 evaluators × 2 rounds = 6).
        let max_total = self.evaluation_max_total_calls();
        if evaluators > max_total {
            return Err(WorkflowError::EvaluationBudgetExceeded(
                evaluators, max_total,
            ));
        }
        let graph = agentmesh_orchestrator::dag::consensus_review_graph(evaluators);

        let row = WorkflowRow {
            id: workflow_id,
            preset: agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW.to_string(),
            goal: goal.to_string(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: None,
            options_json: serde_json::to_string(&options).unwrap_or_else(|_| "{}".to_string()),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: source_workspace.clone(),
        };
        self.workflows.create(&row).await?;

        let p_payload = serde_json::to_value(WorkflowStartedPayload {
            workflow_id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            source_workspace: row.source_workspace.clone(),
            base_revision: None,
            policy: PolicySnapshot::default(),
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_STARTED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::SYSTEM,
                Some("WorkflowService"),
                &p_payload,
            )
            .await;

        // Persisted evaluation group + one member per evaluator node.
        let group_id = Uuid::new_v4();
        self.evaluations
            .create_group(&agentmesh_storage::EvaluationGroupRow {
                id: group_id,
                workflow_id,
                source_task_id: None,
                strategy: strategy.as_str().to_string(),
                quorum: quorum as i64,
                status: agentmesh_storage::evaluation_status::PENDING.to_string(),
                consensus: None,
                snapshot_hash: None,
                round: 0,
                created_at: now.clone(),
                completed_at: None,
            })
            .await?;
        for i in 0..evaluators {
            self.evaluations
                .create_member(&agentmesh_storage::EvaluationMemberRow {
                    id: Uuid::new_v4(),
                    group_id,
                    node_id: format!("evaluator_{}", i + 1),
                    agent_id: String::new(), // assigned when the node dispatches
                    task_id: None,
                    status: agentmesh_storage::member_status::PENDING.to_string(),
                    result_json: None,
                    error: None,
                    created_at: now.clone(),
                    completed_at: None,
                })
                .await?;
        }

        let eval_seed = EvaluationConfig {
            strategy,
            quorum,
            required_evaluators: evaluators,
            source_task_id: None,
        };
        let run = engine.start_dag_with_graph_and_evaluation(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            goal,
            graph,
            options,
            workflow_id,
            eval_seed,
        )?;
        run.set_source_workspace(source_workspace.map(std::path::PathBuf::from));
        self.persist_dag_plan(&run).await?;
        self.spawn_run(LiveRun::Dag(run), None, None).await;
        Ok(workflow_id)
    }

    /// Start a `best-of-n` workflow (Phase 23): Architecture →
    /// {N parallel Candidates in isolated session lanes} →
    /// {M blind evaluators per candidate in isolated lanes} →
    /// {ConsensusGate per candidate} → SelectionGate.
    ///
    /// Evaluators and candidates run with strict session isolation and distinct agents.
    async fn start_best_of_n(
        self: &Arc<Self>,
        goal: &str,
        options: WorkflowOptions,
        source_workspace: Option<String>,
    ) -> Result<Uuid, WorkflowError> {
        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory.clone(), self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();

        let (candidates, evaluators, strategy, quorum) = self.competition_settings();
        let max_total_candidate_calls = self.competition_max_total_calls();

        if candidates > max_total_candidate_calls {
            return Err(WorkflowError::CompetitionBudgetExceeded(
                candidates,
                max_total_candidate_calls,
            ));
        }

        // Distinct candidate agent routing (Phase 23 §7):
        let mut candidate_agents: Vec<String> = Vec::new();

        for _ in 0..candidates {
            match self.router.route_with_constraints(
                &directory,
                agentmesh_core::TaskIntent::Implementation,
                &candidate_agents,
            ) {
                agentmesh_orchestrator::RouteDecision::Agent { agent_id, .. } => {
                    candidate_agents.push(agent_id);
                }
                agentmesh_orchestrator::RouteDecision::NoCapableAgent { .. } => {
                    return Err(WorkflowError::InsufficientCandidates(
                        candidates,
                        candidate_agents.len(),
                    ));
                }
            }
        }

        // For each candidate, verify and resolve distinct evaluator panel excluding candidate agent (no self-review)
        let mut candidate_evaluators: Vec<Vec<String>> = Vec::new();
        for c_agent in candidate_agents.iter() {
            let mut eval_panel: Vec<String> = Vec::new();
            let mut eval_excluded: Vec<String> = Vec::new();
            eval_excluded.push(c_agent.clone()); // No self-review

            for _ in 0..evaluators {
                match self.router.route_with_constraints(
                    &directory,
                    agentmesh_core::TaskIntent::Review,
                    &eval_excluded,
                ) {
                    agentmesh_orchestrator::RouteDecision::Agent { agent_id, .. } => {
                        eval_excluded.push(agent_id.clone());
                        eval_panel.push(agent_id);
                    }
                    agentmesh_orchestrator::RouteDecision::NoCapableAgent { .. } => {
                        return Err(WorkflowError::InsufficientEvaluationPanel(
                            evaluators,
                            eval_panel.len(),
                        ));
                    }
                }
            }
            candidate_evaluators.push(eval_panel);
        }

        let graph = agentmesh_orchestrator::dag::best_of_n_graph(candidates, evaluators);

        let row = WorkflowRow {
            id: workflow_id,
            preset: agentmesh_orchestrator::dag::PRESET_BEST_OF_N.to_string(),
            goal: goal.to_string(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: None,
            options_json: serde_json::to_string(&options).unwrap_or_else(|_| "{}".to_string()),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: source_workspace.clone(),
        };
        self.workflows.create(&row).await?;

        let p_payload = serde_json::to_value(WorkflowStartedPayload {
            workflow_id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            source_workspace: row.source_workspace.clone(),
            base_revision: None,
            policy: PolicySnapshot::default(),
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_STARTED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::SYSTEM,
                Some("WorkflowService"),
                &p_payload,
            )
            .await;

        // Create competition group
        let group_id = Uuid::new_v4();
        let base_rev = "HEAD".to_string();
        self.competitions
            .create_group(&agentmesh_storage::CompetitionGroupRow {
                id: group_id,
                workflow_id,
                source_workspace: source_workspace.clone(),
                base_revision: base_rev,
                candidate_count: candidates as i64,
                status: "running".to_string(),
                winner_candidate_id: None,
                winner_task_id: None,
                winner_workspace_id: None,
                winner_snapshot_hash: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            })
            .await?;

        // Create candidate records and evaluation groups
        for c in 1..=candidates {
            let candidate_id = format!("candidate_{c}");
            let candidate_agent = &candidate_agents[c - 1];
            let lane = format!("candidate:candidate_{c}");

            // Evaluation group for candidate c
            let eval_group_id = Uuid::new_v4();
            self.evaluations
                .create_group(&agentmesh_storage::EvaluationGroupRow {
                    id: eval_group_id,
                    workflow_id,
                    source_task_id: None,
                    strategy: strategy.as_str().to_string(),
                    quorum: quorum as i64,
                    status: agentmesh_storage::evaluation_status::PENDING.to_string(),
                    consensus: None,
                    snapshot_hash: None,
                    round: 0,
                    created_at: now.clone(),
                    completed_at: None,
                })
                .await?;

            // Evaluator members for candidate c
            for e in 1..=evaluators {
                let eval_node_id = format!("eval_c{c}_{e}");
                let eval_agent = candidate_evaluators[c - 1][e - 1].clone();
                self.evaluations
                    .create_member(&agentmesh_storage::EvaluationMemberRow {
                        id: Uuid::new_v4(),
                        group_id: eval_group_id,
                        node_id: eval_node_id,
                        agent_id: eval_agent,
                        task_id: None,
                        status: agentmesh_storage::member_status::PENDING.to_string(),
                        result_json: None,
                        error: None,
                        created_at: now.clone(),
                        completed_at: None,
                    })
                    .await?;
            }

            self.competitions
                .create_candidate(&agentmesh_storage::CompetitionCandidateRow {
                    id: Uuid::new_v4(),
                    group_id,
                    candidate_id: candidate_id.clone(),
                    agent_id: candidate_agent.clone(),
                    session_lane: lane.clone(),
                    task_id: None,
                    workspace_id: None,
                    snapshot_hash: None,
                    evaluation_group_id: Some(eval_group_id),
                    status: "pending".to_string(),
                    summary: None,
                    patch_path: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                })
                .await?;
        }

        let eval_seed = EvaluationConfig {
            strategy,
            quorum,
            required_evaluators: evaluators,
            source_task_id: None,
        };
        let run = engine.start_dag_with_graph_and_evaluation(
            agentmesh_orchestrator::dag::PRESET_BEST_OF_N,
            goal,
            graph,
            options,
            workflow_id,
            eval_seed,
        )?;
        run.set_source_workspace(source_workspace.map(std::path::PathBuf::from));

        // Assign candidate agents, lanes, and evaluator agents & lanes on run:
        for (c_idx, c_agent) in candidate_agents.iter().enumerate() {
            let c = c_idx + 1;
            let c_node_id = format!("candidate_{c}");
            let c_lane = format!("candidate:candidate_{c}");
            run.assign_agent(&c_node_id, c_agent);
            run.assign_lane(&c_node_id, &c_lane);

            for (e_idx, e_agent) in candidate_evaluators[c_idx].iter().enumerate() {
                let e = e_idx + 1;
                let eval_node_id = format!("eval_c{c}_{e}");
                let eval_lane =
                    format!("evaluation:{}:candidate_{}:eval_c{}_{}", group_id, c, c, e);
                run.assign_agent(&eval_node_id, e_agent);
                run.assign_lane(&eval_node_id, &eval_lane);
            }
        }

        self.persist_dag_plan(&run).await?;
        self.spawn_run(LiveRun::Dag(run), None, None).await;
        Ok(workflow_id)
    }

    /// The `[competition]` control-plane settings: `(candidates, evaluators, strategy, quorum)`.
    fn competition_settings(&self) -> (usize, usize, ConsensusStrategy, usize) {
        let config = agentmesh_core::AgentMeshConfig::load();
        let override_ = (*self.competition_override.read().unwrap()).unwrap_or_default();
        let comp = config.competition.as_ref();
        let defaults = agentmesh_core::CompetitionConfig::defaults();

        let max_candidates = override_
            .max_candidates
            .or_else(|| comp.and_then(|c| c.max_candidates))
            .or(defaults.max_candidates)
            .unwrap_or(agentmesh_orchestrator::dag::MAX_CANDIDATES)
            .min(agentmesh_orchestrator::dag::MAX_CANDIDATES);

        let candidates = override_
            .default_candidates
            .or_else(|| comp.and_then(|c| c.default_candidates))
            .or(defaults.default_candidates)
            .unwrap_or(agentmesh_orchestrator::dag::DEFAULT_CANDIDATES)
            .clamp(1, max_candidates);

        let evaluators = override_
            .default_evaluators
            .unwrap_or(agentmesh_orchestrator::dag::DEFAULT_CANDIDATE_EVALUATORS)
            .clamp(1, agentmesh_orchestrator::dag::MAX_CANDIDATE_EVALUATORS);

        let strategy = ConsensusStrategy::Majority;
        let quorum = (evaluators / 2) + 1;

        (candidates, evaluators, strategy, quorum)
    }

    /// The `[competition]` max_total_candidate_calls (Phase 23): total candidate calls allowed.
    fn competition_max_total_calls(&self) -> usize {
        if let Some(override_) = self.competition_override.read().unwrap().as_ref()
            && let Some(value) = override_.max_total_candidate_calls
        {
            return value;
        }
        let config = agentmesh_core::AgentMeshConfig::load();
        config
            .competition
            .as_ref()
            .and_then(|c| c.max_total_candidate_calls)
            .or(agentmesh_core::CompetitionConfig::defaults().max_total_candidate_calls)
            .unwrap_or(agentmesh_orchestrator::dag::MAX_CANDIDATES)
    }

    pub async fn competition_groups(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<agentmesh_storage::CompetitionGroupRow>, WorkflowError> {
        self.competitions
            .list_groups_for_workflow(workflow_id)
            .await
            .map_err(WorkflowError::Storage)
    }

    pub async fn competition_group(
        &self,
        group_id: Uuid,
    ) -> Result<Option<agentmesh_storage::CompetitionGroupRow>, WorkflowError> {
        self.competitions
            .get_group(group_id)
            .await
            .map_err(WorkflowError::Storage)
    }

    pub async fn competition_candidates(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<agentmesh_storage::CompetitionCandidateRow>, WorkflowError> {
        self.competitions
            .list_candidates_for_group(group_id)
            .await
            .map_err(WorkflowError::Storage)
    }

    /// The `[evaluation]` control-plane settings: `(evaluators, quorum,
    /// strategy)`, clamped to safe bounds. An injected override (tests) wins
    /// over the config files.
    fn evaluation_settings(&self) -> (usize, usize, ConsensusStrategy) {
        let config = agentmesh_core::AgentMeshConfig::load();
        let override_ = (*self.evaluation_override.read().unwrap()).unwrap_or_default();
        let eval = config.evaluation.as_ref();
        let defaults = agentmesh_core::EvaluationConfig::defaults();
        let evaluators = override_
            .default_evaluators
            .or_else(|| eval.and_then(|e| e.default_evaluators))
            .or(defaults.default_evaluators)
            .unwrap_or(3)
            .clamp(1, agentmesh_orchestrator::dag::MAX_EVALUATORS);
        let quorum = eval
            .and_then(|e| e.default_quorum)
            .or(defaults.default_quorum)
            .unwrap_or(2)
            .min(evaluators);
        let strategy = match eval.and_then(|e| e.strategy.as_deref()) {
            Some("unanimous") => ConsensusStrategy::Unanimous,
            _ => ConsensusStrategy::Majority,
        };
        (evaluators, quorum, strategy)
    }

    /// Start a standalone evaluation of an existing workflow's latest
    /// implementation (Phase 21 §16 `workflow evaluate`). A new evaluation
    /// workflow runs `{N evaluators} → ConsensusGate` over the source snapshot.
    /// Returns `(workflow_id, group_id)`.
    pub async fn start_evaluation(
        self: &Arc<Self>,
        source_workflow_id: Uuid,
        evaluators_override: Option<usize>,
        strategy_override: Option<&str>,
        quorum_override: Option<usize>,
    ) -> Result<(Uuid, Uuid), WorkflowError> {
        let source = self
            .workflows
            .get(source_workflow_id)
            .await?
            .ok_or(WorkflowError::NotFound(source_workflow_id))?;
        if source.status != WorkflowStatus::Completed.as_str() {
            return Err(WorkflowError::NotRunning(source_workflow_id));
        }

        // Build the evaluation snapshot: goal + latest implementation summary.
        let steps = self.steps.list_for(source_workflow_id).await?;
        let mut summary = None;
        let mut source_task_id = None;
        for step in steps.iter().rev() {
            if step.status != WorkflowStepStatus::Completed.as_str() {
                continue;
            }
            if WorkflowRole::from_str(&step.role) != Some(WorkflowRole::Implementer) {
                continue;
            }
            summary = step.summary.clone();
            source_task_id = step.task_id;
            break;
        }
        let snapshot = format!(
            "Evaluate the implementation of workflow {source_workflow_id} against the \
             original goal. The implementation has completed; assess whether it should be \
             approved.\n\nOriginal goal:\n{goal}\n\nImplementation summary:\n{impl_summary}",
            goal = source.goal,
            impl_summary = summary.unwrap_or_else(|| "(no summary)".to_string()),
        );

        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory, self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();
        let (default_evaluators, default_quorum, _) = self.evaluation_settings();
        let evaluators = evaluators_override
            .unwrap_or(default_evaluators)
            .clamp(1, agentmesh_orchestrator::dag::MAX_EVALUATORS);
        let quorum = quorum_override.unwrap_or(default_quorum).min(evaluators);
        let strategy = match strategy_override {
            Some("unanimous") => ConsensusStrategy::Unanimous,
            _ => ConsensusStrategy::Majority,
        };

        // Graph: evaluator_1..N → consensus_gate.
        let mut nodes = Vec::new();
        let mut evaluator_ids = Vec::new();
        for i in 0..evaluators {
            let id = format!("evaluator_{}", i + 1);
            nodes.push(WorkflowNode::new(&id, WorkflowRole::Evaluator));
            evaluator_ids.push(id);
        }
        nodes.push(WorkflowNode::with_dependencies(
            "consensus_gate",
            WorkflowRole::ConsensusGate,
            evaluator_ids,
        ));
        let graph = WorkflowGraph::new(nodes)?;

        let row = WorkflowRow {
            id: workflow_id,
            preset: "evaluation".to_string(),
            goal: snapshot.clone(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: None,
            options_json: "{}".to_string(),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            // Phase 22 §1: the evaluated workflow's source workspace drives the
            // evaluators' isolated worktrees.
            source_workspace: source.source_workspace.clone(),
        };
        self.workflows.create(&row).await?;

        let group_id = Uuid::new_v4();
        self.evaluations
            .create_group(&agentmesh_storage::EvaluationGroupRow {
                id: group_id,
                workflow_id,
                source_task_id,
                strategy: strategy.as_str().to_string(),
                quorum: quorum as i64,
                status: agentmesh_storage::evaluation_status::PENDING.to_string(),
                consensus: None,
                snapshot_hash: None,
                round: 0,
                created_at: now.clone(),
                completed_at: None,
            })
            .await?;
        for i in 0..evaluators {
            self.evaluations
                .create_member(&agentmesh_storage::EvaluationMemberRow {
                    id: Uuid::new_v4(),
                    group_id,
                    node_id: format!("evaluator_{}", i + 1),
                    agent_id: String::new(),
                    task_id: None,
                    status: agentmesh_storage::member_status::PENDING.to_string(),
                    result_json: None,
                    error: None,
                    created_at: now.clone(),
                    completed_at: None,
                })
                .await?;
        }

        let eval_seed = EvaluationConfig {
            strategy,
            quorum,
            required_evaluators: evaluators,
            source_task_id,
        };
        let run = engine.start_dag_with_graph_and_evaluation(
            "evaluation",
            &snapshot,
            graph,
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: agentmesh_orchestrator::workflow::DEFAULT_MAX_PARALLEL,
            },
            workflow_id,
            eval_seed,
        )?;
        run.set_source_workspace(source.source_workspace.map(std::path::PathBuf::from));
        self.persist_dag_plan(&run).await?;
        self.spawn_run(LiveRun::Dag(run), None, None).await;
        Ok((workflow_id, group_id))
    }

    /// Persist the evaluation-group transitions as a DAG node moves (Phase 21
    /// §10/§14/§18; Phase 22 §12-§14): member rows for evaluators, the snapshot
    /// hash after each code node (implementation / fixer) completes, the group
    /// consensus when a gate runs, and the bounded fix-loop extension when a
    /// gate requests changes. Persistence is best-effort (never fails the
    /// workflow); a budget/policy rejection fails the run explicitly.
    async fn persist_evaluation(&self, run: &DagRun, node_id: &str, status: NodeStatus) {
        let Ok(_groups) = self.evaluations.list_groups(run.workflow_id()).await else {
            return;
        };
        let Some(node) = run.graph().get(node_id).cloned() else {
            return;
        };
        match node.role {
            WorkflowRole::Candidate => {
                let workflow_id = run.workflow_id();
                if status == NodeStatus::Completed {
                    let task_id = run.node_result(node_id).and_then(|r| r.task_id);
                    let hash = self.workspace_snapshot(run, node_id).await;
                    let summary = run
                        .node_result(node_id)
                        .and_then(|r| r.handoff)
                        .map(|h| h.summary);

                    if let Ok(Some(comp_group)) =
                        self.competitions.get_group_for_workflow(workflow_id).await
                        && let Ok(Some(candidate)) = self
                            .competitions
                            .get_candidate(comp_group.id, node_id)
                            .await
                    {
                        let mut ws_id = None;
                        if let Some(tid) = task_id
                            && let Ok(Some(task)) = self.task_manager.get_task(tid).await
                            && let Some(sess_id) = task.agent_session_id
                            && let Ok(ws) = self.workspaces.workspace_for_session(sess_id).await
                        {
                            ws_id = Some(ws.id);
                        }
                        if let Some(tid) = task_id {
                            let _ = self
                                .competitions
                                .update_candidate_task_and_workspace(
                                    comp_group.id,
                                    node_id,
                                    tid,
                                    ws_id,
                                )
                                .await;
                        }
                        let _ = self
                            .competitions
                            .update_candidate_completion(
                                comp_group.id,
                                node_id,
                                "completed",
                                hash.as_deref(),
                                summary.as_deref(),
                                None,
                            )
                            .await;

                        let c_payload = serde_json::to_value(CandidateCompletedPayload {
                            group_id: comp_group.id,
                            candidate_id: node_id.to_string(),
                            agent_id: candidate.agent_id.clone(),
                            task_id,
                            workspace_id: ws_id,
                            snapshot_hash: hash.clone(),
                        })
                        .unwrap_or_default();

                        let _ = self
                            .provenance
                            .append_event(
                                Some(workflow_id),
                                event_type::CANDIDATE_COMPLETED,
                                entity_type::COMPETITION_CANDIDATE,
                                node_id,
                                None,
                                actor_type::AGENT,
                                Some(&candidate.agent_id),
                                &c_payload,
                            )
                            .await;

                        if let Some(eval_group_id) = candidate.evaluation_group_id {
                            if let Some(h) = &hash {
                                let _ = self.evaluations.set_group_snapshot(eval_group_id, h).await;
                            }
                            if let Some(tid) = task_id {
                                let _ = self
                                    .evaluations
                                    .set_group_source_task(eval_group_id, tid)
                                    .await;
                            }
                        }
                    }
                } else if status == NodeStatus::Failed
                    && let Ok(Some(comp_group)) =
                        self.competitions.get_group_for_workflow(workflow_id).await
                {
                    let _ = self
                        .competitions
                        .update_candidate_status(comp_group.id, node_id, "failed")
                        .await;
                }
            }
            WorkflowRole::Implementer | WorkflowRole::Fixer => {
                if status == NodeStatus::Completed {
                    // Record the snapshot the evaluators of THIS round will see
                    // (Phase 21 §14 / Phase 22 §12): the implementation records
                    // H1 for round 0; the fixer (reusing the same worktree)
                    // records the NEW H2 for the round-1 group — never the
                    // round-0 hash.
                    let Some(group) = self.current_evaluation_group(run.workflow_id()).await else {
                        return;
                    };
                    let hash = self.workspace_snapshot(run, node_id).await;
                    if let Some(hash) = hash {
                        let _ = self.evaluations.set_group_snapshot(group.id, &hash).await;
                    }
                    if let Some(task_id) = run.node_result(node_id).and_then(|r| r.task_id) {
                        let _ = self
                            .evaluations
                            .set_group_source_task(group.id, task_id)
                            .await;
                    }
                }
            }
            WorkflowRole::Evaluator => {
                let workflow_id = run.workflow_id();
                let group = if node_id.starts_with("eval_c") {
                    let parts: Vec<&str> = node_id.split('_').collect();
                    if parts.len() >= 2 {
                        let c_num = parts[1].trim_start_matches('c');
                        let candidate_id = format!("candidate_{c_num}");
                        let comp_group = self
                            .competitions
                            .get_group_for_workflow(workflow_id)
                            .await
                            .ok()
                            .flatten();
                        let eval_group_id = if let Some(cg) = comp_group {
                            self.competitions
                                .get_candidate(cg.id, &candidate_id)
                                .await
                                .ok()
                                .flatten()
                                .and_then(|c| c.evaluation_group_id)
                        } else {
                            None
                        };
                        if let Some(egid) = eval_group_id {
                            self.evaluations.get_group(egid).await.ok().flatten()
                        } else {
                            self.evaluator_group_for_node(workflow_id, node_id).await
                        }
                    } else {
                        self.evaluator_group_for_node(workflow_id, node_id).await
                    }
                } else {
                    self.evaluator_group_for_node(workflow_id, node_id).await
                };

                let Some(group) = group else {
                    return;
                };
                let Ok(Some(member)) = self.evaluations.member_for_node(group.id, node_id).await
                else {
                    return;
                };
                match status {
                    NodeStatus::Ready | NodeStatus::Running => {
                        let agent = run.assigned_agent(node_id).unwrap_or_default();
                        let task_id = run.node_result(node_id).and_then(|r| r.task_id);
                        let _ = self
                            .evaluations
                            .set_member_agent(member.id, &agent, task_id)
                            .await;
                        let _ = self
                            .evaluations
                            .update_member(
                                member.id,
                                agentmesh_storage::member_status::RUNNING,
                                None,
                                None,
                            )
                            .await;
                    }
                    NodeStatus::Completed => {
                        let result_json = run
                            .node_result(node_id)
                            .and_then(|r| r.review_result)
                            .map(|review| {
                                serde_json::to_string(
                                    &agentmesh_orchestrator::evaluation::EvaluationResult {
                                        verdict: review.verdict,
                                        confidence: review.confidence,
                                        summary: review.summary,
                                        issues: review.issues,
                                    },
                                )
                                .unwrap_or_default()
                            });
                        let _ = self
                            .evaluations
                            .update_member(
                                member.id,
                                agentmesh_storage::member_status::COMPLETED,
                                result_json.as_deref(),
                                None,
                            )
                            .await;

                        if let Some(res) = run.node_result(node_id).and_then(|r| r.review_result) {
                            let eval_payload = serde_json::to_value(EvaluationCompletedPayload {
                                member_id: member.id,
                                group_id: member.group_id,
                                node_id: member.node_id.clone(),
                                agent_id: member.agent_id.clone(),
                                verdict: res.verdict.key().to_string(),
                                confidence: res.confidence,
                                issue_count: res.issues.len(),
                            })
                            .unwrap_or_default();

                            let _ = self
                                .provenance
                                .append_event(
                                    Some(group.workflow_id),
                                    event_type::EVALUATION_COMPLETED,
                                    entity_type::EVALUATION_MEMBER,
                                    &member.id.to_string(),
                                    None,
                                    actor_type::AGENT,
                                    Some(&member.agent_id),
                                    &eval_payload,
                                )
                                .await;
                        }
                    }
                    NodeStatus::Failed => {
                        let error = run.node_result(node_id).and_then(|r| r.error);
                        let _ = self
                            .evaluations
                            .update_member(
                                member.id,
                                agentmesh_storage::member_status::FAILED,
                                None,
                                error.as_deref(),
                            )
                            .await;
                    }
                    _ => {}
                }
            }
            WorkflowRole::ConsensusGate => {
                if !status.is_terminal() {
                    return;
                }
                let workflow_id = run.workflow_id();
                let is_candidate_gate = node_id.starts_with("consensus_c");
                let group = if is_candidate_gate {
                    let candidate_id = node_id.replace("consensus_c", "candidate_");
                    let comp_group = self
                        .competitions
                        .get_group_for_workflow(workflow_id)
                        .await
                        .ok()
                        .flatten();
                    let eval_group_id = if let Some(cg) = comp_group {
                        self.competitions
                            .get_candidate(cg.id, &candidate_id)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|c| c.evaluation_group_id)
                    } else {
                        None
                    };
                    if let Some(egid) = eval_group_id {
                        self.evaluations.get_group(egid).await.ok().flatten()
                    } else {
                        self.current_evaluation_group(workflow_id).await
                    }
                } else {
                    self.current_evaluation_group(workflow_id).await
                };

                let Some(group) = group else {
                    return;
                };

                let gate_node = run.graph().get(node_id).cloned();
                let mut members = Vec::new();
                let mut total_count = 0usize;
                if let Some(gate_node) = gate_node {
                    for dep in &gate_node.dependencies {
                        let Some(result) = run.node_result(dep) else {
                            continue;
                        };
                        if let Some(review) = result.review_result.clone() {
                            members.push((
                                result.agent_id.clone().unwrap_or_default(),
                                agentmesh_orchestrator::evaluation::EvaluationResult {
                                    verdict: review.verdict,
                                    confidence: review.confidence,
                                    summary: review.summary,
                                    issues: review.issues,
                                },
                            ));
                        }
                        total_count += 1;
                    }
                }
                let strategy = agentmesh_orchestrator::evaluation::ConsensusStrategy::from_str(
                    &group.strategy,
                )
                .unwrap_or(agentmesh_orchestrator::evaluation::ConsensusStrategy::Majority);
                let mut consensus = agentmesh_orchestrator::evaluation::compute_consensus(
                    &members,
                    strategy,
                    group.quorum as usize,
                    total_count,
                );

                let mut group_status = if status == NodeStatus::Completed {
                    agentmesh_storage::evaluation_status::COMPLETED
                } else {
                    agentmesh_storage::evaluation_status::FAILED
                };
                if let Some(recorded) = group.snapshot_hash.clone() {
                    let current = if is_candidate_gate {
                        let candidate_id = node_id.replace("consensus_c", "candidate_");
                        self.workspace_snapshot(run, &candidate_id).await
                    } else {
                        self.implementation_snapshot(run).await
                    };
                    if let Some(current) = current
                        && current != recorded
                    {
                        consensus.outcome = ConsensusOutcome::Unavailable;
                        group_status = agentmesh_storage::evaluation_status::FAILED;
                        if is_candidate_gate {
                            let candidate_id = node_id.replace("consensus_c", "candidate_");
                            self.broadcast_to_workflow(
                                workflow_id,
                                WorkflowStreamEvent::CandidateSnapshotChanged {
                                    workflow_id,
                                    candidate_id,
                                },
                            )
                            .await;
                        } else {
                            self.broadcast_to_workflow(
                                workflow_id,
                                WorkflowStreamEvent::EvaluationSnapshotChanged {
                                    workflow_id,
                                    node_id: node_id.to_string(),
                                },
                            )
                            .await;
                            run.fail_workflow(
                                "EvaluationSnapshotChanged: the implementation workspace changed \
                                 during evaluation; the consensus is void"
                                    .to_string(),
                            );
                        }
                    }
                }
                let consensus_json = serde_json::to_string(&consensus).unwrap_or_default();
                let _ = self
                    .evaluations
                    .complete_group(group.id, group_status, &consensus_json)
                    .await;

                let consensus_payload = serde_json::to_value(ConsensusComputedPayload {
                    group_id: group.id,
                    workflow_id: group.workflow_id,
                    candidate_id: if is_candidate_gate {
                        Some(node_id.replace("consensus_c", "candidate_"))
                    } else {
                        None
                    },
                    round: group.round as usize,
                    outcome: consensus.outcome.as_str().to_string(),
                    approved_count: consensus.approved_count,
                    changes_requested_count: consensus.changes_requested_count,
                    total_issues: consensus.issues.len(),
                })
                .unwrap_or_default();

                let _ = self
                    .provenance
                    .append_event(
                        Some(group.workflow_id),
                        event_type::CONSENSUS_COMPUTED,
                        entity_type::EVALUATION_GROUP,
                        &group.id.to_string(),
                        None,
                        actor_type::SYSTEM,
                        Some("ConsensusGate"),
                        &consensus_payload,
                    )
                    .await;

                if !is_candidate_gate && consensus.outcome == ConsensusOutcome::ChangesRequested {
                    self.maybe_extend_fix_round(run, node_id, &group).await;
                }
            }
            WorkflowRole::SelectionGate => {
                if !status.is_terminal() {
                    return;
                }
                let workflow_id = run.workflow_id();
                let Some(comp_group) = self
                    .competitions
                    .get_group_for_workflow(workflow_id)
                    .await
                    .ok()
                    .flatten()
                else {
                    return;
                };
                if status == NodeStatus::Completed {
                    let result = run.node_result(node_id);
                    if let Some(res) = result {
                        let reason = res.reason.as_deref().unwrap_or("");
                        let winner_candidate_id = if let Some(rest) = reason.strip_prefix("winner ")
                        {
                            rest.trim().to_string()
                        } else {
                            "candidate_1".to_string()
                        };
                        if let Ok(Some(cand)) = self
                            .competitions
                            .get_candidate(comp_group.id, &winner_candidate_id)
                            .await
                        {
                            let _ = self
                                .competitions
                                .set_group_winner(
                                    comp_group.id,
                                    &cand.candidate_id,
                                    cand.task_id,
                                    cand.workspace_id,
                                    cand.snapshot_hash.as_deref(),
                                )
                                .await;
                            let _ = self
                                .competitions
                                .set_group_status(comp_group.id, "completed")
                                .await;

                            let win_payload = serde_json::to_value(WinnerSelectedPayload {
                                group_id: comp_group.id,
                                workflow_id,
                                winner_candidate_id: cand.candidate_id.clone(),
                                winner_agent_id: cand.agent_id.clone(),
                                winner_task_id: cand.task_id,
                                winner_workspace_id: cand.workspace_id,
                                winner_snapshot_hash: cand.snapshot_hash.clone(),
                                candidate_rankings: Vec::new(),
                                selection_reason: reason.to_string(),
                            })
                            .unwrap_or_default();

                            let _ = self
                                .provenance
                                .append_event(
                                    Some(workflow_id),
                                    event_type::WINNER_SELECTED,
                                    entity_type::COMPETITION_GROUP,
                                    &comp_group.id.to_string(),
                                    None,
                                    actor_type::SYSTEM,
                                    Some("SelectionGate"),
                                    &win_payload,
                                )
                                .await;
                        }
                    }
                } else {
                    let _ = self
                        .competitions
                        .set_group_status(comp_group.id, "failed")
                        .await;
                }
            }
            _ => {}
        }
    }

    /// Phase 22 §7-§9/§16-§17: after a ChangesRequested gate, extend the live
    /// DAG with one consensus fix round —
    /// `previous_gate → fix_rN → evaluator_rN_* → consensus_gate_rN`. The
    /// graph is hot-reloaded (Phase 19) without rebuilding the workflow;
    /// completed nodes are never modified. A policy or budget violation fails
    /// the workflow with `EvaluationBudgetExceeded` before any node is
    /// appended.
    async fn maybe_extend_fix_round(
        &self,
        run: &DagRun,
        gate_node_id: &str,
        group: &agentmesh_storage::EvaluationGroupRow,
    ) {
        let workflow_id = run.workflow_id();
        let Ok(Some(row)) = self.workflows.get(workflow_id).await else {
            return;
        };
        let options: WorkflowOptions = serde_json::from_str(&row.options_json).unwrap_or_default();
        // §8: reuse `max_review_rounds` — one bounded fix round per unit.
        if group.round >= options.max_review_rounds as i64 {
            return;
        }
        // Only a consensus-review workflow (with an implementation node)
        // extends; a standalone evaluation has nothing to fix.
        let current = run.graph();
        if current.get("implementation").is_none() {
            return;
        }
        let next_round = group.round + 1;
        let (evaluators, _, _) = self.evaluation_settings();
        let fixer_id = format!("fix_r{next_round}");
        let gate_id = format!("consensus_gate_r{next_round}");

        let mut nodes = current.nodes.clone();
        nodes.push(WorkflowNode::with_dependencies(
            &fixer_id,
            WorkflowRole::Fixer,
            vec![gate_node_id.to_string(), "implementation".to_string()],
        ));
        let mut evaluator_ids = Vec::new();
        for i in 0..evaluators {
            let id = format!("evaluator_r{next_round}_{}", i + 1);
            nodes.push(WorkflowNode::with_dependencies(
                &id,
                WorkflowRole::Evaluator,
                vec![fixer_id.clone()],
            ));
            evaluator_ids.push(id);
        }
        nodes.push(WorkflowNode::with_dependencies(
            &gate_id,
            WorkflowRole::ConsensusGate,
            evaluator_ids,
        ));
        let candidate = match WorkflowGraph::new(nodes) {
            Ok(graph) => graph,
            Err(err) => {
                run.fail_workflow(format!("consensus fix round rejected: {err}"));
                return;
            }
        };

        // §17: candidate graph → Validator (done above) → Policy → Budget. Any
        // violation fails the workflow; the graph is never half-extended. The
        // roles are control-plane preset roles, so only the node-count caps
        // apply.
        let policy = self.policy_engine();
        if let Err(violation) = policy.check_graph_counts(&candidate) {
            run.fail_workflow(format!("EvaluationBudgetExceeded: {violation}"));
            return;
        }
        let evaluator_calls = candidate
            .nodes
            .iter()
            .filter(|n| n.role == WorkflowRole::Evaluator)
            .count();
        let max_total = self.evaluation_max_total_calls();
        if evaluator_calls > max_total {
            run.fail_workflow(format!(
                "EvaluationBudgetExceeded: the fix loop would need {evaluator_calls} evaluator calls but the limit is {max_total}"
            ));
            return;
        }

        // Persist the round-N group + members BEFORE the graph swap so a crash
        // mid-extension still resumes consistently (Phase 22 §14).
        let now = chrono::Utc::now().to_rfc3339();
        let group_id = Uuid::new_v4();
        let _ = self
            .evaluations
            .create_group(&agentmesh_storage::EvaluationGroupRow {
                id: group_id,
                workflow_id,
                source_task_id: None,
                strategy: group.strategy.clone(),
                quorum: group.quorum,
                status: agentmesh_storage::evaluation_status::PENDING.to_string(),
                consensus: None,
                snapshot_hash: None,
                round: next_round,
                created_at: now.clone(),
                completed_at: None,
            })
            .await;
        for i in 0..evaluators {
            let _ = self
                .evaluations
                .create_member(&agentmesh_storage::EvaluationMemberRow {
                    id: Uuid::new_v4(),
                    group_id,
                    node_id: format!("evaluator_r{next_round}_{}", i + 1),
                    agent_id: String::new(),
                    task_id: None,
                    status: agentmesh_storage::member_status::PENDING.to_string(),
                    result_json: None,
                    error: None,
                    created_at: now.clone(),
                    completed_at: None,
                })
                .await;
        }

        // §10/§19/§25: the fixer reuses the implementer's session/worktree, so
        // it is preassigned the implementer's agent. §11: each round re-selects
        // its evaluators independently (agents may repeat across rounds, never
        // within one).
        let implementer_agent = run.assigned_agent("implementation");
        if let Some(agent) = implementer_agent.clone() {
            run.record_assignment(&fixer_id, &agent);
        }
        run.reset_used_agents();

        if let Err(err) = self
            .apply_fix_graph(run, &candidate, &fixer_id, implementer_agent.as_deref())
            .await
        {
            run.fail_workflow(format!("consensus fix round could not be persisted: {err}"));
        }
    }

    /// Atomically swap in a fix-loop graph: replace the persisted node rows +
    /// dependency edges, bump `graph_revision`, and hot-reload the live
    /// scheduler (Phase 19 mechanics; no replan row — the fix loop is
    /// workflow-internal). New fixer rows carry the preassigned agent so a
    /// crash before dispatch still resumes it.
    async fn apply_fix_graph(
        &self,
        run: &DagRun,
        graph: &WorkflowGraph,
        fixer_id: &str,
        fixer_agent: Option<&str>,
    ) -> Result<i64, WorkflowError> {
        let workflow_id = run.workflow_id();
        let lock = self.node_write_guard(workflow_id).await;
        let _guard = lock.lock().await;
        let (mut rows, edges) = self.build_graph_rows(workflow_id, graph).await?;
        if let Some(agent) = fixer_agent
            && let Some(row) = rows
                .iter_mut()
                .find(|r| r.node_id.as_deref() == Some(fixer_id))
        {
            row.agent_id = Some(agent.to_string());
        }
        self.steps.replace_graph(workflow_id, &rows, &edges).await?;
        let revision = self.workflows.increment_graph_revision(workflow_id).await?;
        run.reload_graph(graph.clone());
        Ok(revision)
    }

    /// The snapshot hash of a node's workspace (git-based).
    async fn workspace_snapshot(&self, run: &DagRun, node_id: &str) -> Option<String> {
        let result = run.node_result(node_id)?;
        let task_id = result.task_id?;
        let task = self.task_manager.get_task(task_id).await.ok()??;
        let session_id = task.agent_session_id?;
        let workspace = self
            .workspaces
            .workspace_for_session(session_id)
            .await
            .ok()?;
        let diff = self.workspaces.diff(&workspace).await.ok()?;
        Some(agentmesh_workspace::workspace_snapshot_hash(
            &workspace.path,
            &diff,
        ))
    }

    /// The snapshot hash of the latest completed coding-workspace node
    /// (Implementer or Fixer). The fixer reuses the implementation's
    /// session/worktree, so either node reports the current workspace hash —
    /// H1 during round 0, H2 after the fixer (Phase 22 §12).
    async fn implementation_snapshot(&self, run: &DagRun) -> Option<String> {
        let graph = run.graph();
        let mut candidates: Vec<(usize, String)> = graph
            .nodes
            .iter()
            .filter_map(|n| {
                let is_code = matches!(n.role, WorkflowRole::Implementer | WorkflowRole::Fixer);
                if !is_code {
                    return None;
                }
                let result = run.node_result(&n.node_id)?;
                if result.status != WorkflowStepStatus::Completed {
                    return None;
                }
                Some((run.node_index(&n.node_id)?, n.node_id.clone()))
            })
            .collect();
        candidates.sort();
        let (_, node_id) = candidates.last()?;
        self.workspace_snapshot(run, node_id).await
    }

    /// The current evaluation group of a workflow: the highest round (Phase 22
    /// §13). Round-0 groups evaluate the implementation; round-1 groups the
    /// fixer.
    async fn current_evaluation_group(
        &self,
        workflow_id: Uuid,
    ) -> Option<agentmesh_storage::EvaluationGroupRow> {
        let groups = self.evaluations.list_groups(workflow_id).await.ok()?;
        groups.into_iter().max_by_key(|g| g.round)
    }

    /// The evaluation group whose members include the given evaluator node —
    /// matched by its consensus round (Phase 22 §11).
    async fn evaluator_group_for_node(
        &self,
        workflow_id: Uuid,
        node_id: &str,
    ) -> Option<agentmesh_storage::EvaluationGroupRow> {
        let round = evaluator_round(node_id);
        let groups = self.evaluations.list_groups(workflow_id).await.ok()?;
        groups.into_iter().find(|g| g.round == round)
    }

    /// The `[evaluation]` max_total_evaluator_calls (Phase 22 §16): the total
    /// evaluator agent calls allowed across ALL consensus fix rounds (default
    /// 6 = 3 evaluators × 2 rounds). An injected override (tests) wins.
    fn evaluation_max_total_calls(&self) -> usize {
        if let Some(override_) = self.evaluation_override.read().unwrap().as_ref()
            && let Some(value) = override_.max_total_evaluator_calls
        {
            return value;
        }
        let config = agentmesh_core::AgentMeshConfig::load();
        config
            .evaluation
            .as_ref()
            .and_then(|e| e.max_total_evaluator_calls)
            .or(agentmesh_core::EvaluationConfig::defaults().max_total_evaluator_calls)
            .unwrap_or(6)
    }

    /// The plan policy engine (Phase 22 §17 reuses the `[planner.policy]`
    /// limits for the fix-loop candidate graph).
    fn policy_engine(&self) -> PlanPolicyEngine {
        let config = agentmesh_core::AgentMeshConfig::load();
        let policy = config
            .planner
            .as_ref()
            .and_then(|p| p.policy.as_ref())
            .map(PlanPolicy::from_config)
            .unwrap_or_default();
        PlanPolicyEngine::new(policy)
    }

    /// Phase 22 §3: canonicalize an explicit source workspace and verify it is
    /// a git repository (so isolated worktrees can be created from it). `None`
    /// keeps the legacy daemon-cwd behavior for old workflows.
    async fn validate_source_workspace(
        &self,
        source: Option<String>,
    ) -> Result<Option<String>, WorkflowError> {
        let Some(path) = source else {
            return Ok(None);
        };
        let path = std::path::PathBuf::from(&path);
        let canonical = path.canonicalize().map_err(|err| {
            WorkflowError::InvalidSourceWorkspace(path.display().to_string(), err.to_string())
        })?;
        if !canonical.is_dir() {
            return Err(WorkflowError::InvalidSourceWorkspace(
                path.display().to_string(),
                "not a directory".to_string(),
            ));
        }
        self.workspaces
            .discover_repository(&canonical)
            .await
            .map_err(|err| {
                WorkflowError::InvalidSourceWorkspace(path.display().to_string(), err.to_string())
            })?;
        Ok(Some(canonical.display().to_string()))
    }

    /// The evaluation groups of a workflow, newest first (Phase 21 CLI).
    pub async fn evaluation_groups(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<agentmesh_storage::EvaluationGroupRow>, WorkflowError> {
        Ok(self.evaluations.list_groups(workflow_id).await?)
    }

    /// One evaluation group with its members.
    pub async fn evaluation_group(
        &self,
        group_id: Uuid,
    ) -> Result<Option<agentmesh_storage::EvaluationGroupRow>, WorkflowError> {
        Ok(self.evaluations.get_group(group_id).await?)
    }

    /// The members of an evaluation group.
    pub async fn evaluation_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<agentmesh_storage::EvaluationMemberRow>, WorkflowError> {
        Ok(self.evaluations.list_members(group_id).await?)
    }

    /// Start a plan-executed DAG workflow from an explicit graph (Phase 17).
    ///
    /// The graph comes from a validated [`WorkflowPlan`]; the plan's own
    /// `workflow_id` is bound by the planner service. Persists node rows +
    /// dependency edges so a crash before any node starts is still resumable,
    /// then runs it in the background. Phase 22 §4: `source_workspace` is
    /// explicit execution input — it never lives in the plan JSON.
    pub async fn start_from_graph(
        self: &Arc<Self>,
        goal: &str,
        graph: WorkflowGraph,
        options: WorkflowOptions,
        source_workspace: Option<String>,
    ) -> Result<Uuid, WorkflowError> {
        let source = self.validate_source_workspace(source_workspace).await?;
        let directory = self.directory()?;
        let engine = WorkflowEngine::new(directory, self.router.clone());
        let workflow_id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();

        let row = WorkflowRow {
            id: workflow_id,
            preset: "plan".to_string(),
            goal: goal.to_string(),
            status: WorkflowStatus::Pending.as_str().to_string(),
            context_id: None,
            options_json: serde_json::to_string(&options).unwrap_or_else(|_| "{}".to_string()),
            review_rounds: 0,
            runtime_owner: Some(self.instance_id.to_string()),
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now,
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: source.clone(),
        };
        self.workflows.create(&row).await?;

        let p_payload = serde_json::to_value(WorkflowStartedPayload {
            workflow_id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            source_workspace: row.source_workspace.clone(),
            base_revision: None,
            policy: PolicySnapshot::default(),
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_STARTED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::SYSTEM,
                Some("WorkflowService"),
                &p_payload,
            )
            .await;

        let run = engine.start_dag_with_graph("plan", goal, graph, options, workflow_id)?;
        run.set_source_workspace(source.map(std::path::PathBuf::from));
        self.persist_dag_plan(&run).await?;
        self.spawn_run(LiveRun::Dag(run), None, None).await;
        Ok(workflow_id)
    }

    /// Resume an interrupted workflow: rebuild the plan + context from the
    /// persisted completed steps/nodes and continue in the background.
    pub async fn resume(self: &Arc<Self>, workflow_id: Uuid) -> Result<(), WorkflowError> {
        let directory = self.directory()?;
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(WorkflowError::NotFound(workflow_id))?;
        let status = WorkflowStatus::from_str(&row.status)
            .ok_or_else(|| WorkflowError::InvalidState(format!("unknown status {}", row.status)))?;
        if status != WorkflowStatus::Interrupted {
            return Err(WorkflowError::NotResumable(workflow_id, row.status));
        }

        let res_payload = serde_json::to_value(WorkflowResumedPayload {
            workflow_id,
            from_status: row.status.clone(),
            resumed_nodes: Vec::new(),
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_RESUMED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::USER,
                None,
                &res_payload,
            )
            .await;

        let options: WorkflowOptions = serde_json::from_str(&row.options_json).unwrap_or_default();
        let engine = WorkflowEngine::new(directory, self.router.clone());

        // Claim ownership and flip the workflow back to Running.
        self.workflows
            .update_status(workflow_id, WorkflowStatus::Running.as_str(), None)
            .await?;
        self.workflows
            .set_owner(workflow_id, &self.instance_id.to_string())
            .await?;

        // A plan-executed workflow rebuilds its graph from the stored plan
        // (objectives preserved); a preset DAG rebuilds it from the preset. A
        // replanned workflow (graph_revision > 1) never matches its original
        // plan, so its graph comes from the persisted rows instead. A workflow
        // is a DAG when it has node rows (which covers plan-executed, preset
        // and replanned workflows alike).
        let plan_graph = self.plan_graph_for(workflow_id).await?;
        let graph_override = if row.graph_revision > 1 {
            None
        } else {
            plan_graph.clone()
        };
        let step_rows = self.steps.list_for(workflow_id).await?;
        let is_dag = step_rows.iter().any(|s| s.node_id.is_some());
        if is_dag {
            let seed = self.rebuild_dag_seed(workflow_id, graph_override).await?;
            // The run starts from exactly the seed graph, so `apply_resume`'s
            // seed-vs-run comparison always matches.
            let run_graph = seed.graph.clone();
            let run = engine.start_dag_with_graph(
                &row.preset,
                &row.goal,
                run_graph,
                options,
                workflow_id,
            )?;
            // Phase 22 §3: the source workspace is persisted and immutable —
            // resume restores it from the row, never from cwd or agent output.
            run.set_source_workspace(row.source_workspace.clone().map(std::path::PathBuf::from));
            // Phase 21 §20: restore the evaluation config from the persisted
            // group, so the resumed gate uses the same strategy/quorum.
            if let Ok(groups) = self.evaluations.list_groups(workflow_id).await
                && let Some(group) = groups.first()
            {
                let members = self
                    .evaluations
                    .list_members(group.id)
                    .await
                    .unwrap_or_default();
                run.set_evaluation_config(EvaluationConfig {
                    strategy: ConsensusStrategy::from_str(&group.strategy)
                        .unwrap_or(ConsensusStrategy::Majority),
                    quorum: group.quorum as usize,
                    required_evaluators: members.len().max(1),
                    source_task_id: group.source_task_id,
                });
            }
            // Phase 23: restore competition candidate agents, evaluator agents, and session lanes.
            if let Ok(Some(comp_group)) =
                self.competitions.get_group_for_workflow(workflow_id).await
                && let Ok(candidates) = self
                    .competitions
                    .list_candidates_for_group(comp_group.id)
                    .await
            {
                for cand in &candidates {
                    run.assign_agent(&cand.candidate_id, &cand.agent_id);
                    run.assign_lane(&cand.candidate_id, &cand.session_lane);
                    if let Some(eval_group_id) = cand.evaluation_group_id
                        && let Ok(members) = self.evaluations.list_members(eval_group_id).await
                    {
                        for member in &members {
                            if !member.agent_id.is_empty() {
                                run.assign_agent(&member.node_id, &member.agent_id);
                            }
                            let eval_lane = format!(
                                "evaluation:{}:{}:{}",
                                comp_group.id, cand.candidate_id, member.node_id
                            );
                            run.assign_lane(&member.node_id, &eval_lane);
                        }
                    }
                }
            }
            self.spawn_run(LiveRun::Dag(run), None, Some(seed)).await;
        } else {
            let completed = self.rebuild_completed(&step_rows)?;
            let previous = self.rebuild_previous(&completed).await?;
            let review_rounds = completed
                .iter()
                .filter(|s| s.step.role.is_reviewer())
                .filter(|s| {
                    s.review_result
                        .as_ref()
                        .map(|r| r.verdict == ReviewVerdict::ChangesRequested)
                        .unwrap_or(false)
                })
                .count();
            let seed = WorkflowResumeSeed {
                completed,
                previous,
                review_rounds,
                context_id: row.context_id,
            };
            let run = engine.start_with_id(&row.preset, &row.goal, options, workflow_id)?;
            run.set_source_workspace(row.source_workspace.clone().map(std::path::PathBuf::from));
            self.spawn_run(LiveRun::Sequential(run), Some(seed), None)
                .await;
        }
        Ok(())
    }

    /// Cancel a running workflow (cancels the active A2A task(s) through the
    /// executor; the run persists the Cancelled state).
    pub async fn cancel(&self, workflow_id: Uuid) -> Result<(), WorkflowError> {
        let c_payload = serde_json::to_value(WorkflowCancelledPayload {
            workflow_id,
            reason: None,
        })
        .unwrap_or_default();

        let _ = self
            .provenance
            .append_event(
                Some(workflow_id),
                event_type::WORKFLOW_CANCELLED,
                entity_type::WORKFLOW,
                &workflow_id.to_string(),
                None,
                actor_type::USER,
                None,
                &c_payload,
            )
            .await;

        let live = self
            .live
            .read()
            .await
            .get(&workflow_id)
            .cloned()
            .ok_or(WorkflowError::NotRunning(workflow_id))?;
        match &live.run {
            LiveRun::Sequential(run) => run.cancel().await,
            LiveRun::Dag(run) => run.cancel().await,
        }
        Ok(())
    }

    /// Graceful shutdown (Phase 13): interrupt every live workflow, then wait
    /// (bounded) until each run has persisted its `Interrupted` state.
    pub async fn shutdown_interrupt(&self) {
        let ids: Vec<Uuid> = self.live.read().await.keys().copied().collect();
        for id in &ids {
            if let Some(live) = self.live.read().await.get(id).cloned() {
                match &live.run {
                    LiveRun::Sequential(run) => run.interrupt().await,
                    LiveRun::Dag(run) => run.interrupt().await,
                }
            }
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let mut still_live = false;
            for id in &ids {
                if self.live.read().await.contains_key(id) {
                    still_live = true;
                    break;
                }
            }
            if !still_live || std::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// Mark every running workflow (and its running steps) as `Interrupted`.
    pub async fn recover_interrupted(&self) -> Result<usize, WorkflowError> {
        let reason = "AgentMesh daemon terminated during workflow execution.";
        let count = self.workflows.recover_interrupted(reason).await?;
        let rows = self.workflows.list().await?;
        let interrupted: Vec<Uuid> = rows
            .iter()
            .filter(|r| r.status == WorkflowStatus::Interrupted.as_str())
            .map(|r| r.id)
            .collect();
        if !interrupted.is_empty() {
            self.steps
                .recover_interrupted_for(&interrupted, reason)
                .await?;
        }
        Ok(count)
    }

    /// Spawn the background executor for a fresh or resumed run.
    async fn spawn_run(
        self: &Arc<Self>,
        run: LiveRun,
        resume_seq: Option<WorkflowResumeSeed>,
        resume_dag: Option<DagResumeSeed>,
    ) {
        let workflow_id = run.workflow_id();
        let (events, _) = broadcast::channel(256);
        let live = Arc::new(LiveWorkflow {
            run,
            events: events.clone(),
        });
        self.live.write().await.insert(workflow_id, live.clone());

        let service = self.clone();
        tokio::spawn(async move {
            match &live.run {
                LiveRun::Sequential(run) => {
                    let persister = DbWorkflowPersister::new(service.clone(), workflow_id);
                    let observer = WorkflowStreamObserver {
                        workflow_id,
                        events: events.clone(),
                    };
                    let result = run
                        .run_to_completion_with(&observer, resume_seq.as_ref(), Some(&persister))
                        .await;
                    tracing::info!(workflow_id = %workflow_id, status = ?result.status, "workflow run finished");
                }
                LiveRun::Dag(run) => {
                    let persister = DbDagPersister::new(service.clone(), workflow_id);
                    let observer: Arc<dyn agentmesh_orchestrator::WorkflowObserver> =
                        Arc::new(WorkflowStreamObserver {
                            workflow_id,
                            events: events.clone(),
                        });
                    let result = run
                        .run_to_completion_with(observer, resume_dag, Some(Arc::new(persister)))
                        .await;
                    tracing::info!(workflow_id = %workflow_id, status = ?result.status, "dag workflow run finished");
                }
            }
            service.live.write().await.remove(&workflow_id);
        });
        let _ = live;
    }

    /// Heartbeat every live workflow (called by the daemon's heartbeat loop).
    pub async fn heartbeat_live(&self) {
        let ids: Vec<Uuid> = self.live.read().await.keys().copied().collect();
        for id in ids {
            if let Err(err) = self.workflows.heartbeat(id).await {
                tracing::warn!(workflow_id = %id, error = %err, "workflow heartbeat failed");
            }
        }
    }

    /// Whether a workflow is currently being driven by this daemon.
    pub async fn is_live(&self, workflow_id: Uuid) -> bool {
        self.live.read().await.contains_key(&workflow_id)
    }

    /// Subscribe to live events for a running workflow.
    pub async fn subscribe(
        &self,
        workflow_id: Uuid,
    ) -> Option<broadcast::Receiver<WorkflowStreamEvent>> {
        self.live
            .read()
            .await
            .get(&workflow_id)
            .map(|live| live.events.subscribe())
    }

    // ---------- query ----------

    /// All workflows, newest first.
    pub async fn list(&self) -> Result<Vec<WorkflowInfo>, WorkflowError> {
        let rows = self.workflows.list().await?;
        let mut out = Vec::new();
        for row in rows {
            out.push(self.info_from_row(&row).await?);
        }
        Ok(out)
    }

    /// Full detail of one workflow, including its steps.
    pub async fn get(&self, workflow_id: Uuid) -> Result<Option<WorkflowDetail>, WorkflowError> {
        let Some(row) = self.workflows.get(workflow_id).await? else {
            return Ok(None);
        };
        Ok(Some(self.detail_from_row(&row).await?))
    }

    /// The event replay for attach, reconstructed from the persisted state.
    pub async fn replay(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowStreamEvent>, WorkflowError> {
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(WorkflowError::NotFound(workflow_id))?;
        let status = WorkflowStatus::from_str(&row.status)
            .ok_or_else(|| WorkflowError::InvalidState(format!("unknown status {}", row.status)))?;
        let step_rows = self.steps.list_for(workflow_id).await?;

        let mut events = vec![WorkflowStreamEvent::WorkflowStarted {
            workflow_id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
        }];
        for step in &step_rows {
            let ordinal = step.ordinal as usize;
            let role = WorkflowRole::from_str(&step.role).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown role {}", step.role))
            })?;
            let step_status = WorkflowStepStatus::from_str(&step.status).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown step status {}", step.status))
            })?;
            let node_id = step.node_id.clone();
            // DAG workflows emit node events keyed by node_id.
            if let Some(node_id) = &node_id {
                let node_event = match step_status {
                    WorkflowStepStatus::Completed => Some(WorkflowStreamEvent::NodeCompleted {
                        workflow_id,
                        node_id: node_id.clone(),
                        role,
                    }),
                    WorkflowStepStatus::Failed => Some(WorkflowStreamEvent::NodeFailed {
                        workflow_id,
                        node_id: node_id.clone(),
                        role,
                        error: step
                            .error
                            .clone()
                            .unwrap_or_else(|| "node failed".to_string()),
                    }),
                    WorkflowStepStatus::Cancelled => Some(WorkflowStreamEvent::NodeCancelled {
                        workflow_id,
                        node_id: node_id.clone(),
                        role,
                    }),
                    WorkflowStepStatus::Skipped => Some(WorkflowStreamEvent::NodeSkipped {
                        workflow_id,
                        node_id: node_id.clone(),
                        role,
                    }),
                    WorkflowStepStatus::Running => {
                        step.agent_id
                            .clone()
                            .map(|agent_id| WorkflowStreamEvent::NodeStarted {
                                workflow_id,
                                node_id: node_id.clone(),
                                role,
                                agent_id,
                            })
                    }
                    WorkflowStepStatus::Interrupted => Some(WorkflowStreamEvent::NodeInterrupted {
                        workflow_id,
                        node_id: node_id.clone(),
                        role,
                    }),
                    WorkflowStepStatus::Pending => None,
                };
                if let Some(event) = node_event {
                    events.push(event);
                }
                continue;
            }
            match step_status {
                WorkflowStepStatus::Completed => events.push(WorkflowStreamEvent::StepCompleted {
                    workflow_id,
                    ordinal,
                    role,
                }),
                WorkflowStepStatus::Failed => events.push(WorkflowStreamEvent::StepFailed {
                    workflow_id,
                    ordinal,
                    role,
                    error: step
                        .error
                        .clone()
                        .unwrap_or_else(|| "step failed".to_string()),
                }),
                WorkflowStepStatus::Cancelled => {
                    events.push(WorkflowStreamEvent::StepCancelled {
                        workflow_id,
                        ordinal,
                        role,
                    });
                }
                WorkflowStepStatus::Skipped => events.push(WorkflowStreamEvent::StepSkipped {
                    workflow_id,
                    ordinal,
                    role,
                }),
                WorkflowStepStatus::Running => {
                    if let Some(agent_id) = &step.agent_id {
                        events.push(WorkflowStreamEvent::StepStarted {
                            workflow_id,
                            ordinal,
                            role,
                            agent_id: agent_id.clone(),
                        });
                    }
                }
                WorkflowStepStatus::Pending | WorkflowStepStatus::Interrupted => {}
            }
        }
        match status {
            WorkflowStatus::Interrupted => events.push(WorkflowStreamEvent::WorkflowInterrupted {
                workflow_id,
                reason: row.error.clone().unwrap_or_default(),
            }),
            WorkflowStatus::Completed => events.push(WorkflowStreamEvent::WorkflowCompleted {
                workflow_id,
                final_review_verdict: self.final_verdict(&step_rows),
            }),
            WorkflowStatus::Failed => events.push(WorkflowStreamEvent::WorkflowFailed {
                workflow_id,
                error: row.error.clone(),
            }),
            WorkflowStatus::Cancelled => {
                events.push(WorkflowStreamEvent::WorkflowCancelled { workflow_id })
            }
            WorkflowStatus::Pending | WorkflowStatus::Running => {}
        }
        Ok(events)
    }

    // ---------- persister / rebuild helpers ----------

    /// Build a sequential step row (legacy path).
    #[allow(clippy::too_many_arguments)]
    fn step_row(
        &self,
        workflow_id: Uuid,
        ordinal: usize,
        node_id: Option<&str>,
        step: &WorkflowStep,
        status: WorkflowStepStatus,
        result: Option<&PersistedStepResult>,
        error: Option<&str>,
    ) -> WorkflowStepRow {
        let now = chrono::Utc::now().to_rfc3339();
        WorkflowStepRow {
            id: Uuid::new_v4(),
            workflow_id,
            ordinal: ordinal as i64,
            node_id: node_id.map(str::to_string),
            role: step.role.as_str().to_string(),
            intent: step.intent.key().to_string(),
            objective: step.objective.clone(),
            status: status.as_str().to_string(),
            agent_id: result.and_then(|r| r.agent_id.clone()),
            task_id: result.and_then(|r| r.task_id),
            review_round: 0,
            summary: result.and_then(|r| r.summary.clone()),
            result_json: result
                .map(|r| serde_json::to_string(r).unwrap_or_else(|_| "{}".to_string())),
            created_at: now.clone(),
            started_at: None,
            completed_at: None,
            error: error.map(str::to_string),
        }
    }

    /// Build a DAG node row.
    #[allow(clippy::too_many_arguments)]
    fn node_row(
        &self,
        workflow_id: Uuid,
        ordinal: usize,
        node_id: &str,
        step: &WorkflowStep,
        status: NodeStatus,
        result: Option<&PersistedStepResult>,
        error: Option<&str>,
    ) -> WorkflowStepRow {
        self.step_row(
            workflow_id,
            ordinal,
            Some(node_id),
            step,
            match status {
                NodeStatus::Completed => WorkflowStepStatus::Completed,
                NodeStatus::Failed => WorkflowStepStatus::Failed,
                NodeStatus::Skipped => WorkflowStepStatus::Skipped,
                NodeStatus::Cancelled => WorkflowStepStatus::Cancelled,
                NodeStatus::Interrupted => WorkflowStepStatus::Interrupted,
                NodeStatus::Pending => WorkflowStepStatus::Pending,
                NodeStatus::Ready => WorkflowStepStatus::Pending,
                NodeStatus::Running => WorkflowStepStatus::Running,
            },
            result,
            error,
        )
    }

    /// Persist a DAG's node rows (Pending) and dependency edges up front, so a
    /// crash before any node starts still leaves a resumable workflow.
    async fn persist_dag_plan(&self, run: &Arc<DagRun>) -> Result<(), WorkflowError> {
        let workflow_id = run.workflow_id();
        let mut edges = Vec::new();
        for (ordinal, node) in run.graph().nodes.iter().enumerate() {
            let step = node.to_step();
            let row = self.node_row(
                workflow_id,
                ordinal,
                &node.node_id,
                &step,
                NodeStatus::Pending,
                None,
                None,
            );
            self.steps.upsert(&row).await?;
            for dep in &node.dependencies {
                edges.push((node.node_id.clone(), dep.clone()));
            }
        }
        self.steps.set_dependencies(workflow_id, &edges).await?;
        Ok(())
    }

    /// Upsert the row for a DAG node from the run's live state.
    async fn upsert_node_from_run(
        &self,
        run: &DagRun,
        node_id: &str,
        status: NodeStatus,
    ) -> Result<(), WorkflowError> {
        // Serialize with a replan graph replacement (Phase 19): never write a
        // node row mid-swap.
        let lock = self.node_write_guard(run.workflow_id()).await;
        let _guard = lock.lock().await;
        let Some(ordinal) = run.node_index(node_id) else {
            return Err(WorkflowError::InvalidState(format!(
                "node {node_id} not in run"
            )));
        };
        let Some(node) = run.graph().get(node_id).cloned() else {
            return Err(WorkflowError::InvalidState(format!(
                "node {node_id} not in graph"
            )));
        };
        let step = node.to_step();
        let result = run.node_result(node_id);
        let persisted = result.as_ref().map(PersistedStepResult::from);
        let mut row = self.node_row(
            run.workflow_id(),
            ordinal,
            node_id,
            &step,
            status,
            persisted.as_ref(),
            persisted.as_ref().and_then(|r| r.error.as_deref()),
        );
        // Persist the node's assigned agent even while Running (Phase 21 §20),
        // so an interrupted evaluator resumes with the same session/worktree.
        if row.agent_id.is_none() {
            row.agent_id = run.assigned_agent(node_id);
        }
        self.steps.upsert(&row).await?;
        if let Some(context_id) = run.context_id() {
            self.workflows
                .set_context(run.workflow_id(), context_id)
                .await?;
        }
        Ok(())
    }

    /// Upsert the row for a sequential step from the run's live state.
    async fn upsert_step_from_run(
        &self,
        run: &WorkflowRun,
        index: usize,
    ) -> Result<(), WorkflowError> {
        let step_results = run.step_results();
        let Some(result) = step_results.get(index) else {
            return Ok(());
        };
        let persisted = PersistedStepResult::from(result);
        let status = if persisted.status == WorkflowStepStatus::Running && run.is_cancelled() {
            WorkflowStepStatus::Cancelled
        } else {
            persisted.status
        };
        let row = self.step_row(
            run.workflow_id(),
            index,
            None,
            &persisted.step,
            status,
            Some(&persisted),
            persisted.error.as_deref(),
        );
        let review_round = step_results[..index]
            .iter()
            .filter(|s| s.step.role.is_reviewer())
            .filter(|s| {
                s.review_result
                    .as_ref()
                    .map(|r| r.verdict == ReviewVerdict::ChangesRequested)
                    .unwrap_or(false)
            })
            .count();
        let mut row = row;
        row.review_round = review_round as i64;
        if matches!(
            status,
            WorkflowStepStatus::Running
                | WorkflowStepStatus::Completed
                | WorkflowStepStatus::Failed
                | WorkflowStepStatus::Cancelled
        ) {
            row.started_at = Some(chrono::Utc::now().to_rfc3339());
        }
        if matches!(
            status,
            WorkflowStepStatus::Completed
                | WorkflowStepStatus::Failed
                | WorkflowStepStatus::Cancelled
        ) {
            row.completed_at = Some(chrono::Utc::now().to_rfc3339());
        }
        self.steps.upsert(&row).await?;
        if let Some(context_id) = run.context_id() {
            self.workflows
                .set_context(run.workflow_id(), context_id)
                .await?;
        }
        Ok(())
    }

    /// Rebuild the graph of a plan-executed workflow from its stored plan, so
    /// resume produces the exact same graph (objectives included) the run uses.
    async fn plan_graph_for(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<WorkflowGraph>, WorkflowError> {
        let Some(plan) = self.plans.by_workflow(workflow_id).await? else {
            return Ok(None);
        };
        let Some(json) = &plan.plan_json else {
            return Ok(None);
        };
        let plan: WorkflowPlan = serde_json::from_str(json)
            .map_err(|err| WorkflowError::InvalidState(format!("plan JSON corrupt: {err}")))?;
        let graph = plan
            .validate()
            .map_err(|err| WorkflowError::InvalidState(format!("stored plan invalid: {err}")))?;
        Ok(Some(graph))
    }

    /// Rebuild the DAG resume seed from persisted node rows + dependency edges.
    ///
    /// Plan-executed workflows pass `graph_override` (the plan's graph, with
    /// objectives) so the seed graph matches the run's graph exactly; preset
    /// workflows rebuild the graph from the dependency-edge table.
    async fn rebuild_dag_seed(
        &self,
        workflow_id: Uuid,
        graph_override: Option<WorkflowGraph>,
    ) -> Result<DagResumeSeed, WorkflowError> {
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(WorkflowError::NotFound(workflow_id))?;
        let step_rows = self.steps.list_for(workflow_id).await?;
        let deps = self.steps.list_dependencies(workflow_id).await?;

        let graph = {
            let mut nodes = Vec::new();
            for step in &step_rows {
                let role = WorkflowRole::from_str(&step.role).ok_or_else(|| {
                    WorkflowError::InvalidState(format!("unknown role {}", step.role))
                })?;
                let node_id = step.node_id.clone().ok_or_else(|| {
                    WorkflowError::InvalidState("resumed workflow has no node_id".to_string())
                })?;
                let intent =
                    agentmesh_core::TaskIntent::from_key(&step.intent).ok_or_else(|| {
                        WorkflowError::InvalidState(format!("unknown intent {}", step.intent))
                    })?;
                let mut node_deps: Vec<String> = deps
                    .iter()
                    .filter(|d| d.node_id == node_id)
                    .map(|d| d.depends_on_node_id.clone())
                    .collect();
                node_deps.sort();
                node_deps.dedup();
                // The objective is persisted on the row since Phase 19; a
                // legacy (NULL) row falls back to the original plan graph so
                // a pre-Phase-19 plan still resumes with its objectives. A
                // replanned graph never falls back (it no longer matches the
                // plan).
                let objective = step.objective.clone().or_else(|| {
                    graph_override
                        .as_ref()
                        .and_then(|g| g.get(&node_id))
                        .and_then(|n| n.objective.clone())
                });
                nodes.push(WorkflowNode {
                    node_id,
                    role,
                    intent,
                    dependencies: node_deps,
                    objective,
                });
            }
            agentmesh_orchestrator::WorkflowGraph::new(nodes)?
        };

        let mut completed = Vec::new();
        let mut pending = Vec::new();
        let mut interrupted = Vec::new();
        for step in &step_rows {
            let node_id = step.node_id.clone().unwrap_or_default();
            let status = NodeStatus::from_str(&step.status).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown node status {}", step.status))
            })?;
            match status {
                NodeStatus::Completed => {
                    if let Some(json) = &step.result_json
                        && let Ok(persisted) = serde_json::from_str::<PersistedStepResult>(json)
                    {
                        completed.push(persisted);
                        continue;
                    }
                    // Fallback: rebuild from the row columns.
                    let role = WorkflowRole::from_str(&step.role).ok_or_else(|| {
                        WorkflowError::InvalidState(format!("unknown role {}", step.role))
                    })?;
                    completed.push(PersistedStepResult {
                        step: WorkflowStep::new(node_id.clone(), role),
                        status: WorkflowStepStatus::Completed,
                        agent_id: step.agent_id.clone(),
                        task_id: step.task_id,
                        summary: step.summary.clone(),
                        review_result: None,
                        error: step.error.clone(),
                    });
                }
                NodeStatus::Pending | NodeStatus::Ready => pending.push(node_id),
                NodeStatus::Interrupted => interrupted.push(node_id),
                NodeStatus::Running => interrupted.push(node_id),
                NodeStatus::Failed | NodeStatus::Skipped | NodeStatus::Cancelled => {
                    // A terminal non-completed node (a consensus gate that
                    // requested changes, for example) must still be restored as
                    // a result so downstream promote checks can see it.
                    if let Some(json) = &step.result_json
                        && let Ok(mut persisted) = serde_json::from_str::<PersistedStepResult>(json)
                    {
                        if persisted.task_id.is_none() {
                            persisted.task_id = step.task_id;
                        }
                        completed.push(persisted);
                    } else {
                        let role = WorkflowRole::from_str(&step.role).ok_or_else(|| {
                            WorkflowError::InvalidState(format!("unknown role {}", step.role))
                        })?;
                        completed.push(PersistedStepResult {
                            step: WorkflowStep::new(node_id.clone(), role),
                            status: WorkflowStepStatus::from_str(&step.status)
                                .unwrap_or(WorkflowStepStatus::Failed),
                            agent_id: step.agent_id.clone(),
                            task_id: step.task_id,
                            summary: step.summary.clone(),
                            review_result: None,
                            error: step.error.clone(),
                        });
                    }
                }
            }
        }
        pending.sort();
        interrupted.sort();

        // Rebuild each completed node's handoff from its task's artifacts so
        // fan-in children resume with the same inputs.
        let mut handoffs = HashMap::new();
        for persisted in &completed {
            let Some(task_id) = persisted.task_id else {
                continue;
            };
            let artifacts = self.task_manager.list_artifacts(task_id).await?;
            let a2a: Vec<A2AArtifact> = artifacts.iter().map(to_artifact).collect();
            let handoff = build_handoff(
                task_id,
                persisted.agent_id.clone().unwrap_or_default(),
                persisted.summary.clone(),
                &a2a,
            );
            handoffs.insert(persisted.step.id.clone(), handoff);
        }

        Ok(DagResumeSeed {
            graph,
            completed,
            pending,
            interrupted,
            handoffs,
            context_id: row.context_id,
            // Preserve each node's assigned agent (Phase 21 §20) so an
            // interrupted evaluator resumes with the same session/worktree.
            agent_assignments: step_rows
                .iter()
                .filter_map(|s| s.node_id.clone().zip(s.agent_id.clone()))
                .collect(),
        })
    }

    fn rebuild_completed(
        &self,
        step_rows: &[WorkflowStepRow],
    ) -> Result<Vec<PersistedStepResult>, WorkflowError> {
        let mut completed = Vec::new();
        for row in step_rows {
            if row.status != WorkflowStepStatus::Completed.as_str() {
                continue;
            }
            let role = WorkflowRole::from_str(&row.role)
                .ok_or_else(|| WorkflowError::InvalidState(format!("unknown role {}", row.role)))?;
            if let Some(json) = &row.result_json
                && let Ok(persisted) = serde_json::from_str::<PersistedStepResult>(json)
            {
                completed.push(persisted);
                continue;
            }
            let intent = agentmesh_core::TaskIntent::from_key(&row.intent).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown intent {}", row.intent))
            })?;
            completed.push(PersistedStepResult {
                step: WorkflowStep {
                    id: role.as_str().to_string(),
                    role,
                    intent,
                    objective: None,
                },
                status: WorkflowStepStatus::from_str(&row.status)
                    .unwrap_or(WorkflowStepStatus::Completed),
                agent_id: row.agent_id.clone(),
                task_id: row.task_id,
                summary: row.summary.clone(),
                review_result: None,
                error: row.error.clone(),
            });
        }
        Ok(completed)
    }

    /// Rebuild the last completed step's handoff (summary + artifacts from the
    /// task's artifact repository).
    async fn rebuild_previous(
        &self,
        completed: &[PersistedStepResult],
    ) -> Result<Option<HandoffPackage>, WorkflowError> {
        let Some(last) = completed.last() else {
            return Ok(None);
        };
        let task_id = last.task_id.ok_or_else(|| {
            WorkflowError::InvalidState("completed step has no task id".to_string())
        })?;
        let artifacts = self.task_manager.list_artifacts(task_id).await?;
        let a2a: Vec<A2AArtifact> = artifacts.iter().map(to_artifact).collect();
        let handoff = build_handoff(
            task_id,
            last.agent_id.clone().unwrap_or_default(),
            last.summary.clone(),
            &a2a,
        );
        Ok(Some(handoff))
    }

    fn final_verdict(&self, step_rows: &[WorkflowStepRow]) -> Option<ReviewVerdict> {
        step_rows
            .iter()
            .filter(|r| r.status == WorkflowStepStatus::Completed.as_str())
            .filter_map(|r| r.result_json.as_deref())
            .filter_map(|json| serde_json::from_str::<PersistedStepResult>(json).ok())
            .filter(|r| r.step.role.is_reviewer())
            .filter_map(|r| r.review_result.map(|review| review.verdict))
            .next_back()
    }

    async fn info_from_row(&self, row: &WorkflowRow) -> Result<WorkflowInfo, WorkflowError> {
        Ok(WorkflowInfo {
            workflow_id: row.id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            status: WorkflowStatus::from_str(&row.status).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown status {}", row.status))
            })?,
            context_id: row.context_id,
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            completed_at: row.completed_at.clone(),
            error: row.error.clone(),
            graph_revision: row.graph_revision,
            parent_workflow_id: row.parent_workflow_id,
            recovery_of_node_id: row.recovery_of_node_id.clone(),
            recovery_attempt: row.recovery_attempt,
            source_workspace: row.source_workspace.clone(),
        })
    }

    async fn detail_from_row(&self, row: &WorkflowRow) -> Result<WorkflowDetail, WorkflowError> {
        let steps = self.steps.list_for(row.id).await?;
        let options: WorkflowOptions = serde_json::from_str(&row.options_json).unwrap_or_default();
        let step_infos = steps
            .iter()
            .map(|s| WorkflowStepInfo {
                ordinal: s.ordinal as usize,
                node_id: s.node_id.clone(),
                role: WorkflowRole::from_str(&s.role).unwrap_or(WorkflowRole::Architect),
                status: WorkflowStepStatus::from_str(&s.status)
                    .unwrap_or(WorkflowStepStatus::Pending),
                agent_id: s.agent_id.clone(),
                task_id: s.task_id,
                review_round: s.review_round as usize,
                summary: s.summary.clone(),
                error: s.error.clone(),
            })
            .collect();
        Ok(WorkflowDetail {
            workflow_id: row.id,
            preset: row.preset.clone(),
            goal: row.goal.clone(),
            status: WorkflowStatus::from_str(&row.status).ok_or_else(|| {
                WorkflowError::InvalidState(format!("unknown status {}", row.status))
            })?,
            context_id: row.context_id,
            max_review_rounds: options.max_review_rounds,
            max_parallel: options.effective_max_parallel(),
            final_review_verdict: self.final_verdict(&steps),
            error: row.error.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            completed_at: row.completed_at.clone(),
            graph_revision: row.graph_revision,
            parent_workflow_id: row.parent_workflow_id,
            recovery_of_node_id: row.recovery_of_node_id.clone(),
            recovery_attempt: row.recovery_attempt,
            source_workspace: row.source_workspace.clone(),
            steps: step_infos,
        })
    }
}

/// The consensus fix round an evaluator node belongs to, derived from its
/// stable node id: `evaluator_1` → 0, `evaluator_r1_1` → 1. Mirrors the
/// scheduler's own helper (Phase 22 §11/§13).
fn evaluator_round(node_id: &str) -> i64 {
    match node_id.strip_prefix("evaluator_") {
        Some(rest) if rest.starts_with('r') => rest
            .trim_start_matches('r')
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .unwrap_or(0),
        _ => 0,
    }
}

/// Persister that writes each workflow/step transition to SQLite (sequential).
struct DbWorkflowPersister {
    service: Arc<WorkflowService>,
    workflow_id: Uuid,
}

impl DbWorkflowPersister {
    fn new(service: Arc<WorkflowService>, workflow_id: Uuid) -> Self {
        Self {
            service,
            workflow_id,
        }
    }

    async fn upsert_step(&self, run: &WorkflowRun, index: usize) {
        if let Err(err) = self.service.upsert_step_from_run(run, index).await {
            tracing::error!(
                workflow_id = %self.workflow_id,
                ordinal = index,
                error = %err,
                "failed to persist workflow step"
            );
        }
    }
}

#[async_trait]
impl WorkflowPersister for DbWorkflowPersister {
    async fn on_workflow_started(&self, run: &WorkflowRun) {
        if let Err(err) = self
            .service
            .workflows
            .update_status(self.workflow_id, WorkflowStatus::Running.as_str(), None)
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to mark workflow running");
        }
        if let Err(err) = self
            .service
            .workflows
            .set_owner(self.workflow_id, &self.service.instance_id.to_string())
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to claim workflow owner");
        }
        if let Some(context_id) = run.context_id() {
            let _ = self
                .service
                .workflows
                .set_context(self.workflow_id, context_id)
                .await;
        }
    }

    async fn on_step_started(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_task(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_completed(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_failed(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_cancelled(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_interrupted(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_step_skipped(&self, run: &WorkflowRun, index: usize) {
        self.upsert_step(run, index).await;
    }

    async fn on_workflow_finished(&self, run: &WorkflowRun, result: &WorkflowResult) {
        if let Err(err) = self
            .service
            .workflows
            .mark_completed(
                self.workflow_id,
                result.status.as_str(),
                result.error.as_deref(),
            )
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to mark workflow terminal");
        }

        let term_payload = match result.status {
            WorkflowStatus::Completed => serde_json::to_value(WorkflowCompletedPayload {
                workflow_id: self.workflow_id,
                final_review_verdict: result.final_review_verdict.map(|v| v.key().to_string()),
                winner_candidate_id: None,
            })
            .unwrap_or_default(),
            WorkflowStatus::Failed => serde_json::to_value(WorkflowFailedPayload {
                workflow_id: self.workflow_id,
                error: result
                    .error
                    .clone()
                    .unwrap_or_else(|| "workflow failed".to_string()),
            })
            .unwrap_or_default(),
            WorkflowStatus::Cancelled => serde_json::to_value(WorkflowCancelledPayload {
                workflow_id: self.workflow_id,
                reason: result.error.clone(),
            })
            .unwrap_or_default(),
            _ => serde_json::Value::Null,
        };

        let event_t = match result.status {
            WorkflowStatus::Completed => Some(event_type::WORKFLOW_COMPLETED),
            WorkflowStatus::Failed => Some(event_type::WORKFLOW_FAILED),
            WorkflowStatus::Cancelled => Some(event_type::WORKFLOW_CANCELLED),
            _ => None,
        };

        if let Some(et) = event_t {
            let _ = self
                .service
                .provenance
                .append_event(
                    Some(self.workflow_id),
                    et,
                    entity_type::WORKFLOW,
                    &self.workflow_id.to_string(),
                    None,
                    actor_type::SYSTEM,
                    Some("WorkflowService"),
                    &term_payload,
                )
                .await;
        }

        if result.status == WorkflowStatus::Failed {
            self.service.notify_failed(self.workflow_id).await;
        }
        self.service
            .notify_recovery_terminal(self.workflow_id, result.status, result.error.clone())
            .await;
        let _ = run;
    }

    async fn on_heartbeat(&self, _run: &WorkflowRun) {
        let _ = self.service.workflows.heartbeat(self.workflow_id).await;
    }
}

/// Persister that writes each DAG node transition to SQLite (Phase 16).
struct DbDagPersister {
    service: Arc<WorkflowService>,
    workflow_id: Uuid,
}

impl DbDagPersister {
    fn new(service: Arc<WorkflowService>, workflow_id: Uuid) -> Self {
        Self {
            service,
            workflow_id,
        }
    }

    async fn upsert_node(&self, run: &DagRun, node_id: &str, status: NodeStatus) {
        if let Err(err) = self
            .service
            .upsert_node_from_run(run, node_id, status)
            .await
        {
            tracing::error!(
                workflow_id = %self.workflow_id,
                node_id = node_id,
                status = %status.as_str(),
                error = %err,
                "failed to persist dag node"
            );
        }
    }
}

#[async_trait]
impl DagPersister for DbDagPersister {
    async fn on_workflow_started(&self, _run: &DagRun) {
        if let Err(err) = self
            .service
            .workflows
            .update_status(self.workflow_id, WorkflowStatus::Running.as_str(), None)
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to mark workflow running");
        }
        if let Err(err) = self
            .service
            .workflows
            .set_owner(self.workflow_id, &self.service.instance_id.to_string())
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to claim workflow owner");
        }
    }

    async fn on_node_status(&self, run: &DagRun, node_id: &str, status: NodeStatus) {
        self.upsert_node(run, node_id, status).await;
        self.service.persist_evaluation(run, node_id, status).await;
    }

    async fn on_node_task(&self, run: &DagRun, node_id: &str) {
        self.upsert_node(run, node_id, run.node_status(node_id))
            .await;
        self.service
            .persist_evaluation(run, node_id, run.node_status(node_id))
            .await;
    }

    async fn on_workflow_finished(&self, _run: &DagRun, result: &WorkflowResult) {
        tracing::info!(workflow_id = %self.workflow_id, status = ?result.status, error = ?result.error, "dag workflow run finished");
        if let Err(err) = self
            .service
            .workflows
            .mark_completed(
                self.workflow_id,
                result.status.as_str(),
                result.error.as_deref(),
            )
            .await
        {
            tracing::error!(workflow_id = %self.workflow_id, error = %err, "failed to mark workflow terminal");
        }

        let term_payload = match result.status {
            WorkflowStatus::Completed => serde_json::to_value(WorkflowCompletedPayload {
                workflow_id: self.workflow_id,
                final_review_verdict: result.final_review_verdict.map(|v| v.key().to_string()),
                winner_candidate_id: None,
            })
            .unwrap_or_default(),
            WorkflowStatus::Failed => serde_json::to_value(WorkflowFailedPayload {
                workflow_id: self.workflow_id,
                error: result
                    .error
                    .clone()
                    .unwrap_or_else(|| "workflow failed".to_string()),
            })
            .unwrap_or_default(),
            WorkflowStatus::Cancelled => serde_json::to_value(WorkflowCancelledPayload {
                workflow_id: self.workflow_id,
                reason: result.error.clone(),
            })
            .unwrap_or_default(),
            _ => serde_json::Value::Null,
        };

        let event_t = match result.status {
            WorkflowStatus::Completed => Some(event_type::WORKFLOW_COMPLETED),
            WorkflowStatus::Failed => Some(event_type::WORKFLOW_FAILED),
            WorkflowStatus::Cancelled => Some(event_type::WORKFLOW_CANCELLED),
            _ => None,
        };

        if let Some(et) = event_t {
            let _ = self
                .service
                .provenance
                .append_event(
                    Some(self.workflow_id),
                    et,
                    entity_type::WORKFLOW,
                    &self.workflow_id.to_string(),
                    None,
                    actor_type::SYSTEM,
                    Some("WorkflowService"),
                    &term_payload,
                )
                .await;
        }

        if result.status == WorkflowStatus::Failed {
            self.service.notify_failed(self.workflow_id).await;
        }
        self.service
            .notify_recovery_terminal(self.workflow_id, result.status, result.error.clone())
            .await;
    }

    async fn on_heartbeat(&self, _run: &DagRun) {
        let _ = self.service.workflows.heartbeat(self.workflow_id).await;
    }
}

/// Observer that broadcasts workflow stream events.
struct WorkflowStreamObserver {
    workflow_id: Uuid,
    events: broadcast::Sender<WorkflowStreamEvent>,
}

impl WorkflowStreamObserver {
    fn send(&self, event: WorkflowStreamEvent) {
        let _ = self.events.send(event);
    }
}

impl agentmesh_orchestrator::workflow::WorkflowObserver for WorkflowStreamObserver {
    fn on_step_start(&self, index: usize, _total: usize, step: &WorkflowStep, agent_id: &str) {
        self.send(WorkflowStreamEvent::StepStarted {
            workflow_id: self.workflow_id,
            ordinal: index,
            role: step.role,
            agent_id: agent_id.to_string(),
        });
    }

    fn on_agent_message(&self, agent_id: &str, message: &str) {
        self.send(WorkflowStreamEvent::AgentMessage {
            workflow_id: self.workflow_id,
            agent_id: agent_id.to_string(),
            message: message.to_string(),
        });
    }

    fn on_step_complete(&self, index: usize, step: &WorkflowStep, result: &WorkflowStepResult) {
        let event = match result.status {
            WorkflowStepStatus::Completed => WorkflowStreamEvent::StepCompleted {
                workflow_id: self.workflow_id,
                ordinal: index,
                role: step.role,
            },
            WorkflowStepStatus::Failed => WorkflowStreamEvent::StepFailed {
                workflow_id: self.workflow_id,
                ordinal: index,
                role: step.role,
                error: result
                    .error
                    .clone()
                    .unwrap_or_else(|| "step failed".to_string()),
            },
            WorkflowStepStatus::Cancelled => WorkflowStreamEvent::StepCancelled {
                workflow_id: self.workflow_id,
                ordinal: index,
                role: step.role,
            },
            WorkflowStepStatus::Skipped => WorkflowStreamEvent::StepSkipped {
                workflow_id: self.workflow_id,
                ordinal: index,
                role: step.role,
            },
            WorkflowStepStatus::Pending
            | WorkflowStepStatus::Running
            | WorkflowStepStatus::Interrupted => {
                return;
            }
        };
        self.send(event);
    }

    fn on_workflow_result(&self, result: &WorkflowResult) {
        let event = match result.status {
            WorkflowStatus::Completed => WorkflowStreamEvent::WorkflowCompleted {
                workflow_id: self.workflow_id,
                final_review_verdict: result.final_review_verdict,
            },
            WorkflowStatus::Failed => WorkflowStreamEvent::WorkflowFailed {
                workflow_id: self.workflow_id,
                error: result.error.clone(),
            },
            WorkflowStatus::Cancelled => WorkflowStreamEvent::WorkflowCancelled {
                workflow_id: self.workflow_id,
            },
            WorkflowStatus::Interrupted => WorkflowStreamEvent::WorkflowInterrupted {
                workflow_id: self.workflow_id,
                reason: result.error.clone().unwrap_or_default(),
            },
            WorkflowStatus::Pending | WorkflowStatus::Running => return,
        };
        self.send(event);
    }

    // ---------- Phase 16: DAG node events ----------

    fn on_node_ready(&self, node_id: &str, role: WorkflowRole) {
        self.send(WorkflowStreamEvent::NodeReady {
            workflow_id: self.workflow_id,
            node_id: node_id.to_string(),
            role,
        });
    }

    fn on_node_started(&self, node_id: &str, role: WorkflowRole, agent_id: &str) {
        if role == WorkflowRole::Candidate {
            self.send(WorkflowStreamEvent::CandidateStarted {
                workflow_id: self.workflow_id,
                candidate_id: node_id.to_string(),
                agent_id: agent_id.to_string(),
            });
        }
        self.send(WorkflowStreamEvent::NodeStarted {
            workflow_id: self.workflow_id,
            node_id: node_id.to_string(),
            role,
            agent_id: agent_id.to_string(),
        });
    }

    fn on_node_complete(&self, node_id: &str, role: WorkflowRole, result: &WorkflowStepResult) {
        if role == WorkflowRole::Candidate {
            if result.status == WorkflowStepStatus::Completed {
                self.send(WorkflowStreamEvent::CandidateCompleted {
                    workflow_id: self.workflow_id,
                    candidate_id: node_id.to_string(),
                    snapshot_hash: None,
                });
            } else if result.status == WorkflowStepStatus::Failed {
                self.send(WorkflowStreamEvent::CandidateFailed {
                    workflow_id: self.workflow_id,
                    candidate_id: node_id.to_string(),
                    error: result
                        .error
                        .clone()
                        .unwrap_or_else(|| "candidate failed".to_string()),
                });
            }
        } else if role == WorkflowRole::ConsensusGate && node_id.starts_with("consensus_c") {
            let candidate_id = node_id.replace("consensus_c", "candidate_");
            let outcome = result
                .review_result
                .as_ref()
                .map(|r| r.verdict.key().to_string())
                .unwrap_or_else(|| result.status.as_str().to_string());
            self.send(WorkflowStreamEvent::CandidateConsensusReady {
                workflow_id: self.workflow_id,
                candidate_id,
                outcome,
            });
        } else if role == WorkflowRole::SelectionGate {
            if result.status == WorkflowStepStatus::Completed {
                let winner_candidate_id = result
                    .reason
                    .as_deref()
                    .and_then(|r| r.strip_prefix("winner "))
                    .unwrap_or("candidate_1")
                    .to_string();
                let agent_id = result.agent_id.clone().unwrap_or_default();
                self.send(WorkflowStreamEvent::WinnerSelected {
                    workflow_id: self.workflow_id,
                    candidate_id: winner_candidate_id,
                    agent_id,
                });
            } else if result.status == WorkflowStepStatus::Failed {
                self.send(WorkflowStreamEvent::NoAcceptableCandidate {
                    workflow_id: self.workflow_id,
                });
            }
        }

        let event = match result.status {
            WorkflowStepStatus::Completed => WorkflowStreamEvent::NodeCompleted {
                workflow_id: self.workflow_id,
                node_id: node_id.to_string(),
                role,
            },
            WorkflowStepStatus::Failed => WorkflowStreamEvent::NodeFailed {
                workflow_id: self.workflow_id,
                node_id: node_id.to_string(),
                role,
                error: result
                    .error
                    .clone()
                    .unwrap_or_else(|| "node failed".to_string()),
            },
            WorkflowStepStatus::Cancelled => WorkflowStreamEvent::NodeCancelled {
                workflow_id: self.workflow_id,
                node_id: node_id.to_string(),
                role,
            },
            WorkflowStepStatus::Skipped => WorkflowStreamEvent::NodeSkipped {
                workflow_id: self.workflow_id,
                node_id: node_id.to_string(),
                role,
            },
            WorkflowStepStatus::Interrupted => WorkflowStreamEvent::NodeInterrupted {
                workflow_id: self.workflow_id,
                node_id: node_id.to_string(),
                role,
            },
            WorkflowStepStatus::Pending | WorkflowStepStatus::Running => return,
        };
        self.send(event);
    }
}
