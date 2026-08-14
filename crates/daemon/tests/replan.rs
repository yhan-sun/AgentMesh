//! Phase 19 daemon replan integration tests: user-triggered DAG delta over a
//! running workflow — proposal only, preview without mutation, atomic apply,
//! scheduler hot reload, persistence and crash resume.
//!
//! A fresh `WorkflowService` over the same database simulates a daemon
//! restart; resume must rebuild the *revised* graph from the persisted rows.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{AgentDescriptor, AgentEvent, AgentSkill, Artifact, ArtifactKind};
use agentmesh_daemon::a2a_backend::DaemonA2ABackend;
use agentmesh_daemon::lease::SessionLeaseManager;
use agentmesh_daemon::registry::LiveTaskRegistry;
use agentmesh_daemon::server::DaemonState;
use agentmesh_daemon::workflow_service::WorkflowService;
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::RuleRouter;
use agentmesh_orchestrator::{
    WorkflowGraph, WorkflowNode, WorkflowOptions, WorkflowRole, WorkflowStatus, WorkflowStepStatus,
};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowReplanRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository, replan_status,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Adapter that replays a FIFO script per started task. An empty script keeps
/// the task live; `complete_all` makes every live task finish with `Completed`.
#[derive(Clone)]
struct ScriptedAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<AgentEvent>>>>,
    cancels: Arc<Mutex<HashMap<Uuid, Arc<AtomicBool>>>>,
    live: Arc<Mutex<HashMap<Uuid, Arc<AtomicBool>>>>,
    complete: Arc<AtomicBool>,
    step: std::time::Duration,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            cancels: Arc::new(Mutex::new(HashMap::new())),
            live: Arc::new(Mutex::new(HashMap::new())),
            complete: Arc::new(AtomicBool::new(false)),
            step: std::time::Duration::from_millis(5),
        }
    }

    fn push(&self, script: Vec<AgentEvent>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    /// Ask every currently-live task (empty-script runs) to finish Completed.
    fn complete_all(&self) {
        self.complete
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    async fn spawn_run(&self) -> Result<AgentRunHandle, AgentError> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let run_id = Uuid::new_v4();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancels
            .lock()
            .unwrap()
            .insert(run_id, cancel_flag.clone());
        let complete_flag = Arc::new(AtomicBool::new(false));
        self.live
            .lock()
            .unwrap()
            .insert(run_id, complete_flag.clone());
        let complete = self.complete.clone();
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let step = self.step;
        let cancels = self.cancels.clone();
        let live = self.live.clone();
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let _ = tx.send(AgentEvent::Started).await;
            for event in script {
                tokio::time::sleep(step).await;
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(
                            agentmesh_core::TaskStatus::Cancelled,
                        ))
                        .await;
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
                if tx.send(event.clone()).await.is_err() {
                    return;
                }
                if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
            }
            // Script exhausted without a terminal event: park until cancelled
            // or released to complete (Phase 19 tests control the release).
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(
                            agentmesh_core::TaskStatus::Cancelled,
                        ))
                        .await;
                    cancels.lock().unwrap().remove(&run_id);
                    live.lock().unwrap().remove(&run_id);
                    return;
                }
                if complete.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx.send(AgentEvent::Completed).await;
                    cancels.lock().unwrap().remove(&run_id);
                    live.lock().unwrap().remove(&run_id);
                    return;
                }
            }
        });
        Ok(AgentRunHandle::with_session_channel(run_id, rx, session_rx))
    }
}

#[async_trait]
impl CodingAgentAdapter for ScriptedAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Scripted"
    }
    fn descriptor(&self) -> AgentDescriptor {
        let skills = if self.id == "claude" {
            vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("review", None),
            ]
        } else {
            vec![
                AgentSkill::new("code", None),
                AgentSkill::new("testing", None),
            ]
        };
        AgentDescriptor {
            id: self.id.clone(),
            name: format!("Scripted {}", self.id),
            description: None,
            skills,
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: agentmesh_core::WorkspaceRequirement::None,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, _request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run().await
    }
    async fn resume(
        &self,
        _native_session_id: &str,
        _request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run().await
    }
    async fn cancel(&self, run_id: &str) -> Result<(), AgentError> {
        let run_id = Uuid::parse_str(run_id)
            .map_err(|_| AgentError::InvalidRequest(format!("invalid run id `{run_id}`")))?;
        if let Some(flag) = self.cancels.lock().unwrap().get(&run_id).cloned() {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }
}

fn routing_config() -> agentmesh_core::RoutingConfig {
    agentmesh_core::RoutingConfig {
        architecture: vec!["claude".into()],
        implementation: vec!["codex".into()],
        review: vec!["claude".into()],
        ..agentmesh_core::RoutingConfig::default()
    }
}

struct Env {
    workflows: Arc<WorkflowService>,
    replans: Arc<agentmesh_daemon::replan::ReplanService>,
    steps: WorkflowStepRepository,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    db_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn build_env(
    db_path: &std::path::Path,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    dir: tempfile::TempDir,
) -> Env {
    let db = Database::open(db_path).await.expect("db");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(claude.as_ref().clone()));
    registry.register(Box::new(codex.as_ref().clone()));

    let tasks = TaskRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(db.clone()),
    ));
    let manager = TaskManager::new(
        Arc::new(registry),
        tasks.clone(),
        artifacts,
        contexts,
        sessions,
        workspaces.clone(),
    );

    let token = "replan-test-token".to_string();
    let instance_id = Uuid::new_v4();
    let competitions_repo = agentmesh_storage::CompetitionRepository::new(db.clone());
    let workflows = WorkflowService::new(
        instance_id,
        manager.clone(),
        WorkflowRepository::new(db.clone()),
        WorkflowStepRepository::new(db.clone()),
        WorkflowPlanRepository::new(db.clone()),
        WorkflowReplanRepository::new(db.clone()),
        agentmesh_storage::EvaluationRepository::new(db.clone()),
        competitions_repo.clone(),
        workspaces.clone(),
        RuleRouter::new(routing_config()),
    );
    let replans = agentmesh_daemon::replan::ReplanService::new(
        workflows.clone(),
        WorkflowReplanRepository::new(db.clone()),
    );
    let workflows_repo = WorkflowRepository::new(db.clone());
    let steps = WorkflowStepRepository::new(db.clone());
    let applies = ApplyRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let apply = Arc::new(
        ApplyManager::new(
            tasks.clone(),
            workspaces.clone(),
            workflows_repo.clone(),
            steps.clone(),
            applies.clone(),
        )
        .with_competitions(competitions_repo.clone()),
    );
    let plans = agentmesh_daemon::planner::PlanService::new(
        workflows.clone(),
        WorkflowPlanRepository::new(db.clone()),
    );
    let state = Arc::new(DaemonState {
        instance_id,
        token: token.clone(),
        task_manager: manager.clone(),
        registry: LiveTaskRegistry::new(),
        leases: Arc::new(SessionLeaseManager::new()),
        scope: agentmesh_daemon::Scope::User,
        started_at: chrono::Utc::now(),
        shutdown: Arc::new(Notify::new()),
        shutting_down: AtomicBool::new(false),
        task_repo: tasks,
        workflows: workflows.clone(),
        plans,
        replans: replans.clone(),
        recoveries: agentmesh_daemon::recovery::RecoveryService::new(
            workflows.clone(),
            agentmesh_storage::WorkflowRecoveryRepository::new(db.clone()),
            workspaces.clone(),
        ),
        apply,
        workspaces,
        applies,
        workflows_repo,
        steps: steps.clone(),
        competitions: competitions_repo,
        artifacts,
        a2a_agents: std::sync::Mutex::new(serde_json::json!({})),
        provenance: Arc::new(
            agentmesh_daemon::provenance_service::ProvenanceService::from_db(db.clone()),
        ),
        provenance_repo: agentmesh_storage::ProvenanceRepository::new(db.clone()),
    });

    for adapter in [claude.clone(), codex.clone()] {
        bind_agent_listener(&state, &token, adapter.id()).await;
    }
    let directory = build_directory(&state, &token).await;
    workflows.set_directory(directory);

    Env {
        workflows,
        replans,
        steps,
        claude,
        codex,
        db_path: db_path.to_path_buf(),
        _dir: dir,
    }
}

async fn test_env() -> Env {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    build_env(&dir.path().join("agentmesh.db"), claude, codex, dir).await
}

async fn bind_agent_listener(state: &Arc<DaemonState>, token: &str, agent_id: &str) {
    let backend = Arc::new(DaemonA2ABackend::new(state.clone()));
    let descriptor = state
        .task_manager
        .registry()
        .get(agent_id)
        .expect("agent registered")
        .descriptor();
    let config = Arc::new(agentmesh_a2a::server::A2AServerConfig::new(
        agent_id.to_string(),
        descriptor,
        token.to_string(),
        backend,
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    let mut agents = state.a2a_agents.lock().unwrap();
    agents[agent_id] = serde_json::json!({
        "url": format!("http://{addr}/"),
        "card_url": format!("http://{addr}/.well-known/agent-card.json"),
    });
}

async fn build_directory(state: &Arc<DaemonState>, token: &str) -> AgentDirectory {
    let agents = state.a2a_agents.lock().unwrap().clone();
    let mut discovered = Vec::new();
    for (agent_id, info) in agents.as_object().expect("object") {
        discovered.push(DiscoveredEndpoint {
            agent_id: agent_id.clone(),
            url: info["url"].as_str().unwrap().to_string(),
            card_url: info["card_url"].as_str().unwrap().to_string(),
        });
    }
    let mut directory = AgentDirectory::new();
    directory
        .refresh(
            &discovered,
            &AgentAuth {
                token: Some(token.into()),
            },
        )
        .await
        .expect("refresh directory");
    directory
}

// ---------- scripts ----------

fn json_artifact(name: &str, value: serde_json::Value) -> Artifact {
    let mut artifact = Artifact::text(name, value.to_string());
    artifact.kind = ArtifactKind::Json;
    artifact
}

fn architecture_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("architecture done".into()),
        AgentEvent::Completed,
    ]
}

fn review_script(verdict: &str) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message(format!("review {verdict}")),
        AgentEvent::ArtifactUpdated(json_artifact(
            "review.json",
            serde_json::json!({ "verdict": verdict, "summary": "ok", "issues": [] }),
        )),
        AgentEvent::Completed,
    ]
}

/// A valid replan delta: add a security review after `a`, retarget `c`.
fn replan_delta_script() -> Vec<AgentEvent> {
    let delta = serde_json::json!({
        "version": 1,
        "summary": "add a security review before the implementation",
        "add_nodes": [{
            "id": "security_review",
            "role": "security_review",
            "intent": "review",
            "objective": "Security-review the design",
            "depends_on": ["a"]
        }],
        "update_nodes": [{
            "id": "c",
            "objective": "Review including the security review"
        }],
        "remove_nodes": []
    });
    vec![
        AgentEvent::Message("here is the delta".into()),
        AgentEvent::ArtifactUpdated(json_artifact("replan.json", delta)),
        AgentEvent::Completed,
    ]
}

fn invalid_delta_script(value: serde_json::Value) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("delta".into()),
        AgentEvent::ArtifactUpdated(json_artifact("replan.json", value)),
        AgentEvent::Completed,
    ]
}

/// A 3-node chain graph: a(architect) → b(implementer) → c(reviewer).
fn chain_graph() -> WorkflowGraph {
    WorkflowGraph::new(vec![
        WorkflowNode::new("a", WorkflowRole::Architect),
        WorkflowNode::with_dependencies("b", WorkflowRole::Implementer, vec!["a".to_string()]),
        WorkflowNode::with_dependencies("c", WorkflowRole::Reviewer, vec!["b".to_string()]),
    ])
    .expect("chain")
}

async fn start_chain(env: &Env) -> Uuid {
    env.workflows
        .start_from_graph(
            "Refactor auth",
            chain_graph(),
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
            None,
        )
        .await
        .expect("start")
}

async fn wait_for_status(workflows: &Arc<WorkflowService>, id: Uuid, expected: WorkflowStatus) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(Some(detail)) = workflows.get(id).await
            && detail.status == expected
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "workflow did not reach {expected:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_async<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if f().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "condition not met");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn node_status(
    detail: &agentmesh_daemon::protocol::WorkflowDetail,
    node: &str,
) -> WorkflowStepStatus {
    detail
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some(node))
        .map(|s| s.status)
        .unwrap_or(WorkflowStepStatus::Pending)
}

/// Bring a chain to "a Completed, b Running, c Pending" and return the workflow
/// id (b's live task is held by an empty script).
async fn running_chain(env: &Env) -> Uuid {
    env.claude.push(architecture_script());
    env.codex.push(Vec::new()); // b stays live
    let id = start_chain(env).await;
    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| node_status(&d, "b") == WorkflowStepStatus::Running)
            .unwrap_or(false)
    })
    .await;
    id
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_applies_add_and_update_to_a_running_workflow() {
    let env = test_env().await;
    let id = running_chain(&env).await;
    // The replan planner runs over A2A on claude (Architecture intent).
    env.claude.push(replan_delta_script());
    // After the apply: d = security_review (review → claude), c = review
    // (claude). b finishes when released below.
    env.claude.push(review_script("approved")); // d
    env.claude.push(review_script("approved")); // c

    // Capture b's task id before the replan.
    let b_before = env
        .workflows
        .get(id)
        .await
        .unwrap()
        .unwrap()
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("b"))
        .unwrap()
        .task_id;

    let replan_id = env
        .replans
        .create_proposal(id, "add a security review before implementation", None)
        .await
        .expect("proposal");
    let row = env.replans.get(replan_id).await.unwrap().expect("row");
    if row.status != replan_status::READY {
        eprintln!(
            "DIAG replan row status={} error={:?} task_id={:?}",
            row.status, row.validation_error, row.planner_task_id
        );
    }
    assert_eq!(row.status, replan_status::READY);

    env.replans.apply(replan_id).await.expect("apply");

    // Graph revision bumped; the revised graph is persisted.
    let after_detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(after_detail.graph_revision, 2);
    assert!(
        after_detail
            .steps
            .iter()
            .any(|s| s.node_id.as_deref() == Some("security_review"))
    );
    // c's objective was updated on its persisted row.
    let c_row = env.steps.list_for(id).await.unwrap();
    let c = c_row
        .iter()
        .find(|s| s.node_id.as_deref() == Some("c"))
        .expect("c");
    assert_eq!(
        c.objective.as_deref(),
        Some("Review including the security review")
    );
    // a (Completed) is unchanged, b (Running) kept its task.
    let a_row = c_row
        .iter()
        .find(|s| s.node_id.as_deref() == Some("a"))
        .unwrap();
    assert_eq!(a_row.status, WorkflowStepStatus::Completed.as_str());
    let b_after = after_detail
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("b"))
        .unwrap();
    assert_eq!(
        b_after.task_id, b_before,
        "the Running node's task is never restarted"
    );

    // Release b; the workflow finishes with the new node executed once.
    env.codex.complete_all();
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;
    let done = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    assert_eq!(node_status(&done, "a"), WorkflowStepStatus::Completed);
    assert_eq!(node_status(&done, "b"), WorkflowStepStatus::Completed);
    assert_eq!(node_status(&done, "c"), WorkflowStepStatus::Completed);
    assert_eq!(
        node_status(&done, "security_review"),
        WorkflowStepStatus::Completed
    );
    // a ran exactly one task.
    let a_tasks: Vec<_> = done
        .steps
        .iter()
        .filter(|s| s.node_id.as_deref() == Some("a"))
        .filter_map(|s| s.task_id)
        .collect();
    assert_eq!(a_tasks.len(), 1, "completed node was not rerun");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_check_previews_without_mutating() {
    let env = test_env().await;
    let id = running_chain(&env).await;
    env.claude.push(replan_delta_script());

    let replan_id = env
        .replans
        .create_proposal(id, "add security review", None)
        .await
        .expect("proposal");

    let preview = env
        .replans
        .preview_detail(replan_id)
        .await
        .expect("preview");
    assert_eq!(preview.add_nodes, vec!["security_review".to_string()]);
    assert_eq!(preview.update_nodes, vec!["c".to_string()]);
    assert!(preview.remove_nodes.is_empty());
    assert_eq!(preview.base_graph_revision, 1);
    assert_eq!(preview.current_graph_revision, 1);

    // Nothing mutated: graph_revision stays 1, no new node, proposal still ready.
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 1);
    assert!(
        !workflow
            .steps
            .iter()
            .any(|s| s.node_id.as_deref() == Some("security_review"))
    );
    let row = env.replans.get(replan_id).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::READY);
    assert!(row.applied_graph_revision.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_invalid_delta_marks_proposal_invalid_and_keeps_workflow_unchanged() {
    let env = test_env().await;
    // The delta tries to update the Completed node `a` → ImmutableWorkflowNode.
    let bad = serde_json::json!({
        "version": 1,
        "summary": "tamper",
        "update_nodes": [{ "id": "a", "objective": "tamper" }]
    });
    let id = running_chain(&env).await;
    env.claude.push(invalid_delta_script(bad));

    let replan_id = env
        .replans
        .create_proposal(id, "tamper", None)
        .await
        .expect("proposal");
    let row = env.replans.get(replan_id).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::INVALID);
    assert!(row.validation_error.is_some());

    // The workflow is completely untouched.
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 1);
    assert_eq!(node_status(&workflow, "b"), WorkflowStepStatus::Running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_cycle_delta_is_rejected() {
    let env = test_env().await;
    // Add d depending on c, and update b to depend on d → cycle. b is Running
    // so it is immutable too; the candidate fails regardless.
    let bad = serde_json::json!({
        "version": 1,
        "summary": "cycle",
        "add_nodes": [{
            "id": "d", "role": "implementer", "intent": "implementation",
            "objective": "d", "depends_on": ["c"]
        }],
        "update_nodes": [{ "id": "b", "depends_on": ["d"] }]
    });
    let id = running_chain(&env).await;
    env.claude.push(invalid_delta_script(bad));

    let replan_id = env
        .replans
        .create_proposal(id, "cycle", None)
        .await
        .expect("proposal");
    let row = env.replans.get(replan_id).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::INVALID);
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_policy_violation_is_rejected() {
    let env = test_env().await;
    // A delta that pushes the candidate past the default max_nodes (12) is
    // rejected at proposal time.
    let mut add = Vec::new();
    for i in 0..12 {
        add.push(serde_json::json!({
            "id": format!("n{i}"),
            "role": "implementer",
            "intent": "implementation",
            "objective": format!("o{i}"),
            "depends_on": ["a"]
        }));
    }
    let bad = serde_json::json!({
        "version": 1,
        "summary": "too big",
        "add_nodes": add,
        "update_nodes": [],
        "remove_nodes": []
    });
    let id = running_chain(&env).await;
    env.claude.push(invalid_delta_script(bad));

    let replan_id = env
        .replans
        .create_proposal(id, "too big", None)
        .await
        .expect("proposal");
    let row = env.replans.get(replan_id).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::INVALID);
    assert!(
        row.validation_error
            .as_deref()
            .unwrap_or("")
            .contains("max_nodes")
    );
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_remove_pending_node() {
    let env = test_env().await;
    let delta = serde_json::json!({
        "version": 1,
        "summary": "drop the review",
        "add_nodes": [],
        "update_nodes": [],
        "remove_nodes": ["c"]
    });
    let id = running_chain(&env).await;
    env.claude.push(invalid_delta_script(delta));

    let replan_id = env
        .replans
        .create_proposal(id, "drop c", None)
        .await
        .expect("proposal");
    env.replans.apply(replan_id).await.expect("apply");

    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 2);
    assert!(
        !workflow
            .steps
            .iter()
            .any(|s| s.node_id.as_deref() == Some("c"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_stale_proposal_is_rejected() {
    let env = test_env().await;
    // Two proposals against base revision 1. The first applies (→ revision 2);
    // the second, still based on revision 1, is stale and refused.
    let id = running_chain(&env).await;
    env.claude.push(replan_delta_script());
    env.claude.push(replan_delta_script());

    let first = env
        .replans
        .create_proposal(id, "add security review", None)
        .await
        .expect("first");
    let second = env
        .replans
        .create_proposal(id, "add security review again", None)
        .await
        .expect("second");

    env.replans.apply(first).await.expect("first applies");

    let err = env.replans.apply(second).await.expect_err("stale");
    assert!(matches!(
        err,
        agentmesh_daemon::replan::ReplanError::ReplanStale { .. }
    ));
    // The stale proposal is rejected, not silently applied.
    let row = env.replans.get(second).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::REJECTED);
    assert_eq!(row.applied_graph_revision, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replan_concurrent_applies_only_one_wins() {
    let env = test_env().await;
    let id = running_chain(&env).await;
    env.claude.push(replan_delta_script());

    let replan_id = env
        .replans
        .create_proposal(id, "add security review", None)
        .await
        .expect("proposal");

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let replans = env.replans.clone();
        let rid = replan_id;
        set.spawn(async move { replans.apply(rid).await });
    }
    let mut ok = 0;
    let mut errors = Vec::new();
    while let Some(res) = set.join_next().await {
        match res.expect("join") {
            Ok(_) => ok += 1,
            Err(err) => errors.push(err),
        }
    }
    assert_eq!(ok, 1, "exactly one concurrent apply wins the claim");
    assert_eq!(errors.len(), 3);
    for err in &errors {
        let not_ready_rejected = matches!(
            err,
            agentmesh_daemon::replan::ReplanError::NotReady(_, s)
                if s.as_str() == replan_status::REJECTED
        );
        assert!(
            matches!(
                err,
                agentmesh_daemon::replan::ReplanError::AlreadyApplied(_)
                    | agentmesh_daemon::replan::ReplanError::ApplyInProgress(_)
                    | agentmesh_daemon::replan::ReplanError::ReplanStale { .. }
            ) || not_ready_rejected,
            "losers observe already-applied / in-progress / stale, got {err:?}"
        );
    }
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 2);
    let row = env.replans.get(replan_id).await.unwrap().unwrap();
    assert_eq!(row.status, replan_status::APPLIED);
    assert_eq!(row.applied_graph_revision, Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replan_survives_a_daemon_crash_and_resumes_the_revised_graph() {
    let env = test_env().await;
    let id = running_chain(&env).await;
    // Replan planner (claude), then the post-apply scripts: d (security_review)
    // runs right after apply; c runs after b completes on resume.
    env.claude.push(replan_delta_script());
    env.codex.push(Vec::new()); // resume: b interrupted → new task (live)
    let replan_id = env
        .replans
        .create_proposal(id, "add security review", None)
        .await
        .expect("proposal");
    env.claude.push(review_script("approved")); // d (dispatched after apply)
    env.claude.push(review_script("approved")); // c (after b completes on resume)
    env.replans.apply(replan_id).await.expect("apply");
    // Let the new node run to completion before the crash, so resume must not
    // rerun it.
    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| node_status(&d, "security_review") == WorkflowStepStatus::Completed)
            .unwrap_or(false)
    })
    .await;

    // The revised graph is persisted; b is still Running (live).
    let workflow = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(workflow.graph_revision, 2);
    assert_eq!(node_status(&workflow, "b"), WorkflowStepStatus::Running);
    assert!(
        workflow
            .steps
            .iter()
            .any(|s| s.node_id.as_deref() == Some("security_review"))
    );

    // Crash: a brand-new service over the same database. The old service must
    // be fully stopped first (scheduler drained, interrupted state persisted):
    // a fresh scheduler over the same DB would otherwise race the old one.
    env.workflows.shutdown_interrupt().await;
    let env2 = build_env(
        &env.db_path,
        env.claude.clone(),
        env.codex.clone(),
        tempfile::tempdir().expect("tempdir"),
    )
    .await;
    let new_workflows = env2.workflows.clone();
    new_workflows.recover_interrupted().await.expect("recover");

    let interrupted = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(interrupted.status, WorkflowStatus::Interrupted);
    assert_eq!(
        interrupted.graph_revision, 2,
        "revised graph revision survives"
    );
    // The revised graph survived: security_review is present; a is Completed;
    // b was interrupted; c pending.
    assert!(
        interrupted
            .steps
            .iter()
            .any(|s| s.node_id.as_deref() == Some("security_review"))
    );
    assert_eq!(
        node_status(&interrupted, "a"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(
        node_status(&interrupted, "b"),
        WorkflowStepStatus::Interrupted
    );

    new_workflows.resume(id).await.expect("resume");
    // Release the resumed b task so c can run and the workflow completes.
    env.codex.complete_all();
    wait_for_status(&new_workflows, id, WorkflowStatus::Completed).await;

    let done = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    assert_eq!(done.graph_revision, 2);
    for node in ["a", "b", "c", "security_review"] {
        assert_eq!(
            node_status(&done, node),
            WorkflowStepStatus::Completed,
            "{node}"
        );
    }
    // a ran exactly once (never rerun after the crash).
    let a_tasks: Vec<_> = done
        .steps
        .iter()
        .filter(|s| s.node_id.as_deref() == Some("a"))
        .filter_map(|s| s.task_id)
        .collect();
    assert_eq!(
        a_tasks.len(),
        1,
        "completed node was not rerun after the replan crash"
    );
}
