//! Phase 20 daemon recovery integration tests: a failed workflow stays Failed,
//! the Failure Analyzer (an ordinary A2A agent) proposes a recovery child,
//! explicit execute atomically creates it reusing the parent's context, and
//! bounded attempts + crash recovery hold.

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
use agentmesh_daemon::recovery::RecoveryError;
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
    TaskRepository, WorkflowPlanRepository, WorkflowRecoveryRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository, recovery_status,
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
    complete: Arc<AtomicBool>,
    step: std::time::Duration,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            cancels: Arc::new(Mutex::new(HashMap::new())),
            complete: Arc::new(AtomicBool::new(false)),
            step: std::time::Duration::from_millis(5),
        }
    }

    fn push(&self, script: Vec<AgentEvent>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    /// Ask every currently-live task to finish Completed.
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
        let complete = self.complete.clone();
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let step = self.step;
        let cancels = self.cancels.clone();
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
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(
                            agentmesh_core::TaskStatus::Cancelled,
                        ))
                        .await;
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
                if complete.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx.send(AgentEvent::Completed).await;
                    cancels.lock().unwrap().remove(&run_id);
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
                AgentSkill::new("debug", None),
            ]
        } else {
            vec![
                AgentSkill::new("code", None),
                AgentSkill::new("testing", None),
                AgentSkill::new("debug", None),
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
            tracing::debug!(agent = %self.id, %run_id, "recovery scripted cancel: flag set");
        } else {
            tracing::debug!(agent = %self.id, %run_id, "recovery scripted cancel: run not found");
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
    recoveries: Arc<agentmesh_daemon::recovery::RecoveryService>,
    task_repo: TaskRepository,
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
    plan_policy: agentmesh_orchestrator::PlanPolicy,
    recovery_policy: agentmesh_daemon::recovery::RecoveryPolicy,
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

    let token = "recovery-test-token".to_string();
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
    let recoveries = agentmesh_daemon::recovery::RecoveryService::with_policy(
        workflows.clone(),
        WorkflowRecoveryRepository::new(db.clone()),
        workspaces.clone(),
        plan_policy,
        recovery_policy,
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
    let replans = agentmesh_daemon::replan::ReplanService::new(
        workflows.clone(),
        WorkflowReplanRepository::new(db.clone()),
    );
    let state = Arc::new(DaemonState {
        instance_id,
        token: token.clone(),
        task_manager: manager,
        registry: LiveTaskRegistry::new(),
        leases: Arc::new(SessionLeaseManager::new()),
        scope: agentmesh_daemon::Scope::User,
        started_at: chrono::Utc::now(),
        shutdown: Arc::new(Notify::new()),
        shutting_down: AtomicBool::new(false),
        task_repo: tasks.clone(),
        workflows: workflows.clone(),
        plans,
        replans,
        recoveries: recoveries.clone(),
        apply,
        workspaces,
        applies,
        workflows_repo,
        steps,
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
        recoveries,
        task_repo: tasks,
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
    test_env_with_policies(
        agentmesh_orchestrator::PlanPolicy::default(),
        agentmesh_daemon::recovery::RecoveryPolicy::default(),
    )
    .await
}

async fn test_env_with_policies(
    plan_policy: agentmesh_orchestrator::PlanPolicy,
    recovery_policy: agentmesh_daemon::recovery::RecoveryPolicy,
) -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    build_env(
        &dir.path().join("agentmesh.db"),
        claude,
        codex,
        dir,
        plan_policy,
        recovery_policy,
    )
    .await
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

fn implement_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("implemented".into()),
        AgentEvent::Completed,
    ]
}

/// The Failure Analyzer's output: a 2-node recovery plan (diagnose → fix).
fn analyzer_plan_script() -> Vec<AgentEvent> {
    let plan = serde_json::json!({
        "version": 1,
        "summary": "diagnose and fix",
        "nodes": [
            {"id": "diagnose", "role": "architect", "intent": "architecture", "objective": "Diagnose why the implementation failed", "depends_on": []},
            {"id": "fix", "role": "implementer", "intent": "implementation", "objective": "Fix the implementation", "depends_on": ["diagnose"]}
        ]
    });
    vec![
        AgentEvent::Message("diagnosis".into()),
        AgentEvent::ArtifactUpdated(json_artifact("plan.json", plan)),
        AgentEvent::Completed,
    ]
}

/// A chain graph: a(architect) → b(implementer) → c(reviewer).
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

/// A chain that fails at node `b` (codex): a completes, b fails, c never runs.
/// The workflow ends Failed.
async fn failed_chain(env: &Env) -> Uuid {
    env.claude.push(architecture_script());
    env.codex.push(vec![AgentEvent::Failed(
        "boom: the implementation broke".into(),
    )]);
    let id = start_chain(env).await;
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;
    id
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failure_generates_proposal_and_execute_creates_child() {
    let env = test_env().await;
    let a = failed_chain(&env).await;

    // The Failure Analyzer runs over A2A on codex (TaskIntent::Debug).
    env.codex.push(analyzer_plan_script());
    // Recovery child scripts: diagnose (architect → claude), fix (codex).
    env.claude.push(architecture_script()); // diagnose
    env.codex.push(implement_script()); // fix

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    if row.status != recovery_status::READY {
        eprintln!(
            "DIAG recovery row status={} error={:?} task={:?}",
            row.status, row.validation_error, row.planner_task_id
        );
    }
    assert_eq!(row.status, recovery_status::READY);
    assert_eq!(row.failed_node_id, "b");
    assert_eq!(row.attempt, 1);

    let b = env.recoveries.execute(recovery_id).await.expect("execute");
    wait_for_status(&env.workflows, b, WorkflowStatus::Completed).await;

    // The failed parent stays Failed; history is immutable.
    let parent = env.workflows.get(a).await.unwrap().unwrap();
    assert_eq!(parent.status, WorkflowStatus::Failed);
    assert_eq!(node_status(&parent, "b"), WorkflowStepStatus::Failed);

    // The child carries the parent lineage and reuses the same context.
    let child = env.workflows.get(b).await.unwrap().unwrap();
    assert_eq!(child.status, WorkflowStatus::Completed);
    assert_eq!(child.parent_workflow_id, Some(a));
    assert_eq!(child.recovery_of_node_id.as_deref(), Some("b"));
    assert_eq!(child.recovery_attempt, 1);
    assert_eq!(child.context_id, parent.context_id, "same context reused");
    assert_eq!(
        node_status(&child, "diagnose"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(node_status(&child, "fix"), WorkflowStepStatus::Completed);

    // The proposal is executed and bound to the child.
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::EXECUTED);
    assert_eq!(row.recovery_workflow_id, Some(b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_agent_reuses_session_and_worktree() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    env.claude.push(architecture_script()); // diagnose
    env.codex.push(implement_script()); // fix

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let b = env.recoveries.execute(recovery_id).await.expect("execute");
    wait_for_status(&env.workflows, b, WorkflowStatus::Completed).await;

    // The failed parent's node b (codex) task's session.
    let parent = env.workflows.get(a).await.unwrap().unwrap();
    let b_task_id = parent
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("b"))
        .unwrap()
        .task_id
        .expect("b task");
    let b_session = env
        .task_repo
        .get(b_task_id)
        .await
        .unwrap()
        .unwrap()
        .agent_session_id;

    // The child's fix node (also codex, same intent) reuses that session —
    // the same native session + worktree (Phase 20 §10).
    let child = env.workflows.get(b).await.unwrap().unwrap();
    let fix_task_id = child
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("fix"))
        .unwrap()
        .task_id
        .expect("fix task");
    let fix_session = env
        .task_repo
        .get(fix_task_id)
        .await
        .unwrap()
        .unwrap()
        .agent_session_id;
    assert_eq!(
        fix_session, b_session,
        "same agent reuses the same session/worktree"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_check_previews_without_executing() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");

    let preview = env
        .recoveries
        .preview_detail(recovery_id)
        .await
        .expect("preview");
    assert_eq!(preview.failed_node_id, "b");
    assert_eq!(preview.attempt, 1);
    assert_eq!(preview.node_count, 2);

    // Nothing executed: proposal still ready, no child created.
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::READY);
    assert!(row.recovery_workflow_id.is_none());
    assert!(env.workflows.child_workflows(a).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_and_interrupted_do_not_generate_proposals() {
    let env = test_env().await;
    // A cancelled workflow must not produce a recovery.
    env.claude.push(architecture_script());
    env.codex.push(Vec::new()); // b live
    let id = start_chain(&env).await;
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| node_status(&d, "b") == WorkflowStepStatus::Running)
            .unwrap_or(false)
    })
    .await;
    env.workflows.cancel(id).await.expect("cancel");
    wait_for_status(&env.workflows, id, WorkflowStatus::Cancelled).await;

    let err = env
        .recoveries
        .propose(id, None)
        .await
        .expect_err("not failed");
    assert!(matches!(err, RecoveryError::WorkflowNotFailed(..)));

    // An interrupted (daemon-shutdown) workflow must not produce a recovery.
    let env2 = test_env().await;
    env2.claude.push(architecture_script());
    env2.codex.push(Vec::new());
    let id2 = start_chain(&env2).await;
    wait_for_async(|| async {
        env2.workflows
            .get(id2)
            .await
            .ok()
            .flatten()
            .map(|d| node_status(&d, "b") == WorkflowStepStatus::Running)
            .unwrap_or(false)
    })
    .await;
    env2.workflows.shutdown_interrupt().await;
    wait_for_status(&env2.workflows, id2, WorkflowStatus::Interrupted).await;

    let err = env2
        .recoveries
        .propose(id2, None)
        .await
        .expect_err("not failed");
    assert!(matches!(err, RecoveryError::WorkflowNotFailed(..)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_invalid_plan_marks_proposal_invalid() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    // The analyzer returns a plan with a control field → parse rejects it.
    let bad = serde_json::json!({
        "version": 1, "summary": "s",
        "nodes": [{"id": "x", "role": "implementer", "intent": "implementation", "objective": "x", "depends_on": [], "agent_id": "claude"}]
    });
    env.codex.push(vec![
        AgentEvent::Message("plan".into()),
        AgentEvent::ArtifactUpdated(json_artifact("plan.json", bad)),
        AgentEvent::Completed,
    ]);

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::INVALID);
    assert!(row.validation_error.is_some());
    // The failed workflow is untouched.
    let parent = env.workflows.get(a).await.unwrap().unwrap();
    assert_eq!(parent.status, WorkflowStatus::Failed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_limit_reached_blocks_second_proposal() {
    // max_attempts = 1: after the first recovery child fails, no second
    // proposal is generated.
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    // Recovery child scripts: diagnose (claude) succeeds; fix (codex) FAILS.
    env.claude.push(architecture_script()); // diagnose
    env.codex
        .push(vec![AgentEvent::Failed("fix failed too".into())]); // fix

    let first = env
        .recoveries
        .propose(a, None)
        .await
        .expect("first proposal");
    let b = env.recoveries.execute(first).await.expect("execute");
    wait_for_status(&env.workflows, b, WorkflowStatus::Failed).await;

    // Second proposal: attempt 2 > max_attempts 1 → limit reached, and the
    // analyzer is never invoked.
    let err = env.recoveries.propose(a, None).await.expect_err("limit");
    assert!(matches!(err, RecoveryError::RecoveryLimitReached { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn atomic_recovery_execute_only_one_creates_child() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    env.claude.push(architecture_script()); // diagnose
    env.codex.push(implement_script()); // fix

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let recoveries = env.recoveries.clone();
        let rid = recovery_id;
        set.spawn(async move { recoveries.execute(rid).await });
    }
    let mut ok = 0;
    let mut errors = Vec::new();
    while let Some(res) = set.join_next().await {
        match res.expect("join") {
            Ok(_) => ok += 1,
            Err(err) => errors.push(err),
        }
    }
    assert_eq!(ok, 1, "exactly one concurrent execute creates the child");
    assert_eq!(errors.len(), 3);
    for err in &errors {
        assert!(
            matches!(
                err,
                RecoveryError::AlreadyExecuted(_) | RecoveryError::ExecutionInProgress(_)
            ),
            "losers see already-executed or in-progress, got {err:?}"
        );
    }
    // Exactly one child workflow.
    assert_eq!(env.workflows.child_workflows(a).await.unwrap().len(), 1);
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::EXECUTED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_executing_recovery_is_retryable_or_corrected() {
    let env = test_env().await;
    let repo = env_repo(&env).await;
    let now = chrono::Utc::now().to_rfc3339();
    let now2 = chrono::Utc::now().to_rfc3339();
    let a = Uuid::new_v4();
    let child = Uuid::new_v4();

    // A claim that never created a child → retryable `ready`.
    repo.create(&agentmesh_storage::WorkflowRecoveryRow {
        id: Uuid::new_v4(),
        workflow_id: a,
        failed_node_id: "b".to_string(),
        status: recovery_status::EXECUTING.to_string(),
        planner_agent_id: None,
        planner_task_id: None,
        plan_json: None,
        validation_error: None,
        recovery_workflow_id: None,
        attempt: 1,
        created_at: now.clone(),
        executed_at: None,
    })
    .await
    .expect("insert executing");
    // A proposal stuck in `generating` (the analyzer died mid-run).
    repo.create(&agentmesh_storage::WorkflowRecoveryRow {
        id: Uuid::new_v4(),
        workflow_id: a,
        failed_node_id: "b".to_string(),
        status: recovery_status::GENERATING.to_string(),
        planner_agent_id: None,
        planner_task_id: None,
        plan_json: None,
        validation_error: None,
        recovery_workflow_id: None,
        attempt: 1,
        created_at: now.clone(),
        executed_at: None,
    })
    .await
    .expect("insert generating");
    // A claim that created the child but crashed before marking executed.
    repo.create(&agentmesh_storage::WorkflowRecoveryRow {
        id: Uuid::new_v4(),
        workflow_id: a,
        failed_node_id: "b".to_string(),
        status: recovery_status::EXECUTING.to_string(),
        planner_agent_id: None,
        planner_task_id: None,
        plan_json: None,
        validation_error: None,
        recovery_workflow_id: Some(child),
        attempt: 1,
        created_at: now2,
        executed_at: None,
    })
    .await
    .expect("insert executing with child");

    let (generating_failed, retryable, corrected) = env
        .recoveries
        .recover_stale_executing()
        .await
        .expect("recover");
    assert_eq!(
        generating_failed, 1,
        "analyzer died → failed, never repeated"
    );
    assert_eq!(retryable, 1, "no child → retryable ready");
    assert_eq!(corrected, 1, "child exists → executed, never guessed");
    let all = repo.list_for(a).await.expect("list");
    assert!(all.iter().any(|r| r.status == recovery_status::READY));
    assert!(
        all.iter()
            .any(|r| r.status == recovery_status::FAILED && r.validation_error.is_some())
    );
    assert!(all
        .iter()
        .any(|r| r.status == recovery_status::EXECUTED && r.recovery_workflow_id == Some(child)));
}

async fn env_repo(env: &Env) -> agentmesh_storage::WorkflowRecoveryRepository {
    let db = agentmesh_storage::Database::open(&env.db_path)
        .await
        .expect("db");
    agentmesh_storage::WorkflowRecoveryRepository::new(db)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lineage_shows_parent_and_recovery_child() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    env.claude.push(architecture_script()); // diagnose
    env.codex.push(implement_script()); // fix

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let b = env.recoveries.execute(recovery_id).await.expect("execute");
    wait_for_status(&env.workflows, b, WorkflowStatus::Completed).await;

    let lineage = env.workflows.lineage(a).await.unwrap().unwrap();
    assert!(lineage.parent.is_none());
    assert_eq!(lineage.recovery_children.len(), 1);
    assert_eq!(lineage.recovery_children[0].workflow_id, b);
    assert_eq!(
        lineage.recovery_children[0].recovery_of_node_id.as_deref(),
        Some("b")
    );
    assert_eq!(
        lineage.recovery_children[0].status,
        WorkflowStatus::Completed
    );

    // From the child's side, the parent is shown.
    let child_lineage = env.workflows.lineage(b).await.unwrap().unwrap();
    let parent_node = child_lineage.parent.expect("parent");
    assert_eq!(parent_node.workflow_id, a);
    assert_eq!(parent_node.status, WorkflowStatus::Failed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_workflow_crash_resumes_without_rerunning_completed_nodes() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    env.claude.push(architecture_script()); // diagnose (first run, completes)
    env.codex.push(Vec::new()); // fix (first run, live = the crash point)
    env.claude.push(architecture_script()); // diagnose (resume: not rerun)
    env.codex.push(Vec::new()); // fix (resume: new task, live)

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let b = env.recoveries.execute(recovery_id).await.expect("execute");
    wait_for_async(|| async {
        env.workflows
            .get(b)
            .await
            .ok()
            .flatten()
            .map(|d| node_status(&d, "fix") == WorkflowStepStatus::Running)
            .unwrap_or(false)
    })
    .await;

    // Crash → a fresh service over the same database. The old service is
    // fully stopped first (scheduler drained, interrupted state persisted):
    // a fresh scheduler over the same DB would otherwise race the old one.
    env.workflows.shutdown_interrupt().await;
    let env2 = build_env(
        &env.db_path,
        env.claude.clone(),
        env.codex.clone(),
        tempfile::tempdir().expect("tempdir"),
        agentmesh_orchestrator::PlanPolicy::default(),
        agentmesh_daemon::recovery::RecoveryPolicy::default(),
    )
    .await;
    let new_workflows = env2.workflows.clone();
    new_workflows.recover_interrupted().await.expect("recover");

    let interrupted = new_workflows.get(b).await.unwrap().unwrap();
    assert_eq!(interrupted.status, WorkflowStatus::Interrupted);
    assert_eq!(
        node_status(&interrupted, "diagnose"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(
        node_status(&interrupted, "fix"),
        WorkflowStepStatus::Interrupted
    );
    assert_eq!(interrupted.parent_workflow_id, Some(a), "lineage survives");

    new_workflows.resume(b).await.expect("resume");
    env2.codex.complete_all(); // release the resumed fix task
    wait_for_status(&new_workflows, b, WorkflowStatus::Completed).await;

    let done = new_workflows.get(b).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    // diagnose was never rerun.
    let diagnose_tasks: Vec<_> = done
        .steps
        .iter()
        .filter(|s| s.node_id.as_deref() == Some("diagnose"))
        .filter_map(|s| s.task_id)
        .collect();
    assert_eq!(diagnose_tasks.len(), 1, "completed node was not rerun");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_policy_violation_marks_proposal_invalid() {
    let env = test_env_with_policies(
        agentmesh_orchestrator::PlanPolicy {
            max_nodes: 1,
            max_agent_calls: 1,
            ..agentmesh_orchestrator::PlanPolicy::default()
        },
        agentmesh_daemon::recovery::RecoveryPolicy::default(),
    )
    .await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script()); // 2-node plan > max_nodes 1

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::INVALID);
    assert!(
        row.validation_error
            .as_deref()
            .unwrap_or("")
            .contains("max_nodes"),
        "policy rejection recorded: {:?}",
        row.validation_error
    );
    // The failed workflow is untouched.
    assert_eq!(
        env.workflows.get(a).await.unwrap().unwrap().status,
        WorkflowStatus::Failed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_agent_call_budget_is_enforced_at_execute() {
    let env = test_env_with_policies(
        agentmesh_orchestrator::PlanPolicy::default(),
        agentmesh_daemon::recovery::RecoveryPolicy {
            max_recovery_agent_calls: 1,
            ..agentmesh_daemon::recovery::RecoveryPolicy::default()
        },
    )
    .await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script()); // 2-node plan

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        recovery_status::READY,
        "budget gates execute, not propose"
    );

    let err = env
        .recoveries
        .execute(recovery_id)
        .await
        .expect_err("budget");
    assert!(matches!(err, RecoveryError::RecoveryBudgetExceeded { .. }));
    // No child workflow was created.
    assert!(env.workflows.child_workflows(a).await.unwrap().is_empty());
    // The proposal is still claimable/ready (execute did not consume it).
    let row = env.recoveries.get(recovery_id).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::READY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_recovery_agent_stays_in_its_own_workspace() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script()); // diagnose(claude) → fix(codex)
    env.claude.push(architecture_script()); // diagnose
    env.codex.push(implement_script()); // fix

    let recovery_id = env.recoveries.propose(a, None).await.expect("proposal");
    let b = env.recoveries.execute(recovery_id).await.expect("execute");
    wait_for_status(&env.workflows, b, WorkflowStatus::Completed).await;

    // The failed node b is codex; the recovery `diagnose` node is claude — a
    // different agent — and must NOT reuse the failed agent's session.
    let parent = env.workflows.get(a).await.unwrap().unwrap();
    let a_task = parent
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("a"))
        .unwrap()
        .task_id
        .expect("a task");
    let b_task = parent
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("b"))
        .unwrap()
        .task_id
        .expect("b task");
    let a_session = env
        .task_repo
        .get(a_task)
        .await
        .unwrap()
        .unwrap()
        .agent_session_id;
    let b_session = env
        .task_repo
        .get(b_task)
        .await
        .unwrap()
        .unwrap()
        .agent_session_id;

    let child = env.workflows.get(b).await.unwrap().unwrap();
    let diagnose_task = child
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("diagnose"))
        .unwrap()
        .task_id
        .expect("diagnose task");
    let diagnose_session = env
        .task_repo
        .get(diagnose_task)
        .await
        .unwrap()
        .unwrap()
        .agent_session_id;
    // claude's recovery node reuses claude's own session (a), never codex's (b).
    assert_eq!(
        diagnose_session, a_session,
        "recovery agent reuses its own session"
    );
    assert_ne!(
        diagnose_session, b_session,
        "a different recovery agent never enters the failed agent's workspace"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_recovery_proposal_is_rejected() {
    let env = test_env().await;
    let a = failed_chain(&env).await;
    env.codex.push(analyzer_plan_script());
    env.codex.push(analyzer_plan_script()); // must NOT be consumed

    let first = env
        .recoveries
        .propose(a, None)
        .await
        .expect("first proposal");
    let err = env
        .recoveries
        .propose(a, None)
        .await
        .expect_err("duplicate");
    assert!(matches!(
        err,
        RecoveryError::AlreadyPending { recovery_id, .. } if recovery_id == first
    ));
    // Exactly one proposal exists; no competing proposal was generated.
    assert_eq!(env.recoveries.list_for(a).await.unwrap().len(), 1);
    let row = env.recoveries.get(first).await.unwrap().unwrap();
    assert_eq!(row.status, recovery_status::READY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_generate_and_auto_execute_recovery() {
    let env = test_env().await;
    // Wire the failure-sink consumer exactly like the daemon's build_state
    // (Phase 21 §1 P0): a workflow reaching Failed auto-generates a proposal,
    // and auto_execute=true creates the child workflow.
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    env.workflows.set_failure_sink(tx).await;
    let recoveries = env.recoveries.clone();
    tokio::spawn(async move {
        while let Some(workflow_id) = rx.recv().await {
            if let Ok(recovery_id) = recoveries.propose(workflow_id, None).await {
                let _ = recoveries.execute(recovery_id).await;
            }
        }
    });

    // The failed workflow triggers the sink → proposal → child. Scripts are
    // pushed BEFORE the workflow starts so the analyzer (popped when the sink
    // fires) is already queued.
    env.codex.push(vec![AgentEvent::Failed("boom".into())]); // b fails
    env.codex.push(analyzer_plan_script()); // failure analyzer
    env.codex.push(implement_script()); // child fix
    env.claude.push(architecture_script()); // a
    env.claude.push(architecture_script()); // child diagnose
    let a = start_chain(&env).await;
    wait_for_status(&env.workflows, a, WorkflowStatus::Failed).await;

    wait_for_async(|| async { !env.workflows.child_workflows(a).await.unwrap().is_empty() }).await;

    // The parent stays Failed; exactly one child was auto-created and runs.
    let parent = env.workflows.get(a).await.unwrap().unwrap();
    assert_eq!(parent.status, WorkflowStatus::Failed);
    let children = env.workflows.child_workflows(a).await.unwrap();
    assert_eq!(children.len(), 1);
    let child_id = children[0].id;
    wait_for_status(&env.workflows, child_id, WorkflowStatus::Completed).await;
    // The proposal is executed (auto_execute).
    let rows = env.recoveries.list_for(a).await.unwrap();
    assert!(rows.iter().any(|r| r.status == recovery_status::EXECUTED));
}
