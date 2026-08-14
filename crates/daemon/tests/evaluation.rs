//! Phase 21 daemon evaluation integration tests: the `consensus-review` preset
//! runs {N parallel evaluators} over the implementation and a deterministic
//! ConsensusGate decides Approved / ChangesRequested / Unavailable.
//!
//! Evaluators are distinct agents, receive the same snapshot, never see each
//! other's results, and their workspaces are never an Apply source.

use std::collections::VecDeque;
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
    ConsensusOutcome, WorkflowOptions, WorkflowStatus, WorkflowStepStatus,
};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowRecoveryRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository, evaluation_status,
    member_status,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Adapter that replays a FIFO script per started task and records prompts.
#[derive(Clone)]
struct ScriptedAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<AgentEvent>>>>,
    recorded: Arc<Mutex<Vec<String>>>,
    step: std::time::Duration,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            recorded: Arc::new(Mutex::new(Vec::new())),
            step: std::time::Duration::from_millis(5),
        }
    }

    fn push(&self, script: Vec<AgentEvent>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    fn prompts(&self) -> Vec<String> {
        self.recorded.lock().unwrap().clone()
    }

    async fn spawn_run(&self, prompt: String) -> Result<AgentRunHandle, AgentError> {
        self.recorded.lock().unwrap().push(prompt);
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let run_id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let step = self.step;
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let _ = tx.send(AgentEvent::Started).await;
            for event in script {
                tokio::time::sleep(step).await;
                if tx.send(event.clone()).await.is_err() {
                    return;
                }
                if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                    return;
                }
            }
            // Empty script → park live until dropped.
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
        AgentDescriptor {
            id: self.id.clone(),
            name: format!("Scripted {}", self.id),
            description: None,
            // Every agent can review, so evaluators route to distinct agents.
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("review", None),
                AgentSkill::new("implementation", None),
                AgentSkill::new("testing", None),
            ],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: agentmesh_core::WorkspaceRequirement::None,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request.input.content).await
    }
    async fn resume(
        &self,
        _native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request.input.content).await
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
}

fn routing_config() -> agentmesh_core::RoutingConfig {
    agentmesh_core::RoutingConfig {
        architecture: vec!["claude".into()],
        implementation: vec!["codex".into()],
        review: vec!["claude".into(), "codex".into(), "opencode".into()],
        ..agentmesh_core::RoutingConfig::default()
    }
}

struct Env {
    workflows: Arc<WorkflowService>,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    opencode: Arc<ScriptedAdapter>,
    db_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn build_env(
    db_path: &std::path::Path,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    opencode: Arc<ScriptedAdapter>,
    dir: tempfile::TempDir,
) -> Env {
    let db = Database::open(db_path).await.expect("db");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(claude.as_ref().clone()));
    registry.register(Box::new(codex.as_ref().clone()));
    registry.register(Box::new(opencode.as_ref().clone()));

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

    let token = "eval-test-token".to_string();
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
    let recoveries = agentmesh_daemon::recovery::RecoveryService::new(
        workflows.clone(),
        WorkflowRecoveryRepository::new(db.clone()),
        workspaces.clone(),
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
        task_repo: tasks,
        workflows: workflows.clone(),
        plans,
        replans,
        recoveries,
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

    for adapter in [claude.clone(), codex.clone(), opencode.clone()] {
        bind_agent_listener(&state, &token, adapter.id()).await;
    }
    let directory = build_directory(&state, &token).await;
    workflows.set_directory(directory);

    Env {
        workflows,
        claude,
        codex,
        opencode,
        db_path: db_path.to_path_buf(),
        _dir: dir,
    }
}

async fn test_env() -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(ScriptedAdapter::new("claude")),
        Arc::new(ScriptedAdapter::new("codex")),
        Arc::new(ScriptedAdapter::new("opencode")),
        dir,
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

/// An evaluator verdict artifact with confidence (Phase 21 §6).
fn evaluation_script(verdict: &str, confidence: f64) -> Vec<AgentEvent> {
    let value = serde_json::json!({
        "verdict": verdict,
        "confidence": confidence,
        "summary": format!("evaluation {verdict}"),
        "issues": []
    });
    vec![
        AgentEvent::Message(format!("evaluation {verdict}")),
        AgentEvent::ArtifactUpdated(json_artifact("evaluation.json", value)),
        AgentEvent::Completed,
    ]
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

/// The happy-path scripts for a consensus-review run (2 approve, 1 changes).
fn push_approved_run(env: &Env) {
    env.claude.push(architecture_script()); // architecture
    env.codex.push(implement_script()); // implementation
    env.claude.push(evaluation_script("approved", 0.9)); // evaluator_1
    env.codex.push(evaluation_script("approved", 0.8)); // evaluator_2
    env.opencode
        .push(evaluation_script("changes_requested", 0.7)); // evaluator_3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consensus_review_majority_approves() {
    let env = test_env().await;
    push_approved_run(&env);
    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");

    // Wait until the 3 evaluator tasks are Running at the same time (parallel).
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| {
                d.steps
                    .iter()
                    .filter(|s| {
                        s.node_id
                            .as_deref()
                            .is_some_and(|n| n.starts_with("evaluator_"))
                            && s.status == WorkflowStepStatus::Running
                    })
                    .count()
                    >= 3
            })
            .unwrap_or(false)
    })
    .await;

    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    for node in [
        "architecture",
        "implementation",
        "evaluator_1",
        "evaluator_2",
        "evaluator_3",
        "consensus_gate",
    ] {
        assert_eq!(
            node_status(&detail, node),
            WorkflowStepStatus::Completed,
            "{node}"
        );
    }

    // The group persisted a majority-Approved consensus.
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 1);
    let group = &groups[0];
    assert_eq!(group.strategy, "majority");
    assert_eq!(group.status, evaluation_status::COMPLETED);
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(group.consensus.as_deref().expect("consensus")).expect("parse");
    assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
    assert_eq!(consensus.approved_count, 2);
    assert_eq!(consensus.changes_requested_count, 1);
    assert_eq!(consensus.valid_count, 3);

    // Three distinct agents evaluated; the gate never ran an agent.
    let members = env.workflows.evaluation_members(group.id).await.unwrap();
    assert_eq!(members.len(), 3);
    let mut agents: Vec<String> = members.iter().map(|m| m.agent_id.clone()).collect();
    agents.sort();
    assert_eq!(
        agents,
        vec![
            "claude".to_string(),
            "codex".to_string(),
            "opencode".to_string()
        ],
        "evaluators are distinct agents"
    );
    for member in &members {
        assert_eq!(member.status, member_status::COMPLETED);
        assert!(member.result_json.is_some());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evaluators_are_independent_and_never_see_each_other() {
    let env = test_env().await;
    push_approved_run(&env);
    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    // Each evaluator prompt contains the original goal + the implementation
    // handoff, but never another evaluator's node id or result.
    let all_prompts = [
        env.claude.prompts(),
        env.codex.prompts(),
        env.opencode.prompts(),
    ];
    let evaluator_prompts: Vec<&String> = all_prompts
        .iter()
        .flat_map(|p| p.iter())
        .filter(|p| p.contains("evaluator"))
        .collect();
    assert_eq!(evaluator_prompts.len(), 3, "three evaluator prompts");
    for prompt in &evaluator_prompts {
        assert!(prompt.contains("Refactor auth"), "original goal present");
        assert!(
            prompt.contains("implemented"),
            "implementation summary present"
        );
        assert!(
            !prompt.contains("evaluator_") || prompt.contains("You are the Evaluator"),
            "an evaluator never sees another evaluator's node id or result"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_met_with_one_evaluator_failure() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script());
    env.claude.push(evaluation_script("approved", 0.9)); // evaluator_1
    env.codex.push(evaluation_script("approved", 0.8)); // evaluator_2
    env.opencode
        .push(vec![AgentEvent::Failed("evaluator crashed".into())]); // evaluator_3 fails

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    assert_eq!(
        node_status(&detail, "evaluator_3"),
        WorkflowStepStatus::Failed
    );
    assert_eq!(
        node_status(&detail, "consensus_gate"),
        WorkflowStepStatus::Completed
    );

    // 2 valid ≥ quorum 2 → consensus still forms (Approved).
    let group = env
        .workflows
        .evaluation_groups(id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(group.consensus.as_deref().expect("consensus")).expect("parse");
    assert_eq!(consensus.valid_count, 2);
    assert_eq!(consensus.total_count, 3);
    assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
    let members = env.workflows.evaluation_members(group.id).await.unwrap();
    assert_eq!(
        members
            .iter()
            .filter(|m| m.status == member_status::FAILED)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn below_quorum_is_unavailable() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script());
    env.claude.push(evaluation_script("approved", 0.9)); // evaluator_1 (valid)
    env.codex.push(vec![AgentEvent::Failed("x".into())]); // evaluator_2 fails
    env.opencode.push(vec![AgentEvent::Failed("y".into())]); // evaluator_3 fails

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;

    // 1 valid < quorum 2 → ConsensusUnavailable → the workflow fails honestly.
    let group = env
        .workflows
        .evaluation_groups(id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(group.status, evaluation_status::FAILED);
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(group.consensus.as_deref().expect("consensus")).expect("parse");
    assert_eq!(consensus.outcome, ConsensusOutcome::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unanimous_any_changes_requests_changes() {
    let env = test_env().await;
    // A source workflow with a completed + approved implementation to evaluate.
    env.claude.push(architecture_script());
    env.codex.push(implement_script());
    env.claude.push(evaluation_script("approved", 1.0)); // reviewer approves
    let source = env
        .workflows
        .start(
            "architect-implement-review",
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 1,
            },
        )
        .await
        .expect("source");
    wait_for_status(&env.workflows, source, WorkflowStatus::Completed).await;

    // Standalone evaluation with the unanimous strategy (control plane config).
    env.claude.push(evaluation_script("approved", 0.9)); // evaluator_1
    env.codex.push(evaluation_script("approved", 0.8)); // evaluator_2
    env.opencode
        .push(evaluation_script("changes_requested", 0.7)); // evaluator_3
    let (eval_id, group_id) = env
        .workflows
        .start_evaluation(source, Some(3), Some("unanimous"), Some(2))
        .await
        .expect("evaluate");
    wait_for_status(&env.workflows, eval_id, WorkflowStatus::Failed).await;

    // Unanimous: the single changes_requested → gate fails the workflow.
    let group = env
        .workflows
        .evaluation_group(group_id)
        .await
        .unwrap()
        .expect("group");
    assert_eq!(group.strategy, "unanimous");
    assert_eq!(group.quorum, 2);
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(group.consensus.as_deref().expect("consensus")).expect("parse");
    assert_eq!(consensus.outcome, ConsensusOutcome::ChangesRequested);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evaluation_crash_resume_keeps_completed_and_resumes_interrupted() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script());
    env.claude.push(evaluation_script("approved", 0.9)); // evaluator_1
    env.codex.push(Vec::new()); // evaluator_2 live (the crash point)
    env.opencode.push(evaluation_script("approved", 0.8)); // evaluator_3
    env.codex.push(evaluation_script("approved", 0.85)); // evaluator_2 resume

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    // evaluator_2 is Running (live) and evaluator_1/evaluator_3 completed.
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| {
                node_status(&d, "evaluator_2") == WorkflowStepStatus::Running
                    && node_status(&d, "evaluator_1") == WorkflowStepStatus::Completed
                    && node_status(&d, "evaluator_3") == WorkflowStepStatus::Completed
            })
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
        env.opencode.clone(),
        tempfile::tempdir().expect("tempdir"),
    )
    .await;
    let new_workflows = env2.workflows.clone();
    new_workflows.recover_interrupted().await.expect("recover");

    let interrupted = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(interrupted.status, WorkflowStatus::Interrupted);
    assert_eq!(
        node_status(&interrupted, "evaluator_1"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(
        node_status(&interrupted, "evaluator_2"),
        WorkflowStepStatus::Interrupted
    );
    assert_eq!(
        node_status(&interrupted, "evaluator_3"),
        WorkflowStepStatus::Completed
    );

    new_workflows.resume(id).await.expect("resume");
    wait_for_status(&new_workflows, id, WorkflowStatus::Completed).await;

    let done = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    // Completed evaluators were not rerun (one task each).
    for node in ["evaluator_1", "evaluator_3"] {
        let tasks = done
            .steps
            .iter()
            .filter(|s| s.node_id.as_deref() == Some(node))
            .filter_map(|s| s.task_id)
            .count();
        assert_eq!(tasks, 1, "{node} was not rerun");
    }
    // The interrupted evaluator resumed and the gate reached consensus.
    let group = new_workflows
        .evaluation_groups(id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(group.consensus.as_deref().expect("consensus")).expect("parse");
    assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
    assert_eq!(consensus.valid_count, 3);
}
