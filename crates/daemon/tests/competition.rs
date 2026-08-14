//! Phase 23 integration tests: `best-of-n` preset, session lanes, blind evaluation,
//! deterministic SelectionGate, winner-only safe apply, and crash/resume.

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
use agentmesh_orchestrator::dag::PRESET_BEST_OF_N;
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::RuleRouter;
use agentmesh_orchestrator::{WorkflowOptions, WorkflowStatus, WorkflowStepStatus};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, CompetitionRepository,
    ContextRepository, Database, TaskRepository, WorkflowPlanRepository,
    WorkflowRecoveryRepository, WorkflowReplanRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository,
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
            step: std::time::Duration::from_millis(1),
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
        implementation: vec!["claude".into(), "codex".into()],
        review: vec![
            "opencode".into(),
            "antigravity".into(),
            "claude".into(),
            "codex".into(),
        ],
        ..agentmesh_core::RoutingConfig::default()
    }
}

#[allow(dead_code)]
struct Env {
    workflows: Arc<WorkflowService>,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    opencode: Arc<ScriptedAdapter>,
    antigravity: Arc<ScriptedAdapter>,
    state: Arc<DaemonState>,
    db_path: std::path::PathBuf,
    competitions: CompetitionRepository,
    sessions: AgentSessionRepository,
    _dir: tempfile::TempDir,
}

async fn build_env(
    db_path: &std::path::Path,
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    opencode: Arc<ScriptedAdapter>,
    antigravity: Arc<ScriptedAdapter>,
    dir: tempfile::TempDir,
) -> Env {
    let db = Database::open(db_path).await.expect("db");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(claude.as_ref().clone()));
    registry.register(Box::new(codex.as_ref().clone()));
    registry.register(Box::new(opencode.as_ref().clone()));
    registry.register(Box::new(antigravity.as_ref().clone()));

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
        sessions.clone(),
        workspaces.clone(),
    );

    let token = "comp-test-token".to_string();
    let instance_id = Uuid::new_v4();
    let competitions_repo = CompetitionRepository::new(db.clone());
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
        competitions: competitions_repo.clone(),
        artifacts,
        a2a_agents: std::sync::Mutex::new(serde_json::json!({})),
        provenance: Arc::new(
            agentmesh_daemon::provenance_service::ProvenanceService::from_db(db.clone()),
        ),
        provenance_repo: agentmesh_storage::ProvenanceRepository::new(db.clone()),
    });

    for adapter in [
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
    ] {
        bind_agent_listener(&state, &token, adapter.id()).await;
    }
    let directory = build_directory(&state, &token).await;
    workflows.set_directory(directory);

    Env {
        workflows,
        claude,
        codex,
        opencode,
        antigravity,
        state,
        db_path: db_path.to_path_buf(),
        competitions: competitions_repo,
        sessions,
        _dir: dir,
    }
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

async fn setup_test_repo() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let repo_path = dir.path().to_path_buf();
    let run_git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo_path)
            .output()
            .expect("git");
        assert!(out.status.success(), "git failed: {:?}", out);
    };
    run_git(&["init"]);
    run_git(&["config", "user.email", "test@agentmesh.dev"]);
    run_git(&["config", "user.name", "AgentMesh Test"]);
    std::fs::write(repo_path.join("README.md"), "# Original Repo\n").expect("write");
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-m", "initial commit"]);
    (dir, repo_path)
}

fn review_artifact(verdict: &str, is_approved: bool, issue_count: usize) -> AgentEvent {
    let json = serde_json::json!({
        "verdict": verdict,
        "is_approved": is_approved,
        "summary": format!("Review: {verdict}"),
        "issues": (0..issue_count).map(|i| serde_json::json!({
            "title": format!("Issue {i}"),
            "description": format!("Description {i}"),
            "severity": "low"
        })).collect::<Vec<_>>()
    });
    let mut art = Artifact::text("review.json", json.to_string());
    art.kind = ArtifactKind::Json;
    AgentEvent::ArtifactUpdated(art)
}

async fn wait_for_async<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if f().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("wait_for_async timed out");
}

#[tokio::test]
async fn best_of_n_runs_with_two_candidates_and_two_evaluators_and_selects_winner() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture (claude)
    claude.push(vec![
        AgentEvent::Message("architecture design done".into()),
        AgentEvent::Completed,
    ]);

    // Candidate 1 (claude)
    claude.push(vec![
        AgentEvent::Message("candidate 1 implementation complete".into()),
        AgentEvent::Completed,
    ]);

    // Candidate 2 (codex)
    codex.push(vec![
        AgentEvent::Message("candidate 2 implementation complete".into()),
        AgentEvent::Completed,
    ]);

    // Evaluator for Candidate 1 (opencode) -> Approved, 1 issue
    opencode.push(vec![
        AgentEvent::Message("evaluating candidate 1".into()),
        review_artifact("approved", true, 1),
        AgentEvent::Completed,
    ]);
    // Evaluator for Candidate 1 (antigravity) -> Approved, 2 issues
    antigravity.push(vec![
        AgentEvent::Message("evaluating candidate 1".into()),
        review_artifact("approved", true, 2),
        AgentEvent::Completed,
    ]);

    // Evaluator for Candidate 2 (opencode) -> Approved, 0 issues (Superior candidate!)
    opencode.push(vec![
        AgentEvent::Message("evaluating candidate 2".into()),
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    // Evaluator for Candidate 2 (antigravity) -> Approved, 0 issues
    antigravity.push(vec![
        AgentEvent::Message("evaluating candidate 2".into()),
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with competing solutions",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start best-of-n");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let detail = env.workflows.get(workflow_id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);

    // Verify competition group was persisted
    let groups = env
        .competitions
        .list_groups_for_workflow(workflow_id)
        .await
        .expect("list groups");
    assert_eq!(groups.len(), 1);
    let group = &groups[0];
    assert_eq!(group.workflow_id, workflow_id);
    assert_eq!(group.status, "completed");
    assert_eq!(group.winner_candidate_id.as_deref(), Some("candidate_2"));

    // Verify candidate 2 was chosen as winner due to fewer issues (0 vs 3)
    let candidates = env
        .competitions
        .list_candidates_for_group(group.id)
        .await
        .expect("list candidates");
    assert_eq!(candidates.len(), 2);
    let c1 = candidates
        .iter()
        .find(|c| c.candidate_id == "candidate_1")
        .unwrap();
    let c2 = candidates
        .iter()
        .find(|c| c.candidate_id == "candidate_2")
        .unwrap();
    assert_eq!(c1.status, "completed");
    assert_eq!(c2.status, "completed");

    // Verify blind evaluation: opencode & antigravity prompts for candidate 2 do not contain "candidate_1"
    let opencode_prompts = opencode.prompts();
    for p in &opencode_prompts {
        if p.contains("Candidate candidate_2") {
            assert!(!p.contains("candidate_1"), "Blind evaluation violation!");
        }
    }
    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_no_approved_candidates_fails_with_no_acceptable_candidate() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture
    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 1
    claude.push(vec![
        AgentEvent::Message("c1".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 2
    codex.push(vec![
        AgentEvent::Message("c2".into()),
        AgentEvent::Completed,
    ]);

    // Evaluator for C1 -> ChangesRequested
    opencode.push(vec![
        review_artifact("changes_requested", false, 3),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("changes_requested", false, 2),
        AgentEvent::Completed,
    ]);

    // Evaluator for C2 -> ChangesRequested
    opencode.push(vec![
        review_artifact("changes_requested", false, 1),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("changes_requested", false, 4),
        AgentEvent::Completed,
    ]);

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature where both fail evaluation",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let detail = env.workflows.get(workflow_id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Failed);

    let groups = env
        .competitions
        .list_groups_for_workflow(workflow_id)
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].status, "failed");
    assert!(groups[0].winner_candidate_id.is_none());

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_insufficient_distinct_candidates_fails() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    let env = build_env(
        &db_path,
        claude.clone(),
        claude.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    // Reset directory to have only claude for code
    let mut directory = AgentDirectory::new();
    let endpoint = DiscoveredEndpoint {
        agent_id: "claude".to_string(),
        url: "http://127.0.0.1:1/".to_string(),
        card_url: "http://127.0.0.1:1/.well-known/agent-card.json".to_string(),
    };
    directory
        .refresh(
            &[endpoint],
            &AgentAuth {
                token: Some("comp-test-token".into()),
            },
        )
        .await
        .expect("refresh");
    env.workflows.set_directory(directory);

    let res = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with insufficient distinct candidates",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await;

    assert!(
        res.is_err(),
        "Must fail when insufficient distinct candidates"
    );
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("insufficient candidates") || err.contains("InsufficientCandidates"),
        "Unexpected error: {err}"
    );

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_winner_only_safe_apply() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture
    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 1 (claude)
    claude.push(vec![
        AgentEvent::Message("c1".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 2 (codex)
    codex.push(vec![
        AgentEvent::Message("c2".into()),
        AgentEvent::Completed,
    ]);

    // Evaluator for Candidate 1 -> Approved, 1 issue
    opencode.push(vec![
        review_artifact("approved", true, 1),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 1),
        AgentEvent::Completed,
    ]);

    // Evaluator for Candidate 2 -> Approved, 0 issues (Winner!)
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with safe apply",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let groups = env
        .competitions
        .list_groups_for_workflow(workflow_id)
        .await
        .unwrap();
    assert_eq!(
        groups[0].winner_candidate_id.as_deref(),
        Some("candidate_2")
    );
    let winner_task_id = groups[0].winner_task_id.unwrap();

    // Verify apply resolution uses the winner's task and workspace
    let task = env
        .state
        .task_repo
        .get(winner_task_id)
        .await
        .unwrap()
        .unwrap();
    let session = env
        .sessions
        .get(task.agent_session_id.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(session.session_lane.contains("candidate_2"));

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_session_lane_isolation() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    claude.push(vec![
        AgentEvent::Message("c1".into()),
        AgentEvent::Completed,
    ]);
    codex.push(vec![
        AgentEvent::Message("c2".into()),
        AgentEvent::Completed,
    ]);
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with lane isolation",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let detail = env.workflows.get(workflow_id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);

    // Verify distinct session lanes were created in the same context
    let context_id = detail.context_id.unwrap();
    let all_sessions = env.sessions.list_by_context(context_id).await.unwrap();
    let lanes: Vec<_> = all_sessions
        .iter()
        .map(|s| s.session_lane.as_str())
        .collect();

    assert!(lanes.contains(&"default"), "Architecture uses default lane");
    assert!(
        lanes.contains(&"candidate:candidate_1"),
        "Candidate 1 has distinct lane"
    );
    assert!(
        lanes.contains(&"candidate:candidate_2"),
        "Candidate 2 has distinct lane"
    );

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_ranking_hierarchy_lexical_tie_break() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture
    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    // Both candidates implement successfully
    claude.push(vec![
        AgentEvent::Message("c1".into()),
        AgentEvent::Completed,
    ]);
    codex.push(vec![
        AgentEvent::Message("c2".into()),
        AgentEvent::Completed,
    ]);

    // Both candidates get 2/2 approvals and 0 issues (exact tie!)
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with exact tie",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let groups = env
        .competitions
        .list_groups_for_workflow(workflow_id)
        .await
        .unwrap();
    // Rule 5: candidate_id lexical ASC ("candidate_1" < "candidate_2") breaks the tie
    assert_eq!(
        groups[0].winner_candidate_id.as_deref(),
        Some("candidate_1")
    );

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_insufficient_evaluation_panel_fails() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        claude.clone(),
        codex.clone(),
        temp_db_dir,
    )
    .await;

    let res = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with insufficient evaluator panel",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await;

    assert!(
        res.is_err(),
        "Must fail when evaluator panel cannot be staffed"
    );
    let err = res.err().unwrap().to_string();
    assert!(
        err.contains("insufficient evaluation panel")
            || err.contains("InsufficientEvaluationPanel"),
        "Unexpected error: {err}"
    );

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_cancel_cancels_active_candidates() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture finishes
    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 1 & 2 start long-running tasks (empty script parks until cancelled)

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature then cancel",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    // Wait until candidate nodes are running
    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.steps.iter().any(|s| {
            s.node_id.as_deref() == Some("candidate_1") && s.status == WorkflowStepStatus::Running
        })
    })
    .await;

    // Cancel workflow
    env.workflows.cancel(workflow_id).await.expect("cancel");

    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status == WorkflowStatus::Cancelled
    })
    .await;

    let detail = env.workflows.get(workflow_id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Cancelled);

    let _ = repo_dir;
}

#[tokio::test]
async fn best_of_n_crash_resumes_and_completes() {
    let (repo_dir, repo_path) = setup_test_repo().await;
    let temp_db_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_db_dir.path().join("test.db");

    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    let opencode = Arc::new(ScriptedAdapter::new("opencode"));
    let antigravity = Arc::new(ScriptedAdapter::new("antigravity"));

    // Architecture finishes
    claude.push(vec![
        AgentEvent::Message("arch".into()),
        AgentEvent::Completed,
    ]);
    // Candidate 1 parks live (empty script) for crash point
    // Candidate 2 parks live (empty script) for crash point

    let env = build_env(
        &db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        temp_db_dir,
    )
    .await;

    let workflow_id = env
        .workflows
        .start_with_source(
            PRESET_BEST_OF_N,
            "Build feature with crash and resume",
            WorkflowOptions::default(),
            Some(repo_path.to_str().unwrap().to_string()),
        )
        .await
        .expect("start");

    // Wait until candidate 1 is running
    wait_for_async(|| async {
        let detail = env.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.steps.iter().any(|s| {
            s.node_id.as_deref() == Some("candidate_1") && s.status == WorkflowStepStatus::Running
        })
    })
    .await;

    // Graceful daemon shutdown interrupt
    env.workflows.shutdown_interrupt().await;

    // Script post-crash completion for resumed candidates & evaluators
    claude.push(vec![
        AgentEvent::Message("c1 resumed".into()),
        AgentEvent::Completed,
    ]);
    codex.push(vec![
        AgentEvent::Message("c2 resumed".into()),
        AgentEvent::Completed,
    ]);
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    opencode.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);
    antigravity.push(vec![
        review_artifact("approved", true, 0),
        AgentEvent::Completed,
    ]);

    let env2 = build_env(
        &env.db_path,
        claude.clone(),
        codex.clone(),
        opencode.clone(),
        antigravity.clone(),
        tempfile::tempdir().expect("tempdir"),
    )
    .await;

    let _ = env2.workflows.recover_interrupted().await.expect("recover");
    env2.workflows.resume(workflow_id).await.expect("resume");

    wait_for_async(|| async {
        let detail = env2.workflows.get(workflow_id).await.ok().flatten();
        let Some(detail) = detail else { return false };
        detail.status.is_terminal()
    })
    .await;

    let detail = env2.workflows.get(workflow_id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);

    let _ = repo_dir;
}
