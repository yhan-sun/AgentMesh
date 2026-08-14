//! Phase 22 consensus fix-loop integration tests: after a ChangesRequested
//! gate, the workflow extends its own DAG with ONE fix round
//! (fix_r1 → evaluator_r1_* → consensus_gate_r1), subject to max_review_rounds
//! and the dynamic evaluation budget. Round 2 Approved completes the workflow;
//! round 2 ChangesRequested fails it.
//!
//! Covers: fix-loop shape, budget gate, session/worktree reuse, distinct
//! agents within a round, round persistence, crash resume during each phase,
//! cancellation, and source_workspace persistence + recovery inheritance.

use std::collections::HashMap;
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
use agentmesh_daemon::workflow_service::{EvaluationOverride, WorkflowService};
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::RuleRouter;
use agentmesh_orchestrator::{
    ConsensusOutcome, WorkflowOptions, WorkflowStatus, WorkflowStepStatus,
};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowRecoveryRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository, member_status,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Adapter that replays a FIFO script per started task and records prompts.
/// An empty script keeps the task live; a live task finishes `Cancelled` when
/// its run is cancelled through [`Self::cancel`] (the scheduler's interrupt /
/// cancel path relies on the adapter acknowledging cancellation).
#[derive(Clone)]
struct ScriptedAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<AgentEvent>>>>,
    recorded: Arc<Mutex<Vec<String>>>,
    cancels: Arc<Mutex<HashMap<Uuid, Arc<AtomicBool>>>>,
    step: std::time::Duration,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            recorded: Arc::new(Mutex::new(Vec::new())),
            cancels: Arc::new(Mutex::new(HashMap::new())),
            step: std::time::Duration::from_millis(3),
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
        tracing::debug!(agent = %self.id, script_len = script.len(), "fixloop scripted spawn");
        let run_id = Uuid::new_v4();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancels
            .lock()
            .unwrap()
            .insert(run_id, cancel_flag.clone());
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let step = self.step;
        let agent_id = self.id.clone();
        let cancels = self.cancels.clone();
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let sent = tx.send(AgentEvent::Started).await;
            tracing::debug!(agent = %agent_id, sent_ok = sent.is_ok(), "fixloop scripted started emit");
            for event in script {
                tokio::time::sleep(step).await;
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::debug!(agent = %agent_id, "fixloop scripted cancelled mid-script");
                    let _ = tx
                        .send(AgentEvent::StatusChanged(
                            agentmesh_core::TaskStatus::Cancelled,
                        ))
                        .await;
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
                let sent = tx.send(event.clone()).await;
                tracing::debug!(agent = %agent_id, event = ?event, sent_ok = sent.is_ok(), "fixloop scripted event sent");
                if sent.is_err() {
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
                if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                    cancels.lock().unwrap().remove(&run_id);
                    return;
                }
            }
            // Empty script → park live until cancelled.
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
    async fn cancel(&self, run_id: &str) -> Result<(), AgentError> {
        let run_id = Uuid::parse_str(run_id)
            .map_err(|_| AgentError::InvalidRequest(format!("invalid run id `{run_id}`")))?;
        if let Some(flag) = self.cancels.lock().unwrap().get(&run_id).cloned() {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(agent = %self.id, %run_id, "fixloop scripted cancel: flag set");
        } else {
            tracing::debug!(agent = %self.id, %run_id, "fixloop scripted cancel: run not found");
        }
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
    tasks: TaskRepository,
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

    let artifacts = ArtifactRepository::new(db.clone());
    let token = "fix-test-token".to_string();
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
        task_repo: tasks.clone(),
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
        tasks,
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

/// A clean temp git repository with one tracked file at HEAD.
fn clean_repo() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let git = |args: &[&str]| {
        let status = StdCommand::new("git")
            .args(args)
            .current_dir(&root)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "AgentMesh Test"]);
    git(&["config", "user.email", "agentmesh@example.invalid"]);
    std::fs::write(root.join("tracked.txt"), "base\n").expect("write");
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "initial"]);
    (dir, root)
}

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

fn implement_script(summary: &str) -> Vec<AgentEvent> {
    vec![AgentEvent::Message(summary.into()), AgentEvent::Completed]
}

fn evaluation_script(verdict: &str) -> Vec<AgentEvent> {
    let value = serde_json::json!({
        "verdict": verdict,
        "confidence": 0.9,
        "summary": format!("evaluation {verdict}"),
        "issues": [{"severity": "high", "title": "a bug", "description": "found a bug", "file": "src/x.rs"}],
    });
    vec![
        AgentEvent::Message(format!("evaluation {verdict}")),
        AgentEvent::ArtifactUpdated(json_artifact("evaluation.json", value)),
        AgentEvent::Completed,
    ]
}

/// A fixer script: like a normal implementer (it runs on the codex session).
fn fixer_script(summary: &str) -> Vec<AgentEvent> {
    implement_script(summary)
}

async fn wait_for_status(workflows: &Arc<WorkflowService>, id: Uuid, expected: WorkflowStatus) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let detail = workflows.get(id).await.ok().flatten();
        if detail.as_ref().is_some_and(|d| d.status == expected) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "workflow did not reach {expected:?}; state: {detail:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
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
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
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

/// Queue the full round-0 + round-1 scripts: round 0 requests changes
/// (2 changes, 1 approve), round 1 approves (2 approve, 1 change).
fn push_fix_then_approve(env: &Env) {
    env.claude.push(architecture_script()); // architecture
    env.codex.push(implement_script("implemented v1")); // implementation
    // Round-0 evaluators: 2 changes → gate requests changes.
    env.claude.push(evaluation_script("changes_requested")); // evaluator_1
    env.codex.push(evaluation_script("changes_requested")); // evaluator_2
    env.opencode.push(evaluation_script("approved")); // evaluator_3
    // Fixer reuses the codex session/worktree.
    env.codex.push(fixer_script("fixed the issues")); // fix_r1
    // Round-1 evaluators: 2 approve → gate_r1 approves.
    env.claude.push(evaluation_script("approved")); // evaluator_r1_1
    env.codex.push(evaluation_script("approved")); // evaluator_r1_2
    env.opencode.push(evaluation_script("changes_requested")); // evaluator_r1_3
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approved_first_round_completes_without_fixer() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("implemented"));
    env.claude.push(evaluation_script("approved"));
    env.codex.push(evaluation_script("approved"));
    env.opencode.push(evaluation_script("changes_requested"));

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    // No fixer was appended and no round-1 nodes exist.
    assert_eq!(node_status(&detail, "fix_r1"), WorkflowStepStatus::Pending);
    assert_eq!(
        node_status(&detail, "consensus_gate"),
        WorkflowStepStatus::Completed
    );
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 1, "approved first round has no fix round");
    assert_eq!(groups[0].round, 0);
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(groups[0].consensus.as_deref().unwrap()).unwrap();
    assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_requested_triggers_fix_and_second_round_approves() {
    let env = test_env().await;
    push_fix_then_approve(&env);
    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    // Round-0 gate requested changes (Failed); fix + round-1 nodes ran.
    assert_eq!(
        node_status(&detail, "consensus_gate"),
        WorkflowStepStatus::Failed
    );
    assert_eq!(
        node_status(&detail, "fix_r1"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(
        node_status(&detail, "consensus_gate_r1"),
        WorkflowStepStatus::Completed
    );
    assert_eq!(detail.graph_revision, 2, "the fix loop bumped the revision");

    // Two groups: round 0 ChangesRequested, round 1 Approved.
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 2);
    let g0 = groups.iter().find(|g| g.round == 0).unwrap();
    let g1 = groups.iter().find(|g| g.round == 1).unwrap();
    let c0: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(g0.consensus.as_deref().unwrap()).unwrap();
    let c1: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(g1.consensus.as_deref().unwrap()).unwrap();
    assert_eq!(c0.outcome, ConsensusOutcome::ChangesRequested);
    assert_eq!(c1.outcome, ConsensusOutcome::Approved);
    assert_eq!(g0.round, 0);
    assert_eq!(g1.round, 1, "the round field distinguishes fix rounds");

    // The fixer prompt received the aggregated issues + implementation summary.
    let fixer_prompts: Vec<String> = env
        .codex
        .prompts()
        .into_iter()
        .filter(|p| p.contains("You are the Fixer"))
        .collect();
    assert_eq!(fixer_prompts.len(), 1);
    assert!(
        fixer_prompts[0].contains("a bug"),
        "aggregated issues present"
    );
    assert!(
        fixer_prompts[0].contains("implemented v1"),
        "implementation summary present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_round_changes_requested_fails_workflow() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    // Round 0: changes requested → fixer.
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));
    env.codex.push(fixer_script("fixed once"));
    // Round 1: still changes requested → workflow fails (only one fix round).
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Failed);
    assert_eq!(
        node_status(&detail, "consensus_gate_r1"),
        WorkflowStepStatus::Failed
    );
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 2);
    let c1: agentmesh_orchestrator::evaluation::ConsensusResult = serde_json::from_str(
        groups
            .iter()
            .find(|g| g.round == 1)
            .unwrap()
            .consensus
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(c1.outcome, ConsensusOutcome::ChangesRequested);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_review_rounds_zero_fails_after_first_changes() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));

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

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Failed);
    // No fixer, no round-1 group.
    assert_eq!(node_status(&detail, "fix_r1"), WorkflowStepStatus::Pending);
    assert_eq!(env.workflows.evaluation_groups(id).await.unwrap().len(), 1);
    // No codex task beyond implementation + round-0 evaluator_2.
    let codex_prompts = env.codex.prompts();
    assert_eq!(codex_prompts.len(), 2, "no fixer ever started");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixer_reuses_implementer_session() {
    let env = test_env().await;
    push_fix_then_approve(&env);
    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    let impl_task = detail
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("implementation"))
        .and_then(|s| s.task_id)
        .expect("implementation task");
    let fixer_task = detail
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("fix_r1"))
        .and_then(|s| s.task_id)
        .expect("fixer task");
    let impl_session = env
        .tasks
        .get(impl_task)
        .await
        .unwrap()
        .expect("impl task")
        .agent_session_id;
    let fixer_session = env
        .tasks
        .get(fixer_task)
        .await
        .unwrap()
        .expect("fixer task")
        .agent_session_id;
    assert_eq!(
        impl_session, fixer_session,
        "the fixer reuses the implementer's agent session/worktree"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_two_evaluators_are_distinct_within_the_round() {
    let env = test_env().await;
    push_fix_then_approve(&env);
    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    let g1 = groups.iter().find(|g| g.round == 1).unwrap();
    let members = env.workflows.evaluation_members(g1.id).await.unwrap();
    assert_eq!(members.len(), 3);
    let mut agents: Vec<String> = members.iter().map(|m| m.agent_id.clone()).collect();
    agents.sort();
    let mut distinct = agents.clone();
    distinct.dedup();
    assert_eq!(
        agents.len(),
        distinct.len(),
        "agents distinct within a round"
    );
    for member in &members {
        assert_eq!(member.status, member_status::COMPLETED);
    }
    // Cross-round reuse is allowed (round 1 may use the same agents).
    let g0 = groups.iter().find(|g| g.round == 0).unwrap();
    let r0: Vec<String> = env
        .workflows
        .evaluation_members(g0.id)
        .await
        .unwrap()
        .iter()
        .map(|m| m.agent_id.clone())
        .collect();
    assert_eq!(agents, r0, "round 1 may reuse round-0 agents");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evaluation_budget_rejects_fix_loop_before_graph_mutation() {
    let env = test_env().await;
    // Budget: only 5 evaluator calls allowed (2 rounds × 3 = 6 > 5), so the
    // fix loop must be rejected before any fix node is appended.
    env.workflows.set_evaluation_override(EvaluationOverride {
        max_total_evaluator_calls: Some(5),
        default_evaluators: Some(3),
    });
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Failed);
    let error = detail.error.as_deref().unwrap_or_default();
    assert!(
        error.contains("EvaluationBudgetExceeded"),
        "expected budget error, got: {error}"
    );
    // The graph was NOT mutated: no fixer, revision unchanged, one group.
    assert_eq!(node_status(&detail, "fix_r1"), WorkflowStepStatus::Pending);
    assert_eq!(detail.graph_revision, 1);
    assert_eq!(env.workflows.evaluation_groups(id).await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_budget_rejects_too_many_evaluators() {
    let env = test_env().await;
    env.workflows.set_evaluation_override(EvaluationOverride {
        max_total_evaluator_calls: Some(2),
        default_evaluators: Some(3),
    });
    let err = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect_err("initial evaluator count exceeds budget");
    assert!(
        err.to_string().contains("EvaluationBudgetExceeded"),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_during_fixer_resumes_round_one_without_rerunning_round_zero() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));
    // The fixer stays live (the crash point).
    env.codex.push(Vec::new());
    // Post-resume: fixer completes, round-1 approves.
    env.codex.push(fixer_script("fixed after crash"));
    env.claude.push(evaluation_script("approved"));
    env.codex.push(evaluation_script("approved"));
    env.opencode.push(evaluation_script("changes_requested"));

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    // Wait until the fixer is Running — and actually started: the scripted
    // adapter records the (fixer) prompt only after the A2A start, so the
    // crash provably hits a live task whose first-run (empty) script was
    // consumed; a dispatch-only Running state would leave that script in the
    // queue and the resume would pick it up instead of the post-crash one.
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| node_status(&d, "fix_r1") == WorkflowStepStatus::Running)
            .unwrap_or(false)
            && env
                .codex
                .prompts()
                .iter()
                .any(|p| p.contains("You are the Fixer"))
    })
    .await;

    // Crash → fresh service over the same database. The old service must be
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
        node_status(&interrupted, "consensus_gate"),
        WorkflowStepStatus::Failed,
        "the round-0 gate stays ChangesRequested"
    );
    assert_eq!(
        node_status(&interrupted, "fix_r1"),
        WorkflowStepStatus::Interrupted
    );

    new_workflows.resume(id).await.expect("resume");
    wait_for_status(&new_workflows, id, WorkflowStatus::Completed).await;
    let done = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    assert_eq!(
        node_status(&done, "consensus_gate_r1"),
        WorkflowStepStatus::Completed
    );
    // Round-0 completed nodes were not rerun.
    for node in [
        "architecture",
        "implementation",
        "evaluator_1",
        "evaluator_3",
    ] {
        let tasks = done
            .steps
            .iter()
            .filter(|s| s.node_id.as_deref() == Some(node))
            .filter_map(|s| s.task_id)
            .count();
        assert_eq!(tasks, 1, "{node} was not rerun");
    }
    let groups = new_workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 2);
    let c1: agentmesh_orchestrator::evaluation::ConsensusResult = serde_json::from_str(
        groups
            .iter()
            .find(|g| g.round == 1)
            .unwrap()
            .consensus
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(c1.outcome, ConsensusOutcome::Approved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_during_round_one_evaluator_resumes_unfinished_only() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));
    env.codex.push(fixer_script("fixed"));
    // Round-1 evaluator_1 (claude) stays live (crash point); the others done.
    env.claude.push(Vec::new());
    env.codex.push(evaluation_script("approved"));
    env.opencode.push(evaluation_script("approved"));
    // Post-resume: evaluator_r1_1 completes with approved → gate approves.
    env.claude.push(evaluation_script("approved"));

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| {
                node_status(&d, "evaluator_r1_1") == WorkflowStepStatus::Running
                    && node_status(&d, "evaluator_r1_2") == WorkflowStepStatus::Completed
                    && node_status(&d, "evaluator_r1_3") == WorkflowStepStatus::Completed
            })
            .unwrap_or(false)
    })
    .await;

    // The old service is stopped before a fresh one takes over the same DB
    // (see crash_during_fixer_resumes_round_one_without_rerunning_round_zero).
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
    new_workflows.resume(id).await.expect("resume");
    wait_for_status(&new_workflows, id, WorkflowStatus::Completed).await;

    let done = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, WorkflowStatus::Completed);
    assert_eq!(
        node_status(&done, "consensus_gate_r1"),
        WorkflowStepStatus::Completed
    );
    // Completed round-1 evaluators were not rerun (one task each).
    for node in ["evaluator_r1_2", "evaluator_r1_3"] {
        let tasks = done
            .steps
            .iter()
            .filter(|s| s.node_id.as_deref() == Some(node))
            .filter_map(|s| s.task_id)
            .count();
        assert_eq!(tasks, 1, "{node} was not rerun");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_fixer_cancels_and_never_starts_round_one() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));
    env.codex.push(Vec::new()); // fixer stays live until cancelled

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| node_status(&d, "fix_r1") == WorkflowStepStatus::Running)
            .unwrap_or(false)
    })
    .await;
    env.workflows.cancel(id).await.expect("cancel");
    wait_for_status(&env.workflows, id, WorkflowStatus::Cancelled).await;

    // The round-1 evaluators were never started (no codex/claude/opencode
    // round-1 prompts).
    let round1_prompts = [
        env.claude.prompts(),
        env.codex.prompts(),
        env.opencode.prompts(),
    ]
    .iter()
    .flatten()
    .filter(|p| p.contains("evaluator_r1"))
    .count();
    assert_eq!(round1_prompts, 0, "round-1 evaluators never started");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_evaluator_cancels_all_running_evaluators() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("approved"));
    env.codex.push(Vec::new()); // evaluator_2 stays live
    env.opencode.push(Vec::new()); // evaluator_3 stays live

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| {
                node_status(&d, "evaluator_2") == WorkflowStepStatus::Running
                    && node_status(&d, "evaluator_3") == WorkflowStepStatus::Running
            })
            .unwrap_or(false)
    })
    .await;
    env.workflows.cancel(id).await.expect("cancel");
    wait_for_status(&env.workflows, id, WorkflowStatus::Cancelled).await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(
        node_status(&detail, "evaluator_2"),
        WorkflowStepStatus::Cancelled
    );
    assert_eq!(
        node_status(&detail, "evaluator_3"),
        WorkflowStepStatus::Cancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_workspace_persists_and_survives_resume() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(Vec::new()); // evaluator_1 live (crash point)
    env.codex.push(evaluation_script("approved"));
    env.opencode.push(evaluation_script("approved"));
    env.claude.push(evaluation_script("approved")); // resume

    let (_source, repo_root) = clean_repo();

    let id = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
            Some(repo_root.display().to_string()),
        )
        .await
        .expect("start");

    // The source workspace is rejected when it is not a git repository.
    let bad = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "x",
            WorkflowOptions::default(),
            Some(repo_root.join("missing").display().to_string()),
        )
        .await;
    assert!(bad.is_err(), "non-existent source workspace is rejected");
    assert!(
        bad.unwrap_err()
            .to_string()
            .contains("InvalidSourceWorkspace")
    );

    // Wait for the crash point (evaluator_1 running, siblings done), then resume.
    wait_for_async(|| async {
        env.workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| {
                node_status(&d, "evaluator_1") == WorkflowStepStatus::Running
                    && node_status(&d, "evaluator_2") == WorkflowStepStatus::Completed
                    && node_status(&d, "evaluator_3") == WorkflowStepStatus::Completed
            })
            .unwrap_or(false)
    })
    .await;
    // The old service is stopped before a fresh one takes over the same DB
    // (see crash_during_fixer_resumes_round_one_without_rerunning_round_zero).
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
    new_workflows.resume(id).await.expect("resume");
    wait_for_status(&new_workflows, id, WorkflowStatus::Completed).await;

    let detail = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    let persisted = new_workflows.evaluation_groups(id).await.unwrap();
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(persisted[0].consensus.as_deref().unwrap()).unwrap();
    assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_child_inherits_source_workspace() {
    let env = test_env().await;
    // Parent fails (max_review_rounds=0, changes requested).
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("changes_requested"));
    env.codex.push(evaluation_script("changes_requested"));
    env.opencode.push(evaluation_script("approved"));

    let (_source, repo_root) = clean_repo();

    let parent = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
            Some(repo_root.display().to_string()),
        )
        .await
        .expect("parent");
    wait_for_status(&env.workflows, parent, WorkflowStatus::Failed).await;

    // A recovery child workflow reuses the parent's source workspace.
    let graph = agentmesh_orchestrator::dag::WorkflowGraph::new(vec![
        agentmesh_orchestrator::dag::WorkflowNode::new(
            "implementer",
            agentmesh_orchestrator::WorkflowRole::Implementer,
        ),
        agentmesh_orchestrator::dag::WorkflowNode::with_dependencies(
            "reviewer",
            agentmesh_orchestrator::WorkflowRole::Reviewer,
            vec!["implementer".to_string()],
        ),
    ])
    .expect("chain");
    env.codex.push(implement_script("recovered"));
    env.claude.push(vec![
        AgentEvent::Message("ok".into()),
        AgentEvent::ArtifactUpdated(json_artifact(
            "review.json",
            serde_json::json!({
                "verdict": "approved",
                "summary": "approved",
                "issues": [],
            }),
        )),
        AgentEvent::Completed,
    ]);
    let child = env
        .workflows
        .start_recovery_workflow(
            "Fix the failed workflow",
            graph,
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
            parent,
            "consensus_gate",
            1,
        )
        .await
        .expect("recovery child");
    wait_for_status(&env.workflows, child, WorkflowStatus::Completed).await;

    // The child inherited the parent's source workspace.
    let child_row = env.workflows.get(child).await.unwrap().unwrap();
    assert_eq!(
        child_row.source_workspace.as_deref(),
        Some(
            repo_root
                .canonicalize()
                .unwrap()
                .display()
                .to_string()
                .as_str()
        ),
        "recovery child inherits source_workspace"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_consensus_review_regression_still_fails_on_unavailable() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.codex.push(implement_script("v1"));
    env.claude.push(evaluation_script("approved"));
    env.codex.push(vec![AgentEvent::Failed("x".into())]);
    env.opencode.push(vec![AgentEvent::Failed("y".into())]);

    let id = env
        .workflows
        .start(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;
    // Unavailable never triggers a fix loop.
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(groups.len(), 1);
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(groups[0].consensus.as_deref().unwrap()).unwrap();
    assert_eq!(consensus.outcome, ConsensusOutcome::Unavailable);
}
