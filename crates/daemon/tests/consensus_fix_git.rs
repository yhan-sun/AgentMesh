//! Phase 22 git-backed consensus fix-loop E2E tests.
//!
//! Real temporary git repositories + IsolatedGit adapters exercise the
//! workspace semantics the scripted tests cannot: implementation/fixer work in
//! the same isolated worktree, evaluators review independent snapshots, a
//! mid-evaluation workspace change voids the consensus, and Safe Apply lands
//! the fixer's result in the source repository while HEAD stays untouched.
//!
//! Also covers §5: a failed parent, a recovery child that inherits the parent's
//! source workspace and applies its fix, and a parent that cannot be applied.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentSkill, Artifact, ArtifactKind, WorkspaceRequirement,
};
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
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

// ---------- git-scripted adapter ----------

/// One action an agent task replays against its (real) isolated worktree.
#[derive(Clone)]
enum Step {
    /// Write `content` into `file` inside the task's worktree.
    Edit {
        file: String,
        content: String,
    },
    /// Emit an `evaluation.json` verdict artifact + message + Complete.
    Verdict {
        verdict: &'static str,
    },
    /// Emit a `review.json` approved artifact + Complete.
    ReviewApproved,
    Message(String),
    Complete,
}

/// A recorded run: the isolated worktree path it operated on.
#[derive(Debug, Clone)]
struct GitRun {
    workspace: Option<PathBuf>,
}

/// IsolatedGit adapter replaying FIFO scripts into its worktree.
#[derive(Clone)]
struct GitAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<Step>>>>,
    runs: Arc<Mutex<Vec<GitRun>>>,
    step: std::time::Duration,
}

impl GitAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            runs: Arc::new(Mutex::new(Vec::new())),
            step: std::time::Duration::from_millis(2),
        }
    }

    fn push(&self, script: Vec<Step>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    /// The worktree paths each run used, in start order.
    fn workspaces(&self) -> Vec<PathBuf> {
        self.runs
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| r.workspace.clone())
            .collect()
    }

    async fn spawn_run(
        &self,
        prompt: String,
        workspace: Option<PathBuf>,
    ) -> Result<AgentRunHandle, AgentError> {
        self.runs.lock().unwrap().push(GitRun {
            workspace: workspace.clone(),
        });
        let _ = prompt;
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| vec![Step::Complete]);
        let run_id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let step = self.step;
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let _ = tx.send(AgentEvent::Started).await;
            for action in script {
                match action {
                    Step::Edit { file, content } => {
                        if let Some(ws) = &workspace {
                            let _ = std::fs::write(ws.join(&file), &content);
                        }
                        tokio::time::sleep(step).await;
                    }
                    Step::Verdict { verdict } => {
                        let value = serde_json::json!({
                            "verdict": verdict,
                            "confidence": 0.9,
                            "summary": format!("evaluation {verdict}"),
                            "issues": [{"severity": "high", "title": "a bug", "description": "found a bug", "file": "src/x.rs"}],
                        });
                        let mut artifact = Artifact::text("evaluation.json", value.to_string());
                        artifact.kind = ArtifactKind::Json;
                        if tx
                            .send(AgentEvent::Message(format!("evaluation {verdict}")))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(step).await;
                        if tx
                            .send(AgentEvent::ArtifactUpdated(artifact))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(step).await;
                        if tx.send(AgentEvent::Completed).await.is_err() {
                            return;
                        }
                        return;
                    }
                    Step::ReviewApproved => {
                        let mut artifact = Artifact::text(
                            "review.json",
                            serde_json::json!({
                                "verdict": "approved",
                                "summary": "approved",
                                "issues": [],
                            })
                            .to_string(),
                        );
                        artifact.kind = ArtifactKind::Json;
                        if tx
                            .send(AgentEvent::ArtifactUpdated(artifact))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(step).await;
                        if tx.send(AgentEvent::Completed).await.is_err() {
                            return;
                        }
                        return;
                    }
                    Step::Message(content) => {
                        if tx.send(AgentEvent::Message(content)).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(step).await;
                    }
                    Step::Complete => {
                        if tx.send(AgentEvent::Completed).await.is_err() {
                            return;
                        }
                        return;
                    }
                }
            }
            // A script without a terminal action parks live.
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        Ok(AgentRunHandle::with_session_channel(run_id, rx, session_rx))
    }
}

#[async_trait]
impl CodingAgentAdapter for GitAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "GitScripted"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: self.id.clone(),
            name: format!("GitScripted {}", self.id),
            description: None,
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("review", None),
                AgentSkill::new("implementation", None),
                AgentSkill::new("testing", None),
            ],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request.input.content, request.workspace)
            .await
    }
    async fn resume(
        &self,
        _native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request.input.content, request.workspace)
            .await
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
}

// ---------- environment ----------

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
    claude: Arc<GitAdapter>,
    codex: Arc<GitAdapter>,
    opencode: Arc<GitAdapter>,
    apply: Arc<ApplyManager>,
    _dir: tempfile::TempDir,
}

async fn build_env(
    db_path: &std::path::Path,
    claude: Arc<GitAdapter>,
    codex: Arc<GitAdapter>,
    opencode: Arc<GitAdapter>,
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
    let workspaces = Arc::new(WorkspaceManager::new(
        WorkspaceRepository::new(db.clone()),
        dir.path().join("worktrees"),
    ));
    let manager = TaskManager::new(
        Arc::new(registry),
        tasks.clone(),
        artifacts.clone(),
        contexts,
        sessions,
        workspaces.clone(),
    );

    let token = "git-test-token".to_string();
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
        task_repo: tasks,
        workflows: workflows.clone(),
        plans,
        replans,
        recoveries,
        apply: apply.clone(),
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
        apply,
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

fn git(dir: &Path, args: &[&str]) {
    let status = StdCommand::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
}

fn git_output(dir: &Path, args: &[&str]) -> String {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git runs");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn clean_repo() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().canonicalize().expect("canonicalize");
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "AgentMesh Test"]);
    git(
        &root,
        &["config", "user.email", "agentmesh@example.invalid"],
    );
    std::fs::write(root.join("tracked.txt"), "base\n").expect("write");
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "initial"]);
    (dir, root)
}

async fn wait_for_status(workflows: &Arc<WorkflowService>, id: Uuid, expected: WorkflowStatus) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
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

/// The full fix-loop script: round 0 changes (2×), fixer edits the shared
/// worktree, round 1 approves (2×).
fn push_fix_then_approve(env: &Env) {
    env.claude
        .push(vec![Step::Message("architecture".into()), Step::Complete]);
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "implementation change\n".into(),
        },
        Step::Message("implemented v1".into()),
        Step::Complete,
    ]);
    env.claude.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.codex.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.opencode.push(vec![Step::Verdict {
        verdict: "approved",
    }]);
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "fixed change\n".into(),
        },
        Step::Message("fixed".into()),
        Step::Complete,
    ]);
    env.claude.push(vec![Step::Verdict {
        verdict: "approved",
    }]);
    env.codex.push(vec![Step::Verdict {
        verdict: "approved",
    }]);
    env.opencode.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_change_during_evaluation_rejects_consensus() {
    let (_repo, root) = clean_repo();
    let dir = tempfile::tempdir().expect("tempdir");
    let env = build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(GitAdapter::new("claude")),
        Arc::new(GitAdapter::new("codex")),
        Arc::new(GitAdapter::new("opencode")),
        dir,
    )
    .await;

    env.claude
        .push(vec![Step::Message("architecture".into()), Step::Complete]);
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "implementation change\n".into(),
        },
        Step::Message("implemented".into()),
        Step::Complete,
    ]);
    env.claude.push(vec![Step::Verdict {
        verdict: "approved",
    }]);
    // evaluator_2 (codex, sharing the implementation worktree) modifies the
    // implementation workspace DURING evaluation → the snapshot changes.
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "modified during evaluation\n".into(),
        },
        Step::Verdict {
            verdict: "approved",
        },
    ]);
    env.opencode.push(vec![Step::Verdict {
        verdict: "approved",
    }]);

    let id = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
            Some(root.display().to_string()),
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;

    // The consensus became Unavailable (snapshot changed) and no fix loop ran.
    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    assert_eq!(
        groups.len(),
        1,
        "a changed snapshot never triggers a fix loop"
    );
    let consensus: agentmesh_orchestrator::evaluation::ConsensusResult =
        serde_json::from_str(groups[0].consensus.as_deref().unwrap()).unwrap();
    assert_eq!(consensus.outcome, ConsensusOutcome::Unavailable);
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Failed);
    assert!(
        detail
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("EvaluationSnapshotChanged"),
        "snapshot mismatch must fail the workflow with a clear error"
    );
    assert_eq!(node_status(&detail, "fix_r1"), WorkflowStepStatus::Pending);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_one_gets_a_fresh_snapshot_hash() {
    let (_repo, root) = clean_repo();
    let dir = tempfile::tempdir().expect("tempdir");
    let env = build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(GitAdapter::new("claude")),
        Arc::new(GitAdapter::new("codex")),
        Arc::new(GitAdapter::new("opencode")),
        dir,
    )
    .await;
    push_fix_then_approve(&env);

    let id = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
            Some(root.display().to_string()),
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let groups = env.workflows.evaluation_groups(id).await.unwrap();
    let g0 = groups.iter().find(|g| g.round == 0).unwrap();
    let g1 = groups.iter().find(|g| g.round == 1).unwrap();
    let h1 = g0.snapshot_hash.as_deref().expect("round-0 snapshot");
    let h2 = g1.snapshot_hash.as_deref().expect("round-1 snapshot");
    assert_ne!(
        h1, h2,
        "round-1 evaluators are bound to the NEW post-fix snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safe_apply_e2e_applies_the_fixed_result_and_leaves_head_unchanged() {
    let (_repo, root) = clean_repo();
    let dir = tempfile::tempdir().expect("tempdir");
    let env = build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(GitAdapter::new("claude")),
        Arc::new(GitAdapter::new("codex")),
        Arc::new(GitAdapter::new("opencode")),
        dir,
    )
    .await;
    push_fix_then_approve(&env);

    let id = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: 3,
            },
            Some(root.display().to_string()),
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;

    let head_before = git_output(&root, &["rev-parse", "HEAD"]);

    // The fixer/implementation shared worktree holds the final result; the
    // evaluator workspaces are never an Apply source.
    let plan = env.apply.plan_workflow(id).await.expect("apply plan");
    let codex_workspaces = env.codex.workspaces();
    let opencode_workspaces = env.opencode.workspaces();
    assert!(
        codex_workspaces.contains(&plan.workspace),
        "apply source is the implementation/fixer worktree"
    );
    assert!(
        !opencode_workspaces.contains(&plan.workspace),
        "an evaluator workspace is never the apply source"
    );

    let outcome = env.apply.apply_workflow(id).await.expect("apply");
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "fixed change\n",
        "the source receives the fixed result"
    );
    assert_eq!(
        git_output(&root, &["rev-parse", "HEAD"]),
        head_before,
        "apply never commits; HEAD is unchanged"
    );
    assert!(outcome.tracked_applied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_workflow_without_approval_is_not_applyable() {
    let (_repo, root) = clean_repo();
    let dir = tempfile::tempdir().expect("tempdir");
    let env = build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(GitAdapter::new("claude")),
        Arc::new(GitAdapter::new("codex")),
        Arc::new(GitAdapter::new("opencode")),
        dir,
    )
    .await;
    env.claude
        .push(vec![Step::Message("architecture".into()), Step::Complete]);
    env.codex
        .push(vec![Step::Message("impl".into()), Step::Complete]);
    env.claude.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.codex.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.opencode.push(vec![Step::Verdict {
        verdict: "approved",
    }]);

    let id = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
            Some(root.display().to_string()),
        )
        .await
        .expect("start");
    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;

    let err = env
        .apply
        .plan_workflow(id)
        .await
        .expect_err("failed workflow must not apply");
    assert!(
        err.to_string().contains("not completed"),
        "a failed workflow is not applyable: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_child_apply_inherits_source_and_parent_is_not_applyable() {
    let (_repo, root) = clean_repo();
    let dir = tempfile::tempdir().expect("tempdir");
    let env = build_env(
        &dir.path().join("agentmesh.db"),
        Arc::new(GitAdapter::new("claude")),
        Arc::new(GitAdapter::new("codex")),
        Arc::new(GitAdapter::new("opencode")),
        dir,
    )
    .await;

    // Parent: implementation edits the worktree, evaluators request changes,
    // max_review_rounds=0 → the parent fails (ChangesRequested).
    env.claude
        .push(vec![Step::Message("architecture".into()), Step::Complete]);
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "parent change\n".into(),
        },
        Step::Message("parent impl".into()),
        Step::Complete,
    ]);
    env.claude.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.codex.push(vec![Step::Verdict {
        verdict: "changes_requested",
    }]);
    env.opencode.push(vec![Step::Verdict {
        verdict: "approved",
    }]);

    let parent = env
        .workflows
        .start_with_source(
            agentmesh_orchestrator::dag::PRESET_CONSENSUS_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 3,
            },
            Some(root.display().to_string()),
        )
        .await
        .expect("parent");
    wait_for_status(&env.workflows, parent, WorkflowStatus::Failed).await;

    // Recovery child: implementer fixes the same worktree, reviewer approves.
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
    env.codex.push(vec![
        Step::Edit {
            file: "tracked.txt".into(),
            content: "fixed change\n".into(),
        },
        Step::Message("recovery fix".into()),
        Step::Complete,
    ]);
    env.claude.push(vec![Step::ReviewApproved]);
    let child = env
        .workflows
        .start_recovery_workflow(
            "Recover the failed workflow",
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

    // The child inherited the parent's source workspace and its fix applies.
    let child_detail = env.workflows.get(child).await.unwrap().unwrap();
    assert_eq!(
        child_detail.source_workspace.as_deref(),
        Some(root.display().to_string().as_str())
    );
    let head_before = git_output(&root, &["rev-parse", "HEAD"]);
    env.apply.apply_workflow(child).await.expect("apply child");
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "fixed change\n",
        "the recovery child's result lands in the source"
    );
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head_before);

    // The parent stays Failed and is not applyable.
    let parent_detail = env.workflows.get(parent).await.unwrap().unwrap();
    assert_eq!(parent_detail.status, WorkflowStatus::Failed);
    let err = env
        .apply
        .plan_workflow(parent)
        .await
        .expect_err("parent not applyable");
    assert!(
        err.to_string().contains("not completed"),
        "the failed parent cannot be applied: {err}"
    );
}
