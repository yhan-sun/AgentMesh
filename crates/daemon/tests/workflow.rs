//! Daemon workflow service tests (Phase 12): persistence, daemon-owned
//! execution, crash → Interrupted → resume, attach replay and cancel.
//!
//! A fresh `WorkflowService` over the same database simulates a daemon
//! restart: resume must not rely on the previous in-memory run.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

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
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowReplanRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Adapter that replays a FIFO script of agent events per started task.
/// An empty script keeps the task live (used to simulate a slow step).
#[derive(Clone)]
struct ScriptedAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<AgentEvent>>>>,
    step: std::time::Duration,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            step: std::time::Duration::from_millis(5),
        }
    }

    fn push(&self, script: Vec<AgentEvent>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    async fn spawn_run(&self) -> Result<AgentRunHandle, AgentError> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
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
            // Script exhausted without a terminal event: keep the task live.
        });
        Ok(AgentRunHandle::with_session_channel(
            Uuid::new_v4(),
            rx,
            session_rx,
        ))
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
            workspace_requirement: WorkspaceRequirement::None,
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
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
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
    claude: Arc<ScriptedAdapter>,
    codex: Arc<ScriptedAdapter>,
    db_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn test_env() -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let claude = Arc::new(ScriptedAdapter::new("claude"));
    let codex = Arc::new(ScriptedAdapter::new("codex"));
    build_env(&dir.path().join("agentmesh.db"), claude, codex, dir).await
}

/// Build a fully independent daemon environment over `db_path` with the given
/// (shared) scripted adapters. A fresh lease manager + service instance makes
/// this a faithful "new daemon after a crash" — it never relies on the
/// previous in-memory run.
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

    let token = "workflow-test-token".to_string();
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
        replans: agentmesh_daemon::replan::ReplanService::new(
            workflows.clone(),
            agentmesh_storage::WorkflowReplanRepository::new(db.clone()),
        ),
        recoveries: agentmesh_daemon::recovery::RecoveryService::new(
            workflows.clone(),
            agentmesh_storage::WorkflowRecoveryRepository::new(db.clone()),
            workspaces.clone(),
        ),
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

    // Bind an A2A listener per agent, backed by the daemon.
    for adapter in [claude.clone(), codex.clone()] {
        bind_agent_listener(&state, &token, adapter.id()).await;
    }
    // Build the directory from the listeners and inject it into the service.
    let directory = build_directory(&state, &token).await;
    workflows.set_directory(directory);

    Env {
        workflows: workflows.clone(),
        claude,
        codex,
        db_path: db_path.to_path_buf(),
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

// ---------- workflow scripts ----------

fn plan_script() -> Vec<AgentEvent> {
    let mut plan = Artifact::text("plan.json", r#"{"modules":["core","a2a"]}"#);
    plan.kind = ArtifactKind::Json;
    vec![
        AgentEvent::Message("architecture: split auth".into()),
        AgentEvent::ArtifactUpdated(plan),
        AgentEvent::Completed,
    ]
}

fn implement_script() -> Vec<AgentEvent> {
    let mut patch = Artifact::text(
        "changes.patch",
        "diff --git a/auth.rs b/auth.rs\n+fn x() {}",
    );
    patch.kind = ArtifactKind::Patch;
    vec![
        AgentEvent::Message("implemented auth".into()),
        AgentEvent::ArtifactUpdated(patch),
        AgentEvent::Completed,
    ]
}

fn review_script(verdict: &str) -> Vec<AgentEvent> {
    let mut review = Artifact::text(
        "review.json",
        serde_json::json!({ "verdict": verdict, "summary": "review summary", "issues": [] })
            .to_string(),
    );
    review.kind = ArtifactKind::Json;
    vec![
        AgentEvent::Message(format!("review: {verdict}")),
        AgentEvent::ArtifactUpdated(review),
        AgentEvent::Completed,
    ]
}

fn fix_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("fixed the issues".into()),
        AgentEvent::Completed,
    ]
}

/// Poll the persisted workflow status until it equals `expected`.
async fn wait_for_status(
    workflows: &Arc<WorkflowService>,
    id: Uuid,
    expected: agentmesh_orchestrator::WorkflowStatus,
) {
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

/// Poll an async predicate until true.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_persists_workflow_and_completes_through_daemon() {
    let env = test_env().await;
    env.claude.push(plan_script());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "Refactor auth",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");

    wait_for_status(
        &env.workflows,
        id,
        agentmesh_orchestrator::WorkflowStatus::Completed,
    )
    .await;

    let detail = env.workflows.get(id).await.expect("get").expect("exists");
    assert_eq!(
        detail.status,
        agentmesh_orchestrator::WorkflowStatus::Completed
    );
    assert_eq!(detail.steps.len(), 3);
    for step in &detail.steps {
        assert_eq!(
            step.status,
            agentmesh_orchestrator::WorkflowStepStatus::Completed
        );
    }
    assert_eq!(
        detail.final_review_verdict,
        Some(agentmesh_orchestrator::ReviewVerdict::Approved)
    );
    assert!(detail.context_id.is_some(), "context must be persisted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_disconnect_does_not_stop_the_workflow() {
    let env = test_env().await;
    env.claude.push(plan_script());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    // Start without attaching to any event stream (simulates a CLI that
    // disconnected immediately).
    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");

    wait_for_status(
        &env.workflows,
        id,
        agentmesh_orchestrator::WorkflowStatus::Completed,
    )
    .await;
    // The daemon-owned run reached a terminal state on its own.
    assert_eq!(
        env.workflows.get(id).await.unwrap().unwrap().status,
        agentmesh_orchestrator::WorkflowStatus::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_persists_cancelled_and_skips_rest() {
    let env = test_env().await;
    // Architect stays live until cancelled.
    env.claude.push(Vec::new());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");

    // Wait until step 1 has started (running).
    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| {
                d.steps
                    .iter()
                    .any(|s| s.status == agentmesh_orchestrator::WorkflowStepStatus::Running)
            })
            .unwrap_or(false)
    })
    .await;

    env.workflows.cancel(id).await.expect("cancel");

    wait_for_status(
        &env.workflows,
        id,
        agentmesh_orchestrator::WorkflowStatus::Cancelled,
    )
    .await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(
        detail.status,
        agentmesh_orchestrator::WorkflowStatus::Cancelled
    );
    assert_eq!(
        detail.steps[0].status,
        agentmesh_orchestrator::WorkflowStepStatus::Cancelled
    );
    assert_eq!(
        detail.steps[1].status,
        agentmesh_orchestrator::WorkflowStepStatus::Skipped
    );
    assert_eq!(
        detail.steps[2].status,
        agentmesh_orchestrator::WorkflowStepStatus::Skipped
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_marks_interrupted_and_resume_skips_completed_steps() {
    let env = test_env().await;
    // First run: architect → implement → review(changes) → fixer stays live.
    env.claude.push(plan_script());
    env.codex.push(implement_script());
    env.claude.push(review_script("changes_requested"));
    env.codex.push(Vec::new()); // fixer stays live (the "crash point")
    // Resumed run scripts: fixer (new codex task) + final review.
    env.codex.push(fix_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions {
                max_review_rounds: 1,
                max_parallel: agentmesh_orchestrator::DEFAULT_MAX_PARALLEL,
            },
        )
        .await
        .expect("start");

    // Wait until the fixer (ordinal 3) is running.
    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| {
                d.steps.len() >= 4
                    && d.steps[3].status == agentmesh_orchestrator::WorkflowStepStatus::Running
            })
            .unwrap_or(false)
    })
    .await;

    // Capture the completed steps' task ids before the "crash".
    let before = env.workflows.get(id).await.unwrap().unwrap();
    let completed_before: Vec<Uuid> = before
        .steps
        .iter()
        .filter(|s| s.status == agentmesh_orchestrator::WorkflowStepStatus::Completed)
        .filter_map(|s| s.task_id)
        .collect();
    assert_eq!(
        completed_before.len(),
        3,
        "architect/implement/review completed"
    );

    // Crash: a brand-new daemon over the same database (fresh lease manager,
    // fresh service, shared adapters). Resume must not rely on the old
    // in-memory run. The old service is fully stopped first (its run drains
    // and persists the interrupted state): a fresh run over the same DB would
    // otherwise race the old one.
    env.workflows.shutdown_interrupt().await;
    let env2 = build_env(
        &env.db_path,
        env.claude.clone(),
        env.codex.clone(),
        tempfile::tempdir().expect("tempdir"),
    )
    .await;
    let new_workflows = env2.workflows.clone();

    // Stale recovery marks the running workflow + step Interrupted.
    let recovered = new_workflows.recover_interrupted().await.expect("recover");
    assert_eq!(
        recovered, 0,
        "the graceful shutdown above already interrupted the workflow"
    );
    let interrupted = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(
        interrupted.status,
        agentmesh_orchestrator::WorkflowStatus::Interrupted
    );
    assert_eq!(
        interrupted.steps[3].status,
        agentmesh_orchestrator::WorkflowStepStatus::Interrupted
    );

    // Resume on the new service.
    new_workflows.resume(id).await.expect("resume");

    wait_for_async(|| async {
        let status = new_workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| d.status)
            .unwrap_or(agentmesh_orchestrator::WorkflowStatus::Pending);
        status == agentmesh_orchestrator::WorkflowStatus::Completed
    })
    .await;

    let after = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(
        after.status,
        agentmesh_orchestrator::WorkflowStatus::Completed
    );
    assert_eq!(after.steps.len(), 5);

    // Completed steps were NOT rerun: same task ids for ordinals 0..=2.
    let after_tasks: Vec<Uuid> = after
        .steps
        .iter()
        .filter(|s| s.status == agentmesh_orchestrator::WorkflowStepStatus::Completed)
        .filter_map(|s| s.task_id)
        .collect();
    for task in &completed_before {
        assert!(
            after_tasks.contains(task),
            "completed task {task} must not be rerun"
        );
    }
    // The resumed fixer + final review used NEW tasks (5 distinct tasks total).
    let distinct: std::collections::HashSet<Uuid> = after_tasks.iter().copied().collect();
    assert_eq!(distinct.len(), 5, "3 original + 2 resumed tasks");
    // Context preserved across the crash.
    assert_eq!(after.context_id, before.context_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_rejects_non_interrupted_workflow() {
    let env = test_env().await;
    env.claude.push(plan_script());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");
    wait_for_status(
        &env.workflows,
        id,
        agentmesh_orchestrator::WorkflowStatus::Completed,
    )
    .await;

    let err = env.workflows.resume(id).await.expect_err("must reject");
    assert!(err.to_string().contains("not resumable"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_interrupts_running_workflow() {
    let env = test_env().await;
    // The architect stays live until interrupted (Phase 13 graceful stop).
    env.claude.push(Vec::new());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");

    // Wait until step 0 (architect) is running.
    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| {
                d.steps
                    .iter()
                    .any(|s| s.status == agentmesh_orchestrator::WorkflowStepStatus::Running)
            })
            .unwrap_or(false)
    })
    .await;

    // Graceful shutdown interrupts (never Cancels) the live workflow.
    env.workflows.shutdown_interrupt().await;

    let detail = env.workflows.get(id).await.unwrap().unwrap();
    // The workflow and its running step persist `Interrupted` — resumable
    // later, distinct from an explicit user Cancelled.
    assert_eq!(
        detail.status,
        agentmesh_orchestrator::WorkflowStatus::Interrupted
    );
    assert_eq!(
        detail.steps[0].status,
        agentmesh_orchestrator::WorkflowStepStatus::Interrupted
    );
    // Remaining steps are left untouched (never marked skipped).
    assert_eq!(
        detail.steps[1].status,
        agentmesh_orchestrator::WorkflowStepStatus::Pending
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_replays_persisted_events() {
    let env = test_env().await;
    env.claude.push(plan_script());
    env.codex.push(implement_script());
    env.claude.push(review_script("approved"));

    let id = env
        .workflows
        .start(
            "architect-implement-review",
            "goal",
            agentmesh_orchestrator::WorkflowOptions::default(),
        )
        .await
        .expect("start");
    wait_for_status(
        &env.workflows,
        id,
        agentmesh_orchestrator::WorkflowStatus::Completed,
    )
    .await;

    // Attach after completion: the replay comes from persisted state.
    let events = env.workflows.replay(id).await.expect("replay");
    use agentmesh_daemon::protocol::WorkflowStreamEvent as E;
    let completed_steps = events
        .iter()
        .filter(|e| matches!(e, E::StepCompleted { .. }))
        .count();
    assert_eq!(completed_steps, 3, "{events:?}");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, E::WorkflowCompleted { .. }))
    );
}
