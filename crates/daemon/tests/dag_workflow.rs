//! Daemon DAG workflow tests (Phase 16): persistence, parallel execution,
//! crash → Interrupted → resume without rerunning completed nodes.
//!
//! A fresh `WorkflowService` over the same database simulates a daemon
//! restart; resume must not rely on the previous in-memory run.

use std::collections::{HashSet, VecDeque};
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
    PRESET_PARALLEL_REVIEW, WorkflowOptions, WorkflowStatus, WorkflowStepStatus,
};
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

/// A shared test barrier: `parties` runs park together (Phase 19 §2).
type SharedBarrier = Arc<Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<AtomicBool>)>>>;

/// Adapter that replays a FIFO script of agent events per started task.
/// An empty script keeps the task live (simulates a slow/running node).
/// `cancel` sets a flag that the live task observes and emits `Cancelled`,
/// mirroring real agents (Claude/Codex) which always surface cancellation.
///
/// A test may share a barrier across adapters via [`ScriptedAdapter::set_barrier`]:
/// every started run after the *first* parks until `parties` runs have arrived.
/// The first run is always the single root node (the scheduler awaits it before
/// promoting children), so a shared parties=N barrier parks exactly the N
/// parallel children regardless of which adapter serves them (Phase 19 §2).
#[derive(Clone)]
struct ScriptedAdapter {
    id: String,
    scripts: Arc<Mutex<VecDeque<Vec<AgentEvent>>>>,
    cancels: Arc<Mutex<std::collections::HashMap<Uuid, Arc<AtomicBool>>>>,
    step: std::time::Duration,
    /// A shared barrier + a shared global "first run" flag, set by the test.
    barrier: SharedBarrier,
}

impl ScriptedAdapter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            scripts: Arc::new(Mutex::new(VecDeque::new())),
            cancels: Arc::new(Mutex::new(std::collections::HashMap::new())),
            step: std::time::Duration::from_millis(5),
            barrier: Arc::new(Mutex::new(None)),
        }
    }

    /// Number of scripts still queued; each started run pops one, so the
    /// caller can detect that a run actually started (after the A2A start).
    fn scripts_left(&self) -> usize {
        self.scripts.lock().unwrap().len()
    }

    /// Share a barrier (and the global "first run" flag) across adapters, so
    /// every started run after the first parks until `parties` have arrived.
    fn set_barrier(&self, barrier: Arc<tokio::sync::Barrier>, first_skip: Arc<AtomicBool>) {
        *self.barrier.lock().unwrap() = Some((barrier, first_skip));
    }

    fn push(&self, script: Vec<AgentEvent>) {
        self.scripts.lock().unwrap().push_back(script);
    }

    async fn spawn_run(&self) -> Result<AgentRunHandle, AgentError> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
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
        let barrier = self.barrier.clone();
        tokio::spawn(async move {
            // Deterministic synchronization: after the first run, every started
            // run parks until `parties` runs have arrived. The first run is the
            // single root (the scheduler awaits it before promoting children),
            // so a parties=N barrier parks exactly the N parallel children.
            let (b, first_skip) = match &*barrier.lock().unwrap() {
                Some((b, first_skip)) => (Some(b.clone()), Some(first_skip.clone())),
                None => (None, None),
            };
            if let Some(b) = b
                && let Some(first_skip) = first_skip
                && first_skip.swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                // Not the first run: park until `parties` runs have arrived.
                // Never wait forever: under extreme test load a sibling run may
                // be delayed, and a stranded barrier must not deadlock the
                // test — the barrier degrades to no-sync once the wait times
                // out (the run still executes its script normally).
                tokio::select! {
                    _ = b.wait() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {}
                }
                *barrier.lock().unwrap() = None;
            }
            // The first run passes straight through (it would deadlock a
            // parties>1 barrier on its own).
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            tracing::debug!(agent = %agent_id, script_len = script.len(), "scripted spawn emitting");
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
            // Script exhausted without a terminal event: keep the task live
            // until cancelled (the loop above only checks the flag between
            // events, so a live task parks here).
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
            tracing::debug!(%run_id, "scripted cancel: flag set");
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        } else {
            tracing::debug!(%run_id, "scripted cancel: run not found");
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

    let token = "dag-test-token".to_string();
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
        workflows: workflows.clone(),
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

fn review_artifact(verdict: &str, summary: &str) -> Artifact {
    let mut review = Artifact::text(
        "review.json",
        serde_json::json!({ "verdict": verdict, "summary": summary, "issues": [] }).to_string(),
    );
    review.kind = ArtifactKind::Json;
    review
}

fn architecture_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("architecture: split auth".into()),
        AgentEvent::Completed,
    ]
}

fn review_script(verdict: &str) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message(format!("review {verdict}")),
        AgentEvent::ArtifactUpdated(review_artifact(verdict, "ok")),
        AgentEvent::Completed,
    ]
}

fn analysis_script(summary: &str) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message(summary.to_string()),
        AgentEvent::Completed,
    ]
}

fn implement_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("implemented".into()),
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

/// Push the full happy-path scripts for a completed parallel-review run.
fn push_completion_scripts(env: &Env) {
    env.claude.push(architecture_script()); // architecture
    env.claude.push(review_script("approved")); // security_review
    env.codex.push(analysis_script("tests planned")); // test_planning
    env.codex.push(implement_script()); // implementation
    env.claude.push(review_script("approved")); // review
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_persists_dag_nodes_and_dependencies_and_completes() {
    let env = test_env().await;
    push_completion_scripts(&env);

    let id = env
        .workflows
        .start(
            PRESET_PARALLEL_REVIEW,
            "Refactor auth",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
        )
        .await
        .expect("start");

    wait_for_status(&env.workflows, id, WorkflowStatus::Completed).await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    assert_eq!(detail.status, WorkflowStatus::Completed);
    assert_eq!(detail.max_parallel, 2);
    assert_eq!(detail.steps.len(), 5);
    // Every node row carries its node_id + Completed status. Nodes are
    // persisted in deterministic (node_id ascending) order.
    let node_ids: Vec<&str> = detail
        .steps
        .iter()
        .filter_map(|s| s.node_id.as_deref())
        .collect();
    assert_eq!(
        node_ids,
        vec![
            "architecture",
            "implementation",
            "review",
            "security_review",
            "test_planning"
        ]
    );
    for step in &detail.steps {
        assert_eq!(step.status, WorkflowStepStatus::Completed, "{step:?}");
    }

    // Dependency edges were persisted.
    let deps = env.steps.list_dependencies(id).await.expect("deps");
    assert!(!deps.is_empty(), "dependency edges must be persisted");
    assert_eq!(deps.len(), 5); // arch→sec, arch→test, sec→impl, test→impl, impl→review
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_resumes_without_rerunning_completed_nodes() {
    let env = test_env().await;
    // First run: architecture completes; security_review (claude) completes;
    // test_planning (codex) stays live = the "crash point".
    env.claude.push(architecture_script());
    env.claude.push(review_script("approved")); // security_review (first run)
    env.claude.push(review_script("approved")); // review (resume)
    env.codex.push(Vec::new()); // test_planning (first run, live)
    env.codex.push(analysis_script("tests planned")); // test_planning (resume)
    env.codex.push(implement_script()); // implementation

    let id = env
        .workflows
        .start(
            PRESET_PARALLEL_REVIEW,
            "goal",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
        )
        .await
        .expect("start");

    // Wait until security_review is Completed AND test_planning is Running
    // (architecture already done; both parallel nodes dispatched). Inlined
    // with diagnostics: a time-out here means a node never reached its
    // expected state (failed / not dispatched), and the panic shows which.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let detail = env.workflows.get(id).await.ok().flatten();
            let done = detail.as_ref().map(|d| {
                d.steps.iter().any(|s| {
                    s.node_id.as_deref() == Some("security_review")
                        && s.status == WorkflowStepStatus::Completed
                }) && d.steps.iter().any(|s| {
                    s.node_id.as_deref() == Some("test_planning")
                        && s.status == WorkflowStepStatus::Running
                })
            });
            if done == Some(true) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "security_review Completed + test_planning Running, got: {detail:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let before = env.workflows.get(id).await.unwrap().unwrap();
    let completed_before: HashSet<Uuid> = before
        .steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .filter_map(|s| s.task_id)
        .collect();
    assert_eq!(completed_before.len(), 2, "architecture + security_review");

    // Crash: a brand-new service over the same database. The old service is
    // fully stopped first (scheduler drained, interrupted state persisted):
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

    let recovered = new_workflows.recover_interrupted().await.expect("recover");
    assert_eq!(
        recovered, 0,
        "the graceful shutdown above already interrupted the workflow"
    );
    let interrupted = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(interrupted.status, WorkflowStatus::Interrupted);
    // test_planning was running → Interrupted; implementation/review still Pending.
    let tp = interrupted
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("test_planning"))
        .unwrap();
    assert_eq!(tp.status, WorkflowStepStatus::Interrupted);

    new_workflows.resume(id).await.expect("resume");
    wait_for_async(|| async {
        let status = new_workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| d.status)
            .unwrap_or(WorkflowStatus::Pending);
        status == WorkflowStatus::Completed
    })
    .await;

    let after = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(after.status, WorkflowStatus::Completed);
    assert_eq!(after.steps.len(), 5);

    // Completed nodes were NOT rerun: same task ids for architecture +
    // security_review.
    let after_tasks: HashSet<Uuid> = after
        .steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .filter_map(|s| s.task_id)
        .collect();
    for task in &completed_before {
        assert!(
            after_tasks.contains(task),
            "completed task {task} must not be rerun"
        );
    }
    // test_planning got a NEW task (resumed), implementation + review also new.
    assert_eq!(after_tasks.len(), 5, "2 original + 3 resumed/new tasks");
    assert_eq!(after.context_id, before.context_id, "context preserved");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_crash_both_running_nodes_resume() {
    let env = test_env().await;
    // First run: architecture completes; BOTH security_review (claude) and
    // test_planning (codex) stay live → the crash hits two parallel nodes.
    env.claude.push(architecture_script());
    env.claude.push(Vec::new()); // security_review (first run, live)
    env.claude.push(review_script("approved")); // security_review (resume)
    env.claude.push(review_script("approved")); // review
    env.codex.push(Vec::new()); // test_planning (first run, live)
    env.codex.push(analysis_script("tests planned")); // test_planning (resume)
    env.codex.push(implement_script()); // implementation

    // Deterministic (Phase 20 §3): the first run (architecture, the root) passes
    // through the shared barrier; security_review + test_planning then BOTH park
    // until they have started, so the crash provably hits two Running nodes.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_skip = Arc::new(AtomicBool::new(false));
    env.claude.set_barrier(barrier.clone(), first_skip.clone());
    env.codex.set_barrier(barrier.clone(), first_skip.clone());

    let id = env
        .workflows
        .start(
            PRESET_PARALLEL_REVIEW,
            "goal",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
        )
        .await
        .expect("start");

    // Wait until both parallel nodes are running. Inlined with diagnostics:
    // a time-out here means a node never reached Running (failed / not
    // dispatched), and the panic must show which.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let detail = env.workflows.get(id).await.ok().flatten();
            let running = detail
                .as_ref()
                .map(|d| {
                    d.steps
                        .iter()
                        .filter(|s| s.status == WorkflowStepStatus::Running)
                        .count()
                })
                .unwrap_or(0);
            // Both parallel nodes must also have *started* (each started run
            // pops one script): a dispatch-only Running crash would leave the
            // first-run scripts in the queues, and the resume would consume
            // them instead of the post-crash ones. claude pops architecture +
            // security_review (4 → ≤2), codex pops test_planning (3 → ≤2).
            let started = env.claude.scripts_left() <= 2 && env.codex.scripts_left() <= 2;
            if running >= 2 && started {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "expected 2 Running nodes, saw {running}; state: {detail:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // Crash → new service. The old service is fully stopped first (scheduler
    // drained, interrupted state persisted): a fresh scheduler over the same
    // DB would otherwise race the old one.
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
    let running_before: Vec<&str> = interrupted
        .steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Interrupted)
        .filter_map(|s| s.node_id.as_deref())
        .collect();
    assert!(running_before.contains(&"security_review"));
    assert!(running_before.contains(&"test_planning"));
    // architecture stays Completed; implementation/review stay Pending.
    let arch = interrupted
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("architecture"))
        .unwrap();
    assert_eq!(arch.status, WorkflowStepStatus::Completed);

    new_workflows.resume(id).await.expect("resume");
    wait_for_async(|| async {
        let status = new_workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| d.status)
            .unwrap_or(WorkflowStatus::Pending);
        status == WorkflowStatus::Completed
    })
    .await;

    let after = new_workflows.get(id).await.unwrap().unwrap();
    assert_eq!(after.status, WorkflowStatus::Completed);
    assert_eq!(after.steps.len(), 5);
    // Implementation (fan-in) ran only after both dependencies completed.
    let impl_node = after
        .steps
        .iter()
        .find(|s| s.node_id.as_deref() == Some("implementation"))
        .unwrap();
    assert_eq!(impl_node.status, WorkflowStepStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_dag_cancels_all_running_nodes() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.claude.push(Vec::new()); // security_review live
    env.codex.push(Vec::new()); // test_planning live
    env.codex.push(Vec::new());
    env.claude.push(Vec::new());

    let id = env
        .workflows
        .start(
            PRESET_PARALLEL_REVIEW,
            "goal",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
        )
        .await
        .expect("start");

    wait_for_async(|| async {
        let detail = env.workflows.get(id).await.ok().flatten();
        detail
            .map(|d| {
                d.steps
                    .iter()
                    .filter(|s| s.status == WorkflowStepStatus::Running)
                    .count()
                    >= 2
            })
            .unwrap_or(false)
    })
    .await;

    env.workflows.cancel(id).await.expect("cancel");
    wait_for_status(&env.workflows, id, WorkflowStatus::Cancelled).await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    let by_id: std::collections::HashMap<&str, &agentmesh_daemon::protocol::WorkflowStepInfo> =
        detail
            .steps
            .iter()
            .map(|s| (s.node_id.as_deref().unwrap_or(""), s))
            .collect();
    assert_eq!(by_id["architecture"].status, WorkflowStepStatus::Completed);
    assert_eq!(
        by_id["security_review"].status,
        WorkflowStepStatus::Cancelled
    );
    assert_eq!(by_id["test_planning"].status, WorkflowStepStatus::Cancelled);
    assert_eq!(by_id["implementation"].status, WorkflowStepStatus::Skipped);
    assert_eq!(by_id["review"].status, WorkflowStepStatus::Skipped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dag_failure_persists_failed_and_skips_downstream() {
    let env = test_env().await;
    env.claude.push(architecture_script());
    env.claude
        .push(vec![AgentEvent::Failed("security review failed".into())]);
    env.codex.push(Vec::new()); // test_planning (starts, stays live)
    env.codex.push(Vec::new()); // implementation (never starts)
    env.claude.push(Vec::new()); // review (never starts)

    // Deterministic sibling timing (Phase 19 §2): the first run (architecture,
    // the single root) passes through; security_review + test_planning then
    // both park on this shared 2-party barrier until they have *both* started,
    // so the failure cannot race ahead of test_planning's start.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_skip = Arc::new(AtomicBool::new(false));
    env.claude.set_barrier(barrier.clone(), first_skip.clone());
    env.codex.set_barrier(barrier.clone(), first_skip.clone());

    let id = env
        .workflows
        .start(
            PRESET_PARALLEL_REVIEW,
            "goal",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
        )
        .await
        .expect("start");

    wait_for_status(&env.workflows, id, WorkflowStatus::Failed).await;
    let detail = env.workflows.get(id).await.unwrap().unwrap();
    let by_id: std::collections::HashMap<&str, &agentmesh_daemon::protocol::WorkflowStepInfo> =
        detail
            .steps
            .iter()
            .map(|s| (s.node_id.as_deref().unwrap_or(""), s))
            .collect();
    // 1. The failure node is Failed.
    assert_eq!(by_id["security_review"].status, WorkflowStepStatus::Failed);
    // 2. test_planning was already Running when the failure landed (the barrier
    //    guarantees it started first) → Cancelled, never Skipped.
    assert_eq!(
        by_id["test_planning"].status,
        WorkflowStepStatus::Cancelled,
        "running sibling is cancelled (barrier makes this deterministic)"
    );
    // 3. Not-yet-started downstream never starts → Skipped.
    assert_eq!(by_id["implementation"].status, WorkflowStepStatus::Skipped);
    assert_eq!(by_id["review"].status, WorkflowStepStatus::Skipped);
    // And only three nodes ever contacted an agent (architecture, the failing
    // security_review, and the cancelled test_planning).
    let claude_runs = env.claude.scripts.lock().unwrap().len();
    let codex_runs = env.codex.scripts.lock().unwrap().len();
    assert_eq!(claude_runs, 1, "review never started (claude queue left 1)");
    assert_eq!(
        codex_runs, 1,
        "implementation never started (codex queue left 1)"
    );
}
