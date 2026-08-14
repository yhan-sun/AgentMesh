//! Phase 14 cleanup end-to-end tests over the daemon layer.
//!
//! Uses a real HTTP daemon for the main cleanup flow and direct
//! [`crate::cleanup`] calls for the safety guards (live task, lease, workflow
//! all-or-nothing). Workspaces are real Git worktrees; no external agent.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentMessage, AgentSession, AgentSkill, AgentTask, Context,
    WorkspaceRequirement,
};
use agentmesh_daemon::cleanup;
use agentmesh_daemon::lease::SessionLeaseManager;
use agentmesh_daemon::registry::LiveTaskRegistry;
use agentmesh_daemon::server::{self, DaemonState};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ApplyRow, ApplyStatus, ArtifactRepository,
    ContextRepository, Database, TaskRepository, WorkflowPlanRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowRow, WorkflowStepRepository, WorkflowStepRow, WorkspaceRepository,
    WorkspaceState,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::{Workspace, WorkspaceManager, workspace_snapshot_hash};
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Writes an agent change into its worktree, then completes.
struct WorktreeWriterAdapter {
    id: String,
}

impl WorktreeWriterAdapter {
    fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[async_trait]
impl CodingAgentAdapter for WorktreeWriterAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "WorktreeWriter"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: self.id.clone(),
            name: "WorktreeWriter".to_string(),
            description: None,
            skills: vec![AgentSkill::new("code", None)],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        let workspace = request.workspace.expect("workspace");
        std::fs::write(workspace.join("tracked.txt"), "agent change\n").expect("write");
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let _ = tx.send(AgentEvent::Started).await;
            let _ = tx.send(AgentEvent::Completed).await;
        });
        Ok(AgentRunHandle::with_session_channel(
            Uuid::new_v4(),
            rx,
            session_rx,
        ))
    }
    async fn resume(
        &self,
        _native_session_id: &str,
        _request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        Err(AgentError::Unsupported("not needed".into()))
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
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

fn branch_exists(dir: &Path, branch: &str) -> bool {
    !git_output(dir, &["branch", "--list", branch]).is_empty()
}

struct Env {
    state: Arc<DaemonState>,
    contexts: ContextRepository,
    db: Database,
    token: String,
    source_root: PathBuf,
    _dir: tempfile::TempDir,
}

async fn build_env(source_root: &Path) -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("db");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(WorktreeWriterAdapter::new("writer")));

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
        artifacts,
        contexts.clone(),
        sessions,
        workspaces.clone(),
    );
    let instance_id = Uuid::new_v4();
    let token = "test-token-1234567890abcdef".to_string();
    let competitions_repo = agentmesh_storage::CompetitionRepository::new(db.clone());
    let workflows = agentmesh_daemon::workflow_service::WorkflowService::new(
        instance_id,
        manager.clone(),
        WorkflowRepository::new(db.clone()),
        WorkflowStepRepository::new(db.clone()),
        WorkflowPlanRepository::new(db.clone()),
        WorkflowReplanRepository::new(db.clone()),
        agentmesh_storage::EvaluationRepository::new(db.clone()),
        competitions_repo.clone(),
        workspaces.clone(),
        agentmesh_orchestrator::router::RuleRouter::new(
            agentmesh_core::AgentMeshConfig::load().routing_config(),
        ),
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
        artifacts: ArtifactRepository::new(db.clone()),
        a2a_agents: std::sync::Mutex::new(serde_json::json!({})),
        provenance: Arc::new(
            agentmesh_daemon::provenance_service::ProvenanceService::from_db(db.clone()),
        ),
        provenance_repo: agentmesh_storage::ProvenanceRepository::new(db.clone()),
    });
    Env {
        state,
        contexts,
        db,
        token,
        source_root: source_root.to_path_buf(),
        _dir: dir,
    }
}

/// Create a context + session + task + isolated worktree; returns (task_id, workspace).
async fn create_chain(env: &Env, agent: &str) -> (Uuid, Workspace) {
    let context = Context::new();
    let session = AgentSession::new(context.id, agent);
    let mut task = AgentTask::with_workspace(agent, AgentMessage::user("work"), None);
    task.context_id = context.id;
    task.agent_session_id = Some(session.id);
    env.contexts
        .create_run_setup(&context, &session, &task)
        .await
        .expect("create chain");
    let workspace = env
        .state
        .workspaces
        .ensure_workspace(&session, &env.source_root)
        .await
        .expect("workspace");
    (task.id, workspace)
}

/// Record a completed apply for a workspace and mark it `Applied`.
async fn mark_applied(env: &Env, task_id: Uuid, ws: &Workspace) {
    std::fs::write(ws.path.join("tracked.txt"), "agent\n").unwrap();
    let diff = env.state.workspaces.diff(ws).await.unwrap();
    let hash = workspace_snapshot_hash(&ws.path, &diff);
    env.state
        .applies
        .create(&ApplyRow {
            id: Uuid::new_v4(),
            task_id: Some(task_id),
            workflow_id: None,
            workspace_id: ws.id,
            source_repository: env.source_root.clone(),
            base_revision: ws.base_revision.clone(),
            status: ApplyStatus::Completed,
            error: None,
            created_at: "2026-08-01T00:00:00+00:00".to_string(),
            completed_at: None,
            workspace_snapshot_hash: Some(hash),
        })
        .await
        .unwrap();
    env.state
        .workspaces
        .repository()
        .set_state(ws.id, WorkspaceState::Applied)
        .await
        .unwrap();
}

// ---------- HTTP main flow ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_end_to_end_over_http() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (addr, router, listener) = server::bind(env.state.clone()).await.expect("bind");
    tokio::spawn(server::serve(listener, router, env.state.shutdown.clone()));
    let client = agentmesh_daemon::DaemonClient::new(
        &agentmesh_daemon::protocol::DaemonMeta {
            protocol_version: agentmesh_daemon::protocol::DAEMON_PROTOCOL_VERSION,
            instance_id: env.state.instance_id.to_string(),
            pid: 0,
            address: addr.to_string(),
            started_at: String::new(),
        },
        env.token.clone(),
    );

    // Run + apply a task so the workspace becomes `Applied`.
    let run = client
        .run("writer", "change tracked.txt", Some(&root))
        .await
        .expect("run");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let done = client
            .get_task(run.task_id)
            .await
            .ok()
            .flatten()
            .map(|t| {
                let status = t.get("status").and_then(|v| v.as_str()).unwrap_or("");
                matches!(status, "completed" | "failed" | "cancelled")
            })
            .unwrap_or(false);
        if done || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    match client.apply_task(run.task_id, false).await.expect("apply") {
        agentmesh_daemon::protocol::ApplyResponse::Applied { outcome } => {
            assert!(outcome.tracked_applied);
        }
        _ => panic!("apply must succeed"),
    }

    // Resolve the workspace path + branch from the DB.
    let task = env
        .state
        .task_manager
        .get_task(run.task_id)
        .await
        .unwrap()
        .unwrap();
    let session_id = task.agent_session_id.unwrap();
    let ws = env
        .state
        .workspaces
        .workspace_for_session(session_id)
        .await
        .unwrap();
    let path = ws.path.clone();
    let branch = ws.branch.clone();
    let head = git_output(&root, &["rev-parse", "HEAD"]);

    // --check: preview only, nothing deleted.
    match client.cleanup_task(run.task_id, true).await.expect("plan") {
        agentmesh_daemon::protocol::CleanupResponse::Plan { plan } => {
            assert!(plan.safe);
        }
        _ => panic!("expected a plan"),
    }
    assert!(path.exists());
    assert!(branch_exists(&root, &branch));

    // --yes: worktree + managed branch removed; source and history preserved.
    match client
        .cleanup_task(run.task_id, false)
        .await
        .expect("cleanup")
    {
        agentmesh_daemon::protocol::CleanupResponse::Removed { outcome } => {
            assert!(outcome.worktree_removed);
            assert!(outcome.branch_removed);
        }
        _ => panic!("expected an outcome"),
    }
    assert!(!path.exists(), "worktree must be gone");
    assert!(
        !branch_exists(&root, &branch),
        "managed branch must be gone"
    );
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);
    // The source keeps the applied result — cleanup never reverts it.
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "agent change\n"
    );

    // History is preserved: task, session, apply all still queryable.
    assert!(
        env.state
            .task_manager
            .get_task(run.task_id)
            .await
            .unwrap()
            .is_some()
    );
    let applies = env.state.applies.list().await.unwrap();
    assert_eq!(applies.len(), 1);
    let row = env
        .state
        .workspaces
        .repository()
        .get(ws.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, WorkspaceState::Removed);

    // The AgentSession and its native session id are preserved (Phase 14 §15).
    let sessions = AgentSessionRepository::new(env.db.clone());
    let session = sessions
        .get(session_id)
        .await
        .unwrap()
        .expect("session kept");
    assert_eq!(session.id, session_id);
    assert!(
        session.native_session_id.is_some(),
        "native session must be preserved"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_task_blocks_cleanup() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (task_id, ws) = create_chain(&env, "writer").await;
    mark_applied(&env, task_id, &ws).await;

    // A live (working) task is bound to the workspace's session.
    let live = Arc::new(agentmesh_daemon::registry::LiveTask {
        task_id: Uuid::new_v4(),
        context_id: Uuid::new_v4(),
        agent_session_id: ws.agent_session_id,
        agent_id: "writer".to_string(),
        status: tokio::sync::RwLock::new(agentmesh_core::TaskStatus::Working),
        replay: tokio::sync::RwLock::new(agentmesh_daemon::registry::ReplayBuffer::new(8, 1024)),
        broadcaster: tokio::sync::broadcast::channel(8).0,
        manager: env.state.task_manager.clone(),
        run_id: Uuid::new_v4(),
    });
    env.state.registry.insert(live).await;

    let err = cleanup::plan_cleanup_task(&env.state, task_id).await;
    let msg = err.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        msg.contains("not safe to remove"),
        "live task must block cleanup: {msg}"
    );
    // Nothing was removed.
    assert!(ws.path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_lease_blocks_cleanup() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (task_id, ws) = create_chain(&env, "writer").await;
    mark_applied(&env, task_id, &ws).await;

    // Hold the session lease as if a run were active.
    let _lease = env
        .state
        .leases
        .acquire(ws.agent_session_id, Uuid::new_v4())
        .unwrap();

    let err = cleanup::plan_cleanup_task(&env.state, task_id).await;
    let msg = err.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        msg.contains("not safe to remove"),
        "an active lease must block cleanup: {msg}"
    );
    assert!(ws.path.exists());
}

// ---------- workflow cleanup ----------

fn workflow_row(id: Uuid, status: &str) -> WorkflowRow {
    WorkflowRow {
        id,
        preset: "architect-implement-review".to_string(),
        goal: "goal".to_string(),
        status: status.to_string(),
        context_id: None,
        options_json: "{}".to_string(),
        review_rounds: 0,
        runtime_owner: None,
        runtime_heartbeat_at: None,
        error: None,
        created_at: "2026-08-01T00:00:00+00:00".to_string(),
        updated_at: "2026-08-01T00:00:00+00:00".to_string(),
        completed_at: Some("2026-08-01T00:00:01+00:00".to_string()),
        graph_revision: 1,
        parent_workflow_id: None,
        recovery_of_node_id: None,
        recovery_attempt: 0,
        source_workspace: None,
    }
}

fn step_row(workflow_id: Uuid, ordinal: usize, role: &str, task_id: Uuid) -> WorkflowStepRow {
    WorkflowStepRow {
        id: Uuid::new_v4(),
        workflow_id,
        ordinal: ordinal as i64,
        node_id: None,
        role: role.to_string(),
        intent: "implementation".to_string(),
        objective: None,
        status: "completed".to_string(),
        agent_id: Some("codex".to_string()),
        task_id: Some(task_id),
        review_round: 0,
        summary: Some("done".to_string()),
        result_json: None,
        created_at: "2026-08-01T00:00:00+00:00".to_string(),
        started_at: None,
        completed_at: Some("2026-08-01T00:00:01+00:00".to_string()),
        error: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_cleanup_all_or_nothing() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (a_task, a_ws) = create_chain(&env, "codex").await;
    let (b_task, b_ws) = create_chain(&env, "claude").await;
    let (c_task, c_ws) = create_chain(&env, "codex").await;
    mark_applied(&env, a_task, &a_ws).await;
    mark_applied(&env, b_task, &b_ws).await;
    // `c` is left unapplied + active: it makes the whole workflow unsafe.

    let workflow_id = Uuid::new_v4();
    env.state
        .workflows_repo
        .create(&workflow_row(workflow_id, "completed"))
        .await
        .unwrap();
    env.state
        .steps
        .upsert(&step_row(workflow_id, 0, "implementer", a_task))
        .await
        .unwrap();
    env.state
        .steps
        .upsert(&step_row(workflow_id, 1, "reviewer", b_task))
        .await
        .unwrap();
    env.state
        .steps
        .upsert(&step_row(workflow_id, 2, "fixer", c_task))
        .await
        .unwrap();

    // One unsafe workspace → the whole cleanup is refused, nothing removed.
    let err = cleanup::cleanup_workflow(&env.state, workflow_id).await;
    assert!(
        err.is_err(),
        "unsafe workspace must fail the whole workflow cleanup"
    );
    assert!(a_ws.path.exists(), "A must not be removed");
    assert!(b_ws.path.exists(), "B must not be removed");
    assert!(c_ws.path.exists(), "C must not be removed");

    // Make C safe → the whole workflow cleans up.
    mark_applied(&env, c_task, &c_ws).await;
    let outcomes = cleanup::cleanup_workflow(&env.state, workflow_id)
        .await
        .expect("cleanup");
    assert_eq!(outcomes.len(), 3);
    assert!(!a_ws.path.exists());
    assert!(!b_ws.path.exists());
    assert!(!c_ws.path.exists());
    // The source repository is untouched.
    assert!(
        git_output(&root, &["status", "--porcelain"])
            .trim()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_workflow_refuses_cleanup() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (a_task, a_ws) = create_chain(&env, "codex").await;
    mark_applied(&env, a_task, &a_ws).await;

    let workflow_id = Uuid::new_v4();
    env.state
        .workflows_repo
        .create(&workflow_row(workflow_id, "running"))
        .await
        .unwrap();
    env.state
        .steps
        .upsert(&step_row(workflow_id, 0, "implementer", a_task))
        .await
        .unwrap();

    let err = cleanup::cleanup_workflow(&env.state, workflow_id).await;
    let msg = err.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(msg.contains("still active"), "{msg}");
    assert!(a_ws.path.exists());
}

// ---------- artifact prune ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artifact_prune_preview_then_execute() {
    let (_repo, root) = clean_repo();
    let env = build_env(&root).await;
    let (task_id, ws) = create_chain(&env, "writer").await;

    // A file-backed artifact of a terminal task whose workspace is removed.
    let store =
        agentmesh_storage::artifact_store::ArtifactStore::new(env._dir.path().join("artifacts"));
    let artifact_repo =
        agentmesh_storage::ArtifactRepository::with_store(env.db.clone(), store.clone());
    let artifact_id = Uuid::new_v4();
    // Content larger than MAX_INLINE_CONTENT forces the file-backed store path.
    let artifact = agentmesh_core::Artifact {
        id: artifact_id,
        name: "changes.patch".to_string(),
        kind: agentmesh_core::ArtifactKind::Patch,
        mime_type: "text/x-diff".to_string(),
        path: None,
        content: vec![b'x'; 300_000],
        metadata: Default::default(),
    };
    artifact_repo.insert(task_id, &artifact).await.unwrap();
    let stored = artifact_repo.list_by_task(task_id).await.unwrap().remove(0);
    let path = stored.path.expect("file-backed artifact path");

    // Terminal task, workspace removed → prunable. A future cutoff makes the
    // just-inserted artifact (created_at = now) qualify as "old enough".
    env.state.task_repo.mark_completed(task_id).await.unwrap();
    env.state
        .workspaces
        .repository()
        .set_state(ws.id, WorkspaceState::Removed)
        .await
        .unwrap();

    let older = chrono::Utc::now() + chrono::Duration::days(1);
    let preview = artifact_repo.prune_files(&older, true).await.unwrap();
    assert_eq!(preview.candidates, 1);
    assert_eq!(preview.pruned, 0);
    assert!(path.exists(), "preview must not delete");

    let run = artifact_repo.prune_files(&older, false).await.unwrap();
    assert_eq!(run.pruned, 1);
    assert!(!path.exists(), "file must be pruned");
    // SQLite metadata row is kept with its path cleared.
    let rows = artifact_repo.list_by_task(task_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].path.is_none(), "path must be cleared, history kept");
    let _ = ws;
}
