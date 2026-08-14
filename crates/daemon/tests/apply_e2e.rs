//! End-to-end apply over the daemon HTTP API (Phase 13).
//!
//! A real HTTP daemon owns a task whose isolated-Git adapter writes agent
//! changes into its worktree; the CLI-shaped calls `apply_task(check)` and
//! `apply_task(apply)` drive the whole `daemon → ApplyManager → git` chain
//! against a temporary source repository. No external agent is required.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_core::{AgentDescriptor, AgentEvent, AgentSkill, WorkspaceRequirement};
use agentmesh_daemon::lease::SessionLeaseManager;
use agentmesh_daemon::protocol::ApplyResponse;
use agentmesh_daemon::registry::LiveTaskRegistry;
use agentmesh_daemon::server::{self, DaemonState};
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

/// Adapter that requires an isolated Git worktree and writes an agent change
/// into it before completing.
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
        let workspace = request.workspace.expect("isolated workspace");
        // The agent modifies a tracked file inside its worktree.
        std::fs::write(workspace.join("tracked.txt"), "agent change\n").expect("write");
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        tokio::spawn(async move {
            let _ = session_tx.send(Some("native-e2e".to_string()));
            let _ = tx.send(AgentEvent::Started).await;
            let _ = tx.send(AgentEvent::Message("made changes".into())).await;
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
        Err(AgentError::Unsupported("resume not needed".into()))
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

async fn daemon_state() -> (Arc<DaemonState>, tempfile::TempDir) {
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
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(db.clone()),
    ));
    let manager = TaskManager::new(
        Arc::new(registry),
        tasks.clone(),
        artifacts.clone(),
        contexts,
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
        agentmesh_apply::ApplyManager::new(
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
    (state, dir)
}

async fn wait_task_terminal(client: &agentmesh_daemon::DaemonClient, task_id: Uuid) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(Some(task)) = client.get_task(task_id).await {
            let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if matches!(status, "completed" | "failed" | "cancelled") {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "task did not reach a terminal state"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// The full `agentmesh apply` flow over HTTP: preview leaves the source
/// untouched, then `--yes` writes the agent changes while HEAD and the agent
/// worktree stay unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_end_to_end_over_daemon_http() {
    let (_repo, root) = clean_repo();
    let head = git_output(&root, &["rev-parse", "HEAD"]);
    let (state, _dir) = daemon_state().await;

    let (addr, router, listener) = server::bind(state.clone()).await.expect("bind");
    tokio::spawn(server::serve(listener, router, state.shutdown.clone()));
    let client = agentmesh_daemon::DaemonClient::new(
        &agentmesh_daemon::protocol::DaemonMeta {
            protocol_version: agentmesh_daemon::protocol::DAEMON_PROTOCOL_VERSION,
            instance_id: state.instance_id.to_string(),
            pid: 0,
            address: addr.to_string(),
            started_at: String::new(),
        },
        "test-token-1234567890abcdef".to_string(),
    );

    // Run a task in the source repository; the adapter writes to the worktree.
    let run = client
        .run("writer", "change tracked.txt", Some(&root))
        .await
        .expect("run");
    wait_task_terminal(&client, run.task_id).await;

    // --check: plan only, source unchanged.
    match client.apply_task(run.task_id, true).await.expect("plan") {
        ApplyResponse::Plan { plan } => {
            assert!(plan.applicable, "plan must be applicable");
            assert_eq!(plan.source_repository, root);
            assert!(
                plan.changed_files
                    .iter()
                    .any(|f| f.path == "tracked.txt" && f.status == "M"),
                "{:?}",
                plan.changed_files
            );
        }
        ApplyResponse::Applied { .. } => panic!("check must return a plan"),
    }
    assert!(git_output(&root, &["status", "--porcelain"]).is_empty());
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);

    // --yes: apply; the source gains the change, HEAD and the worktree do not.
    let outcome = match client.apply_task(run.task_id, false).await.expect("apply") {
        ApplyResponse::Applied { outcome } => outcome,
        ApplyResponse::Plan { .. } => panic!("apply must return an outcome"),
    };
    assert!(outcome.tracked_applied);
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "agent change\n"
    );
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);

    // Re-applying the same workspace is rejected as AlreadyApplied.
    let err = client
        .apply_task(run.task_id, false)
        .await
        .expect_err("already applied");
    assert!(
        err.to_string().contains("already applied") || err.to_string().contains("already_applied"),
        "{err}"
    );
}
