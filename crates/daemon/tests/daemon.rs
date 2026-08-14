//! Daemon integration tests: real HTTP server on 127.0.0.1:0 with a
//! controllable mock adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{AgentDescriptor, AgentEvent, Artifact, TaskStatus, WorkspaceRequirement};
use agentmesh_daemon::lease::SessionLeaseManager;
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
use sqlx::Row;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Controllable adapter: blocks until released, then completes. Cancel
/// produces Cancelled.
struct ControllableAdapter {
    started: Arc<Notify>,
    release: Arc<Notify>,
    cancel: Arc<AtomicBool>,
    with_artifact: bool,
}

impl ControllableAdapter {
    fn new(started: Arc<Notify>, release: Arc<Notify>, with_artifact: bool) -> Self {
        Self {
            started,
            release,
            cancel: Arc::new(AtomicBool::new(false)),
            with_artifact,
        }
    }
}

#[async_trait]
impl CodingAgentAdapter for ControllableAdapter {
    fn id(&self) -> &str {
        "controllable"
    }
    fn name(&self) -> &str {
        "Controllable"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "controllable".into(),
            name: "Controllable".into(),
            description: None,
            skills: vec![],
            endpoint: "agent://controllable".into(),
            workspace_requirement: WorkspaceRequirement::None,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, _request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let started = self.started.clone();
        let release = self.release.clone();
        let cancel = self.cancel.clone();
        let with_artifact = self.with_artifact;
        tokio::spawn(async move {
            let _ = session_tx.send(Some("controllable-native-1".to_string()));
            let _ = tx.send(AgentEvent::Started).await;
            started.notify_one();
            tokio::select! {
                _ = release.notified() => {}
                _ = cancel_notified(cancel.clone()) => {
                    let _ = tx.send(AgentEvent::StatusChanged(TaskStatus::Cancelled)).await;
                    return;
                }
            }
            if cancel.load(Ordering::Relaxed) {
                let _ = tx
                    .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                    .await;
                return;
            }
            let _ = tx
                .send(AgentEvent::Message("hello from controllable".into()))
                .await;
            if with_artifact {
                let _ = tx
                    .send(AgentEvent::ArtifactUpdated(Artifact::text(
                        "note.txt", "hi",
                    )))
                    .await;
            }
            let _ = tx.send(AgentEvent::Completed).await;
            let _ = session_tx;
        });
        Ok(AgentRunHandle::with_session_channel(
            Uuid::new_v4(),
            rx,
            session_rx,
        ))
    }
    async fn resume(
        &self,
        native_session_id: &str,
        _request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        // Same behavior as start, echoing the native session id.
        let session_id = native_session_id.to_string();
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(Some(session_id.clone()));
        let started = self.started.clone();
        let release = self.release.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let _ = session_tx.send(Some(session_id));
            let _ = tx.send(AgentEvent::Started).await;
            started.notify_one();
            tokio::select! {
                _ = release.notified() => {}
                _ = cancel_notified(cancel.clone()) => {
                    let _ = tx.send(AgentEvent::StatusChanged(TaskStatus::Cancelled)).await;
                    return;
                }
            }
            let _ = tx.send(AgentEvent::Message("resumed ok".into())).await;
            let _ = tx.send(AgentEvent::Completed).await;
        });
        Ok(AgentRunHandle::with_session_channel(
            Uuid::new_v4(),
            rx,
            session_rx,
        ))
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        self.cancel.store(true, Ordering::Relaxed);
        Ok(())
    }
}

async fn cancel_notified(cancel: Arc<AtomicBool>) {
    // Poll the flag until cancel is set.
    while !cancel.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

struct TestEnv {
    state: Arc<DaemonState>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    addr: std::net::SocketAddr,
    token: String,
    db: Database,
    _dir: tempfile::TempDir,
}

async fn test_env() -> TestEnv {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("db");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = ControllableAdapter::new(started.clone(), release.clone(), false);
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(adapter));

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
        shutting_down: std::sync::atomic::AtomicBool::new(false),
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
    let (addr, router, listener) = server::bind(state.clone()).await.expect("bind");
    tokio::spawn(server::serve(listener, router, state.shutdown.clone()));
    TestEnv {
        state,
        started,
        release,
        addr,
        token,
        db,
        _dir: dir,
    }
}

fn client(env: &TestEnv) -> agentmesh_daemon::DaemonClient {
    agentmesh_daemon::DaemonClient::new(
        &agentmesh_daemon::protocol::DaemonMeta {
            protocol_version: agentmesh_daemon::protocol::DAEMON_PROTOCOL_VERSION,
            instance_id: env.state.instance_id.to_string(),
            pid: 0,
            address: env.addr.to_string(),
            started_at: String::new(),
        },
        env.token.clone(),
    )
}

use futures::StreamExt;

#[tokio::test]
async fn health_requires_auth() {
    let env = test_env().await;
    let bad = agentmesh_daemon::DaemonClient::new(
        &agentmesh_daemon::protocol::DaemonMeta {
            protocol_version: 1,
            instance_id: "x".into(),
            pid: 0,
            address: env.addr.to_string(),
            started_at: String::new(),
        },
        "wrong-token".into(),
    );
    let err = bad.health().await;
    assert!(matches!(
        err,
        Err(agentmesh_daemon::DaemonError::Unauthorized)
    ));
}

#[tokio::test]
async fn health_ok_with_valid_token() {
    let env = test_env().await;
    let health = client(&env).health().await.expect("health");
    assert_eq!(health.status, "ok");
    assert_eq!(health.instance_id, env.state.instance_id.to_string());
}

#[tokio::test]
async fn run_streams_and_completes() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    let mut stream = Box::pin(client(&env).events(response.task_id, 0));
    let mut messages = Vec::new();
    let mut completed = false;
    for _ in 0..100 {
        tokio::select! {
            event = stream.next() => {
                let Some(event) = event else { break };
                let event = event.expect("stream");
                if let agentmesh_daemon::protocol::DaemonStreamEvent::Agent { event } = event.data {
                    match event {
                        AgentEvent::Message(content) => messages.push(content),
                        AgentEvent::Completed => {
                            completed = true;
                            break;
                        }
                        _ => {}
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                env.release.notify_one();
            }
        }
    }
    assert!(completed, "task must complete");
    assert!(messages.contains(&"hello from controllable".to_string()));
}

#[tokio::test]
async fn attach_replays_buffered_events() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;
    // Let events accumulate.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    env.release.notify_one();

    // Wait for completion, then attach and replay everything.
    let mut saw_completed = false;
    for _ in 0..50 {
        if let Some(task) = client(&env).get_task(response.task_id).await.expect("get") {
            let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "completed" {
                saw_completed = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(saw_completed);

    let mut stream = Box::pin(client(&env).events(response.task_id, 0));
    let mut replay = Vec::new();
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(event))) => match &event.data {
                agentmesh_daemon::protocol::DaemonStreamEvent::Agent {
                    event: AgentEvent::Message(content),
                } => {
                    replay.push(content.clone());
                }
                agentmesh_daemon::protocol::DaemonStreamEvent::Agent {
                    event: AgentEvent::Completed,
                } => break,
                _ => {}
            },
            Ok(None) => break,
            _ => break,
        }
    }
    assert!(
        replay.contains(&"hello from controllable".to_string()),
        "buffered message must be replayable: {replay:?}"
    );
}

#[tokio::test]
async fn client_disconnect_does_not_cancel() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    tokio::time::timeout(std::time::Duration::from_secs(2), env.started.notified())
        .await
        .expect("started timeout");
    // Never attach: the daemon must keep running the task.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    env.release.notify_one();

    let mut completed = false;
    for _ in 0..50 {
        if let Some(task) = client(&env).get_task(response.task_id).await.expect("get") {
            let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "completed" {
                completed = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(completed, "task must complete without any attached client");
}

#[tokio::test]
async fn cross_client_cancel_really_terminates() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    // Second client cancels.
    client(&env).cancel(response.task_id).await.expect("cancel");

    let mut cancelled = false;
    for _ in 0..50 {
        if let Some(task) = client(&env).get_task(response.task_id).await.expect("get") {
            let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "cancelled" {
                cancelled = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(cancelled, "task must be cancelled in the database");

    // Cancel of a terminal task is idempotent-friendly.
    client(&env)
        .cancel(response.task_id)
        .await
        .expect("idempotent cancel");
}

#[tokio::test]
async fn session_lease_blocks_concurrent_resume() {
    // Resume needs a real session; use the run of a session then try to
    // resume it while it is still live (unsupported adapter → but the lease
    // must be checked first). Instead, verify the lease manager directly.
    let env = test_env().await;
    let lease = env
        .state
        .leases
        .acquire(Uuid::from_u128(1), Uuid::from_u128(100));
    assert!(lease.is_ok());
    let second = env
        .state
        .leases
        .acquire(Uuid::from_u128(1), Uuid::from_u128(200));
    assert!(matches!(
        second,
        Err(agentmesh_daemon::lease::LeaseError::SessionBusy { .. })
    ));
    drop(lease);
    let third = env
        .state
        .leases
        .acquire(Uuid::from_u128(1), Uuid::from_u128(300));
    assert!(third.is_ok(), "lease released after terminal");
}

#[tokio::test]
async fn shutdown_refuses_live_tasks_without_force() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    let err = client(&env).shutdown(false).await;
    assert!(err.is_err(), "shutdown must refuse live tasks");

    // Force shutdown cancels then completes.
    let result = client(&env).shutdown(true).await.expect("force shutdown");
    assert!(result.cancelled_tasks >= 1);
    let _ = response;
}

#[tokio::test]
async fn runtime_endpoint_reports_live_tasks() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    let runtime = client(&env).runtime().await.expect("runtime");
    assert_eq!(
        runtime
            .get("instance_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        env.state.instance_id.to_string()
    );
    let live_tasks = runtime
        .get("live_tasks")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        live_tasks
            .iter()
            .any(|t| t.get("task_id").and_then(|v| v.as_str())
                == Some(&response.task_id.to_string())),
        "live task must be listed"
    );
    env.release.notify_one();
}

#[tokio::test]
async fn native_session_is_persisted_via_daemon() {
    let env = test_env().await;
    let response = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    // Give the forwarder a moment to persist the watch value.
    let mut persisted: Option<String> = None;
    for _ in 0..50 {
        let row = sqlx::query("SELECT native_session_id FROM agent_sessions WHERE id = ?")
            .bind(response.agent_session_id.to_string())
            .fetch_optional(env.db.pool())
            .await
            .expect("query");
        if let Some(row) = row {
            persisted = row.get::<Option<String>, _>("native_session_id");
            if persisted.is_some() {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        persisted.as_deref(),
        Some("controllable-native-1"),
        "native session must be persisted while the run is active"
    );
    env.release.notify_one();
}

#[tokio::test]
async fn concurrent_resume_same_session_is_rejected() {
    let env = test_env().await;
    // First task: a fresh session, left running.
    let first = client(&env)
        .run("controllable", "hello", None)
        .await
        .expect("run");
    env.started.notified().await;

    // Resuming the same session while it is live must fail with session_busy.
    let err = client(&env).resume(first.task_id, "again").await;
    assert!(
        matches!(
            err,
            Err(agentmesh_daemon::DaemonError::Api { ref code, .. }) if code == "session_busy"
        ),
        "expected session_busy, got {err:?}"
    );

    // Complete the first task; the lease is released and resume now works.
    env.release.notify_one();
    let mut completed = false;
    for _ in 0..50 {
        if let Some(task) = client(&env).get_task(first.task_id).await.expect("get")
            && task.get("status").and_then(|v| v.as_str()).unwrap_or("") == "completed"
        {
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(completed);

    let resumed = client(&env)
        .resume(first.task_id, "now it can resume")
        .await;
    assert!(
        resumed.is_ok(),
        "resume after terminal must succeed: {resumed:?}"
    );
    env.release.notify_one();
}
