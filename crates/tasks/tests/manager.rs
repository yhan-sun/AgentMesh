//! TaskManager tests: full lifecycle with a fixture adapter (no real agents).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentMessage, Artifact, ArtifactKind, TaskStatus,
    WorkspaceRequirement,
};
use agentmesh_storage::{
    AgentSessionRepository, ArtifactRepository, ContextRepository, Database, TaskFilter,
    TaskRepository, WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

/// Test adapter that replays a fixed event script with cancellable pacing.
struct FixtureAdapter {
    /// Adapter id; the test harness uses distinct ids for distinct agents.
    id: String,
    script: Vec<AgentEvent>,
    cancel: Arc<AtomicBool>,
    fail_start: bool,
    /// Native session id published via the watch channel, when set.
    native_session_id: Option<String>,
    /// Whether resume() should be supported.
    resume_ok: bool,
    /// Workspace requirement declared in the descriptor.
    workspace_requirement: WorkspaceRequirement,
    /// (filename, content) written into the execution workspace on start.
    write_file: Option<(&'static str, &'static str)>,
}

#[async_trait]
impl CodingAgentAdapter for FixtureAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Fixture"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: self.id.clone(),
            name: "Fixture".into(),
            description: None,
            skills: vec![],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: self.workspace_requirement,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        if self.fail_start {
            return Err(AgentError::Unavailable(
                "fixture".into(),
                "binary vanished".into(),
            ));
        }
        if let Some((name, content)) = self.write_file
            && let Some(workspace) = &request.workspace
        {
            std::fs::write(workspace.join(name), content).expect("write agent file");
            // Also modify a tracked file so the diff patch has content.
            std::fs::write(workspace.join("foo.txt"), "modified by agent\n")
                .expect("modify tracked");
        }
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let script = self.script.clone();
        let cancel = self.cancel.clone();
        let native_session_id = self.native_session_id.clone();
        tokio::spawn(async move {
            if let Some(session_id) = native_session_id {
                let _ = session_tx.send(Some(session_id));
            }
            for event in script {
                if cancel.load(Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                        .await;
                    return;
                }
                if tx.send(event).await.is_err() {
                    return;
                }
                // Pacing: gives cancel() a chance to take effect.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
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
        if !self.resume_ok {
            return Err(AgentError::Unsupported(
                "fixture does not support resume".into(),
            ));
        }
        // Echo the received native session id back through the watch channel
        // so tests can assert which session id the adapter was given.
        let session_id = native_session_id.to_string();
        let (session_tx, session_rx) = watch::channel(Some(session_id.clone()));
        let (tx, rx) = mpsc::channel(64);
        let script = self.script.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let _ = session_tx.send(Some(session_id));
            for event in script {
                if cancel.load(Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                        .await;
                    return;
                }
                if tx.send(event).await.is_err() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
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

fn registry_with(adapter: FixtureAdapter) -> Arc<AgentRegistry> {
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(adapter));
    Arc::new(registry)
}

struct TestContext {
    manager: TaskManager,
    db: Database,
    tasks: TaskRepository,
    artifacts: ArtifactRepository,
    workspaces: WorkspaceManager,
    _dir: tempfile::TempDir,
}

async fn test_context(registry: Arc<AgentRegistry>) -> TestContext {
    test_context_with_workspace_root(registry, None).await
}

async fn test_context_with_workspace_root(
    registry: Arc<AgentRegistry>,
    workspace_root: Option<std::path::PathBuf>,
) -> TestContext {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let db = Database::open(&path).await.expect("open");
    let tasks = TaskRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let ws_root = workspace_root.unwrap_or_else(|| dir.path().join("workspaces"));
    let workspaces = std::sync::Arc::new(WorkspaceManager::new(
        WorkspaceRepository::new(db.clone()),
        ws_root,
    ));
    let manager = TaskManager::new(
        registry,
        tasks.clone(),
        artifacts.clone(),
        contexts,
        sessions,
        workspaces.clone(),
    );
    TestContext {
        manager,
        db,
        tasks,
        artifacts,
        workspaces: (*workspaces).clone(),
        _dir: dir,
    }
}

fn isolated_fixture(script: Vec<AgentEvent>) -> FixtureAdapter {
    FixtureAdapter {
        id: "fixture".into(),
        script,
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::IsolatedGit,
        write_file: None,
    }
}

fn request(prompt: &str) -> AgentRunRequest {
    AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), AgentMessage::user(prompt))
}

async fn drain(run: &mut agentmesh_tasks::ManagedTaskRun) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    while let Some(event) = run.next_event().await {
        let done = matches!(
            event,
            AgentEvent::Completed
                | AgentEvent::Failed(_)
                | AgentEvent::StatusChanged(TaskStatus::Cancelled)
        );
        events.push(event);
        if done {
            break;
        }
    }
    events
}

#[tokio::test]
async fn successful_task_reaches_completed() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![
            AgentEvent::Started,
            AgentEvent::Message("hello".into()),
            AgentEvent::Completed,
        ],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let repo = TaskRepository::new(ctx.db.clone());

    let mut run = ctx
        .manager
        .start("fixture", request("hello"))
        .await
        .expect("start");
    let events = drain(&mut run).await;

    assert!(events.contains(&AgentEvent::Message("hello".into())));
    assert!(events.contains(&AgentEvent::Completed));

    let task = repo.get(run.task_id()).await.expect("get").expect("exists");
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.completed_at.is_some());
    assert!(task.started_at.is_some());
}

#[tokio::test]
async fn adapter_failure_persists_failed_task() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: true,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let repo = TaskRepository::new(ctx.db.clone());

    let err = ctx.manager.start("fixture", request("hello")).await;
    assert!(err.is_err(), "start must fail");

    let tasks = repo
        .list(&TaskFilter::default().limit(5))
        .await
        .expect("list");
    assert_eq!(tasks.len(), 1, "failed start must leave a task record");
    assert_eq!(tasks[0].status, TaskStatus::Failed);
    assert!(
        tasks[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("binary vanished")
    );
    assert!(tasks[0].completed_at.is_some());
}

#[tokio::test]
async fn artifact_events_are_persisted() {
    let artifact = Artifact::text("summary.md", "content");
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![
            AgentEvent::Started,
            AgentEvent::ArtifactUpdated(artifact),
            AgentEvent::Completed,
        ],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let artifacts = ArtifactRepository::new(ctx.db.clone());

    let mut run = ctx
        .manager
        .start("fixture", request("hi"))
        .await
        .expect("start");
    drain(&mut run).await;

    let stored = artifacts.list_by_task(run.task_id()).await.expect("list");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].name, "summary.md");
}

#[tokio::test]
async fn stream_events_still_reach_caller() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![
            AgentEvent::Started,
            AgentEvent::Message("m1".into()),
            AgentEvent::Message("m2".into()),
            AgentEvent::Completed,
        ],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;

    let mut run = ctx
        .manager
        .start("fixture", request("hi"))
        .await
        .expect("start");
    let events = drain(&mut run).await;

    assert!(events.contains(&AgentEvent::Message("m1".into())));
    assert!(events.contains(&AgentEvent::Message("m2".into())));
    assert!(events.contains(&AgentEvent::Completed));
}

#[tokio::test]
async fn agent_not_found_persists_failed_task() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let repo = TaskRepository::new(ctx.db.clone());

    let err = ctx.manager.start("ghost", request("hi")).await;
    assert!(matches!(
        err,
        Err(agentmesh_tasks::TaskError::AgentNotFound(_))
    ));

    let tasks = repo
        .list(&TaskFilter::default().limit(5))
        .await
        .expect("list");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].status, TaskStatus::Failed);
    assert!(
        tasks[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("not found")
    );
}

#[tokio::test]
async fn cancel_via_run_kills_and_persists_cancelled() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![
            AgentEvent::Started,
            AgentEvent::Message("working".into()),
            AgentEvent::Completed,
        ],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let repo = TaskRepository::new(ctx.db.clone());

    let run = ctx
        .manager
        .start("fixture", request("hi"))
        .await
        .expect("start");
    run.cancel().await.expect("cancel");

    let mut run = run;
    let events = drain(&mut run).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, AgentEvent::StatusChanged(TaskStatus::Cancelled))),
        "expected cancellation event, got {events:?}"
    );

    let task = repo.get(run.task_id()).await.expect("get").expect("exists");
    assert_eq!(task.status, TaskStatus::Cancelled);
}

fn fixture(script: Vec<AgentEvent>) -> FixtureAdapter {
    FixtureAdapter {
        id: "fixture".into(),
        script,
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: None,
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }
}

#[tokio::test]
async fn fresh_run_creates_context_session_and_task() {
    let ctx = test_context(registry_with(fixture(vec![
        AgentEvent::Started,
        AgentEvent::Message("hi".into()),
        AgentEvent::Completed,
    ])))
    .await;
    let repo = TaskRepository::new(ctx.db.clone());
    let contexts = ContextRepository::new(ctx.db.clone());
    let sessions = AgentSessionRepository::new(ctx.db.clone());

    let mut run = ctx
        .manager
        .start("fixture", request("hi"))
        .await
        .expect("start");
    drain(&mut run).await;

    let task = repo.get(run.task_id()).await.expect("get").expect("exists");
    let session_id = run.agent_session_id().expect("session id");
    assert_eq!(task.context_id, run.context_id());
    assert_eq!(task.agent_session_id, Some(session_id));

    let context = contexts
        .get(run.context_id())
        .await
        .expect("get")
        .expect("exists");
    let session = sessions
        .get(session_id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(session.context_id, context.id);
    assert_eq!(session.agent_id, "fixture");
}

#[tokio::test]
async fn native_session_id_is_persisted_before_completion() {
    let ctx = test_context(registry_with(FixtureAdapter {
        id: "fixture".into(),
        script: vec![AgentEvent::Started, AgentEvent::Completed],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: Some("session-123".into()),
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::None,
        write_file: None,
    }))
    .await;
    let sessions = AgentSessionRepository::new(ctx.db.clone());

    let mut run = ctx
        .manager
        .start("fixture", request("hi"))
        .await
        .expect("start");
    // Consume the first event so the adapter had time to publish the watch
    // value; then check the database BEFORE the run completes.
    let first = run.next_event().await;
    assert!(first.is_some());

    // Give the forwarder a moment to process the watch notification.
    for _ in 0..50 {
        let session_id = run.agent_session_id().expect("session id");
        let session = sessions
            .get(session_id)
            .await
            .expect("get")
            .expect("exists");
        if session.native_session_id.as_deref() == Some("session-123") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let session_id = run.agent_session_id().expect("session id");
    let session = sessions
        .get(session_id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        session.native_session_id.as_deref(),
        Some("session-123"),
        "native session must be persisted while the run is still active"
    );
    // The run must still be alive (not yet terminal).
    assert!(!run_done(&mut run).await);
}

async fn run_done(run: &mut agentmesh_tasks::ManagedTaskRun) -> bool {
    tokio::time::timeout(std::time::Duration::from_millis(30), run.next_event())
        .await
        .map(|opt| opt.is_none())
        .unwrap_or(false)
}

#[tokio::test]
async fn resume_from_database_reload_uses_persisted_session() {
    // First "process": run a task that captures native session id fake-123.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    let db = Database::open(&path).await.expect("open");
    let (source_task_id, source_context_id) = {
        let tasks = TaskRepository::new(db.clone());
        let artifacts = ArtifactRepository::new(db.clone());
        let contexts = ContextRepository::new(db.clone());
        let sessions = AgentSessionRepository::new(db.clone());
        let workspaces = std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("workspaces"),
        ));
        let manager = TaskManager::new(
            registry_with(FixtureAdapter {
                id: "fixture".into(),
                script: vec![AgentEvent::Started, AgentEvent::Completed],
                cancel: Arc::new(AtomicBool::new(false)),
                fail_start: false,
                native_session_id: Some("fake-123".into()),
                resume_ok: true,
                workspace_requirement: WorkspaceRequirement::None,
                write_file: None,
            }),
            tasks,
            artifacts,
            contexts,
            sessions,
            workspaces,
        );
        let mut run = manager
            .start("fixture", request("remember"))
            .await
            .expect("start");
        drain(&mut run).await;
        (run.task_id(), run.context_id())
    };

    // Second "process": brand new manager + repos over the same database.
    let db2 = Database::open(&path).await.expect("reopen");
    let tasks = TaskRepository::new(db2.clone());
    let artifacts = ArtifactRepository::new(db2.clone());
    let contexts = ContextRepository::new(db2.clone());
    let sessions = AgentSessionRepository::new(db2.clone());
    let manager2 = TaskManager::new(
        registry_with(fixture(vec![AgentEvent::Started, AgentEvent::Completed])),
        tasks.clone(),
        artifacts,
        contexts,
        sessions,
        std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db2.clone()),
            dir.path().join("workspaces"),
        )),
    );

    let mut run = manager2
        .resume(source_task_id, request("what did I say?"))
        .await
        .expect("resume");
    drain(&mut run).await;

    // Same context, same session, new task.
    assert_eq!(run.context_id(), source_context_id);
    let resumed_task = tasks
        .get(run.task_id())
        .await
        .expect("get")
        .expect("exists");
    let source_task = tasks
        .get(source_task_id)
        .await
        .expect("get")
        .expect("exists");
    assert_ne!(resumed_task.id, source_task.id);
    assert_eq!(resumed_task.agent_session_id, source_task.agent_session_id);
    assert_eq!(resumed_task.status, TaskStatus::Completed);
    assert_eq!(source_task.status, TaskStatus::Completed);
}

#[tokio::test]
async fn resume_restores_session_workspace() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let ws = tempfile::tempdir().expect("tempdir");

    let db = Database::open(&path).await.expect("open");
    let source_task_id = {
        let tasks = TaskRepository::new(db.clone());
        let artifacts = ArtifactRepository::new(db.clone());
        let contexts = ContextRepository::new(db.clone());
        let sessions = AgentSessionRepository::new(db.clone());
        let workspaces = std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("workspaces"),
        ));
        let manager = TaskManager::new(
            registry_with(FixtureAdapter {
                id: "fixture".into(),
                script: vec![AgentEvent::Started, AgentEvent::Completed],
                cancel: Arc::new(AtomicBool::new(false)),
                fail_start: false,
                native_session_id: Some("ws-session".into()),
                resume_ok: true,
                workspace_requirement: WorkspaceRequirement::None,
                write_file: None,
            }),
            tasks,
            artifacts,
            contexts,
            sessions,
            workspaces,
        );
        let mut request = request("first");
        request.workspace = Some(ws.path().to_path_buf());
        let mut run = manager.start("fixture", request).await.expect("start");
        drain(&mut run).await;
        run.task_id()
    };

    // New manager, and the resume request deliberately does NOT carry a
    // workspace: the session's workspace must win.
    let db2 = Database::open(&path).await.expect("reopen");
    let tasks = TaskRepository::new(db2.clone());
    let artifacts = ArtifactRepository::new(db2.clone());
    let contexts = ContextRepository::new(db2.clone());
    let sessions = AgentSessionRepository::new(db2.clone());
    let manager2 = TaskManager::new(
        registry_with(fixture(vec![AgentEvent::Started, AgentEvent::Completed])),
        tasks.clone(),
        artifacts,
        contexts,
        sessions,
        std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db2.clone()),
            dir.path().join("workspaces"),
        )),
    );

    let mut run = manager2
        .resume(source_task_id, request("again"))
        .await
        .expect("resume");
    drain(&mut run).await;
    let resumed = tasks
        .get(run.task_id())
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        resumed.workspace.as_deref(),
        Some(ws.path()),
        "resumed task must reuse the session workspace"
    );
}

#[tokio::test]
async fn resume_missing_workspace_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let ws = tempfile::tempdir().expect("tempdir");
    let ws_path = ws.path().to_path_buf();
    let ws_exists = ws_path.clone();

    let db = Database::open(&path).await.expect("open");
    let source_task_id = {
        let tasks = TaskRepository::new(db.clone());
        let artifacts = ArtifactRepository::new(db.clone());
        let contexts = ContextRepository::new(db.clone());
        let sessions = AgentSessionRepository::new(db.clone());
        let workspaces = std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("workspaces"),
        ));
        let manager = TaskManager::new(
            registry_with(FixtureAdapter {
                id: "fixture".into(),
                script: vec![AgentEvent::Started, AgentEvent::Completed],
                cancel: Arc::new(AtomicBool::new(false)),
                fail_start: false,
                native_session_id: Some("ws-session".into()),
                resume_ok: true,
                workspace_requirement: WorkspaceRequirement::None,
                write_file: None,
            }),
            tasks,
            artifacts,
            contexts,
            sessions,
            workspaces,
        );
        let mut request = request("first");
        request.workspace = Some(ws_path);
        let mut run = manager.start("fixture", request).await.expect("start");
        drain(&mut run).await;
        run.task_id()
    };
    // Delete the workspace directory.
    drop(ws);
    assert!(!ws_exists.exists());

    let db2 = Database::open(&path).await.expect("reopen");
    let manager2 = TaskManager::new(
        registry_with(fixture(vec![AgentEvent::Started, AgentEvent::Completed])),
        TaskRepository::new(db2.clone()),
        ArtifactRepository::new(db2.clone()),
        ContextRepository::new(db2.clone()),
        AgentSessionRepository::new(db2.clone()),
        std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db2.clone()),
            dir.path().join("workspaces"),
        )),
    );
    let err = manager2.resume(source_task_id, request("again")).await;
    assert!(
        matches!(
            err,
            Err(agentmesh_tasks::TaskError::WorkspaceUnavailable(_))
        ),
        "expected WorkspaceUnavailable"
    );
}

#[tokio::test]
async fn resume_legacy_task_without_session_fails_cleanly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let db = Database::open(&path).await.expect("open");

    // Insert a legacy task (Phase 4 style: no context/session links).
    let legacy_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO tasks (id, agent_id, status, prompt, created_at)
         VALUES (?, 'fixture', 'completed', 'legacy', '2026-08-01T00:00:00+00:00')",
    )
    .bind(legacy_id.to_string())
    .execute(db.pool())
    .await
    .expect("insert legacy task");

    let manager = TaskManager::new(
        registry_with(fixture(vec![])),
        TaskRepository::new(db.clone()),
        ArtifactRepository::new(db.clone()),
        ContextRepository::new(db.clone()),
        AgentSessionRepository::new(db.clone()),
        std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("workspaces"),
        )),
    );
    let err = manager.resume(legacy_id, request("hi")).await;
    assert!(
        matches!(
            err,
            Err(agentmesh_tasks::TaskError::NativeSessionUnavailable(_))
        ),
        "expected NativeSessionUnavailable"
    );
}

#[tokio::test]
async fn resume_unsupported_adapter_fails_new_task() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    let db = Database::open(&path).await.expect("open");
    let source_task_id = {
        let tasks = TaskRepository::new(db.clone());
        let artifacts = ArtifactRepository::new(db.clone());
        let contexts = ContextRepository::new(db.clone());
        let sessions = AgentSessionRepository::new(db.clone());
        let workspaces = std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("workspaces"),
        ));
        let manager = TaskManager::new(
            registry_with(FixtureAdapter {
                id: "fixture".into(),
                script: vec![AgentEvent::Started, AgentEvent::Completed],
                cancel: Arc::new(AtomicBool::new(false)),
                fail_start: false,
                native_session_id: Some("fake-123".into()),
                resume_ok: true,
                workspace_requirement: WorkspaceRequirement::None,
                write_file: None,
            }),
            tasks,
            artifacts,
            contexts,
            sessions,
            workspaces,
        );
        let mut run = manager
            .start("fixture", request("first"))
            .await
            .expect("start");
        drain(&mut run).await;
        run.task_id()
    };

    // Second process with an adapter that does NOT support resume.
    let db2 = Database::open(&path).await.expect("reopen");
    let tasks = TaskRepository::new(db2.clone());
    let manager2 = TaskManager::new(
        registry_with(FixtureAdapter {
            id: "fixture".into(),
            script: vec![],
            cancel: Arc::new(AtomicBool::new(false)),
            fail_start: false,
            native_session_id: None,
            resume_ok: false,
            workspace_requirement: WorkspaceRequirement::None,
            write_file: None,
        }),
        tasks.clone(),
        ArtifactRepository::new(db2.clone()),
        ContextRepository::new(db2.clone()),
        AgentSessionRepository::new(db2.clone()),
        std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db2.clone()),
            dir.path().join("workspaces"),
        )),
    );

    let err = manager2.resume(source_task_id, request("again")).await;
    assert!(err.is_err(), "resume must fail");

    // A new failed task must exist; the source task stays completed.
    let tasks_list = tasks
        .list(&TaskFilter::default().limit(5))
        .await
        .expect("list");
    assert_eq!(tasks_list.len(), 2);
    let newest = &tasks_list[0];
    assert_ne!(newest.id, source_task_id);
    assert_eq!(newest.status, TaskStatus::Failed);
    assert!(newest.error.as_deref().unwrap_or("").contains("resume"));
    let source = tasks
        .get(source_task_id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(source.status, TaskStatus::Completed);
}

// ---------- Phase 6: workspace isolation integration ----------

fn git(dir: &std::path::Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
}

/// Clean temp repo with one committed file.
fn clean_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    git(dir.path(), &["init", "-q"]);
    git(dir.path(), &["config", "user.name", "AgentMesh Test"]);
    git(
        dir.path(),
        &["config", "user.email", "agentmesh@example.invalid"],
    );
    std::fs::write(dir.path().join("foo.txt"), "base\n").expect("write");
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-q", "-m", "initial"]);
    dir
}

/// Fixture adapter that writes a file into its execution workspace, so tests
/// can verify which directory the adapter actually received.
fn writing_fixture(filename: &'static str, content: &'static str) -> FixtureAdapter {
    FixtureAdapter {
        id: "fixture".into(),
        script: vec![AgentEvent::Started, AgentEvent::Completed],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: Some("ws-native".into()),
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::IsolatedGit,
        write_file: Some((filename, content)),
    }
}

#[tokio::test]
async fn fresh_run_creates_isolated_worktree_and_source_stays_clean() {
    let repo = clean_repo();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let ctx = test_context_with_workspace_root(
        registry_with(writing_fixture("agent_file.txt", "agent content\n")),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    let mut request = request("create a file");
    request.workspace = Some(repo.path().to_path_buf());
    let mut run = ctx.manager.start("fixture", request).await.expect("start");
    drain(&mut run).await;

    // The adapter must have run inside an isolated worktree, not the repo.
    assert!(
        !repo.path().join("agent_file.txt").exists(),
        "source repository must stay clean"
    );
    let task = ctx
        .tasks
        .get(run.task_id())
        .await
        .expect("get")
        .expect("exists");
    let workspace_path = task.workspace.expect("workspace path");
    assert_ne!(workspace_path, repo.path());
    assert!(workspace_path.join("agent_file.txt").exists());

    // The workspace is registered for the session.
    let session_id = run.agent_session_id().expect("session");
    let workspace = ctx
        .workspaces
        .workspace_for_session(session_id)
        .await
        .expect("workspace");
    assert_eq!(workspace.path, workspace_path);
    assert!(workspace.branch.starts_with("agentmesh/fixture/"));
}

#[tokio::test]
async fn completed_isolated_task_generates_patch_artifact() {
    let repo = clean_repo();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let ctx = test_context_with_workspace_root(
        registry_with(writing_fixture("agent_file.txt", "agent content\n")),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    let mut request = request("create a file");
    request.workspace = Some(repo.path().to_path_buf());
    let mut run = ctx.manager.start("fixture", request).await.expect("start");
    drain(&mut run).await;

    let artifacts = ctx
        .artifacts
        .list_by_task(run.task_id())
        .await
        .expect("list");
    let patch = artifacts
        .iter()
        .find(|a| a.name == "changes.patch")
        .expect("patch artifact");
    assert_eq!(patch.kind, ArtifactKind::Patch);
    assert_eq!(patch.mime_type, "text/x-diff");
    // Tracked modification appears in the patch; untracked files are listed
    // as metadata only.
    assert!(patch.content_as_str().unwrap_or("").contains("foo.txt"));
    assert_eq!(
        patch.metadata.get("scope").map(String::as_str),
        Some("workspace")
    );
    assert!(
        patch
            .metadata
            .get("untracked_files")
            .map(String::as_str)
            .unwrap_or("")
            .contains("agent_file.txt"),
        "untracked files must be reported in metadata"
    );
}

#[tokio::test]
async fn no_changes_means_no_patch_artifact() {
    let repo = clean_repo();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let ctx = test_context_with_workspace_root(
        registry_with(isolated_fixture(vec![
            AgentEvent::Started,
            AgentEvent::Completed,
        ])),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    let mut request = request("do nothing");
    request.workspace = Some(repo.path().to_path_buf());
    let mut run = ctx.manager.start("fixture", request).await.expect("start");
    drain(&mut run).await;

    let artifacts = ctx
        .artifacts
        .list_by_task(run.task_id())
        .await
        .expect("list");
    assert!(
        artifacts.iter().all(|a| a.name != "changes.patch"),
        "empty workspace must not produce a patch artifact"
    );
}

#[tokio::test]
async fn resume_reuses_same_worktree_across_manager_instances() {
    let repo = clean_repo();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let ws_root = data_dir.path().join("worktrees");
    let db_path = data_dir.path().join("agentmesh.db");

    // First process: fresh run writes a file.
    let source_task_id = {
        let db = Database::open(&db_path).await.expect("open");
        let workspaces = std::sync::Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            ws_root.clone(),
        ));
        let manager = TaskManager::new(
            registry_with(writing_fixture("keep.txt", "from first run\n")),
            TaskRepository::new(db.clone()),
            ArtifactRepository::new(db.clone()),
            ContextRepository::new(db.clone()),
            AgentSessionRepository::new(db.clone()),
            workspaces,
        );
        let mut request = request("first");
        request.workspace = Some(repo.path().to_path_buf());
        let mut run = manager.start("fixture", request).await.expect("start");
        drain(&mut run).await;
        run.task_id()
    };

    // Second process: resume; the adapter must see the same worktree with the
    // file from the first run still present.
    let db2 = Database::open(&db_path).await.expect("reopen");
    let sessions = AgentSessionRepository::new(db2.clone());
    let workspaces = std::sync::Arc::new(WorkspaceManager::new(
        WorkspaceRepository::new(db2.clone()),
        ws_root,
    ));
    let manager2 = TaskManager::new(
        registry_with(FixtureAdapter {
            id: "fixture".into(),
            script: vec![AgentEvent::Started, AgentEvent::Completed],
            cancel: Arc::new(AtomicBool::new(false)),
            fail_start: false,
            native_session_id: Some("ws-native".into()),
            resume_ok: true,
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
            write_file: None,
        }),
        TaskRepository::new(db2.clone()),
        ArtifactRepository::new(db2.clone()),
        ContextRepository::new(db2.clone()),
        sessions.clone(),
        workspaces.clone(),
    );
    let mut run = manager2
        .resume(source_task_id, request("second"))
        .await
        .expect("resume");
    drain(&mut run).await;

    let session_id = run.agent_session_id().expect("session");
    let workspace = workspaces
        .workspace_for_session(session_id)
        .await
        .expect("workspace");
    assert!(
        workspace.path.join("keep.txt").exists(),
        "resume must continue in the same worktree with prior state"
    );
    let resumed_task = manager2
        .get_task(run.task_id())
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        resumed_task.workspace.as_deref(),
        Some(workspace.path.as_path())
    );
    let _ = sessions;
}

#[tokio::test]
async fn dirty_source_repo_fails_task_cleanly() {
    let repo = clean_repo();
    std::fs::write(repo.path().join("foo.txt"), "dirty\n").expect("dirty");
    let data_dir = tempfile::tempdir().expect("tempdir");
    let ctx = test_context_with_workspace_root(
        registry_with(isolated_fixture(vec![
            AgentEvent::Started,
            AgentEvent::Completed,
        ])),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    let mut request = request("do something");
    request.workspace = Some(repo.path().to_path_buf());
    let err = ctx.manager.start("fixture", request).await;
    assert!(err.is_err(), "dirty source must fail");

    let tasks = ctx
        .tasks
        .list(&TaskFilter::default().limit(5))
        .await
        .expect("list");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].status, TaskStatus::Failed);
    assert!(
        tasks[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("uncommitted")
    );
}

#[tokio::test]
async fn mock_without_git_repo_still_works() {
    // Mock requires no workspace; a non-git cwd must not fail.
    let dir = tempfile::tempdir().expect("tempdir");
    let ctx = test_context(registry_with(fixture(vec![
        AgentEvent::Started,
        AgentEvent::Message("hello".into()),
        AgentEvent::Completed,
    ])))
    .await;
    let mut request = request("hi");
    request.workspace = Some(dir.path().to_path_buf());
    let mut run = ctx.manager.start("fixture", request).await.expect("start");
    let events = drain(&mut run).await;
    assert!(events.contains(&AgentEvent::Completed));
}

// ---------- Phase 11: context workspace provisioning ----------

/// A `writing_fixture` variant with a configurable adapter id.
fn writing_fixture_named(
    id: &str,
    filename: &'static str,
    content: &'static str,
) -> FixtureAdapter {
    FixtureAdapter {
        id: id.to_string(),
        script: vec![AgentEvent::Started, AgentEvent::Completed],
        cancel: Arc::new(AtomicBool::new(false)),
        fail_start: false,
        native_session_id: Some(format!("ws-native-{id}")),
        resume_ok: true,
        workspace_requirement: WorkspaceRequirement::IsolatedGit,
        write_file: Some((filename, content)),
    }
}

#[tokio::test]
async fn new_agent_in_context_gets_its_own_worktree_from_the_same_repo() {
    let repo = clean_repo();
    let root = repo.path().canonicalize().expect("canonicalize repo");
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(writing_fixture_named(
        "alpha",
        "alpha.txt",
        "alpha content\n",
    )));
    registry.register(Box::new(writing_fixture_named(
        "beta",
        "beta.txt",
        "beta content\n",
    )));
    let ctx = test_context_with_workspace_root(
        Arc::new(registry),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    // Alpha starts the context (fresh worktree from the repo).
    let mut req_a = request("architect");
    req_a.workspace = Some(repo.path().to_path_buf());
    let mut run_a = ctx
        .manager
        .start("alpha", req_a)
        .await
        .expect("alpha start");
    let context_id = run_a.context_id();
    drain(&mut run_a).await;

    // Beta joins the SAME context, deriving the source repository from
    // alpha's workspace (its request carries no workspace at all). The
    // daemon's A2A backend resolves-or-creates the session first.
    ctx.manager
        .resolve_or_create_context_session(context_id, "beta")
        .await
        .expect("resolve beta session");
    let mut run_b = ctx
        .manager
        .start_in_context(context_id, "beta", request("implement"))
        .await
        .expect("beta joins context");
    let beta_session_id = run_b.agent_session_id().expect("beta session");
    drain(&mut run_b).await;

    let alpha_session_id = run_a.agent_session_id().expect("alpha session");
    assert_ne!(alpha_session_id, beta_session_id, "one session per agent");

    // Each agent has its own isolated worktree of the same repository.
    let ws_a = ctx
        .workspaces
        .workspace_for_session(alpha_session_id)
        .await
        .expect("alpha workspace");
    let ws_b = ctx
        .workspaces
        .workspace_for_session(beta_session_id)
        .await
        .expect("beta workspace");
    assert_ne!(ws_a.id, ws_b.id, "distinct workspace rows");
    assert_ne!(ws_a.path, ws_b.path, "agents never share a worktree");
    assert_eq!(
        ws_a.repository_root, root,
        "alpha rooted at the source repo"
    );
    assert_eq!(
        ws_b.repository_root, root,
        "beta rooted at the same source repo"
    );
    assert_ne!(
        ws_b.path, root,
        "beta must not run directly in the source repo"
    );

    // The beta adapter ran inside its own worktree, not the source repo.
    assert!(
        ws_b.path.join("beta.txt").exists(),
        "beta's worktree received the adapter's file"
    );
    assert!(
        !repo.path().join("beta.txt").exists(),
        "the source repository must stay clean"
    );
}

#[tokio::test]
async fn same_agent_in_context_reuses_session_and_worktree() {
    let repo = clean_repo();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut registry = AgentRegistry::default();
    registry.register(Box::new(writing_fixture_named(
        "alpha",
        "alpha.txt",
        "alpha content\n",
    )));
    let ctx = test_context_with_workspace_root(
        Arc::new(registry),
        Some(data_dir.path().join("worktrees")),
    )
    .await;

    let mut req_a = request("first");
    req_a.workspace = Some(repo.path().to_path_buf());
    let mut run_a = ctx.manager.start("alpha", req_a).await.expect("start");
    let context_id = run_a.context_id();
    let first_session = run_a.agent_session_id().expect("session");
    drain(&mut run_a).await;
    let ws_first = ctx
        .workspaces
        .workspace_for_session(first_session)
        .await
        .expect("first workspace");

    // The same agent continues in the same context: same session + worktree.
    let mut run_b = ctx
        .manager
        .start_in_context(context_id, "alpha", request("second"))
        .await
        .expect("alpha continues");
    let second_session = run_b.agent_session_id().expect("session");
    drain(&mut run_b).await;

    assert_eq!(
        first_session, second_session,
        "same agent reuses its session"
    );
    let ws_second = ctx
        .workspaces
        .workspace_for_session(second_session)
        .await
        .expect("second workspace");
    assert_eq!(ws_first.id, ws_second.id, "same worktree reused");
    assert_eq!(ws_first.path, ws_second.path);
}
