//! Integration test for cross-agent context sharing and handoff.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{AgentDescriptor, AgentEvent, Artifact, WorkspaceRequirement};
use agentmesh_daemon::lease::SessionLeaseManager;
use agentmesh_daemon::registry::LiveTaskRegistry;
use agentmesh_daemon::server::{self, DaemonState};
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowRepository, WorkflowStepRepository,
    WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// Adapter that records the exact prompt received and emits scripted events.
struct RecordingAdapter {
    id: String,
    received_prompts: Arc<Mutex<Vec<String>>>,
    emit_artifact: bool,
}

impl RecordingAdapter {
    fn new(id: &str, emit_artifact: bool) -> (Self, Arc<Mutex<Vec<String>>>) {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                id: id.to_string(),
                received_prompts: prompts.clone(),
                emit_artifact,
            },
            prompts,
        )
    }
}

#[async_trait]
impl CodingAgentAdapter for RecordingAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Recording"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: self.id.clone(),
            name: format!("Recording {}", self.id),
            description: None,
            skills: vec![],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: WorkspaceRequirement::None,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(Some("1.0.0".into()), None))
    }
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.received_prompts
            .lock()
            .unwrap()
            .push(request.input.content.clone());
        let run_id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(16);
        let (session_tx, session_rx) = watch::channel(None);
        let emit_artifact = self.emit_artifact;
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{}", Uuid::new_v4())));
            let _ = tx.send(AgentEvent::Started).await;
            if emit_artifact {
                let art = Artifact::text("spec.json", "{\"service\":\"auth\"}");
                let _ = tx.send(AgentEvent::ArtifactUpdated(art)).await;
            }
            let _ = tx.send(AgentEvent::Message("Finished step".into())).await;
            let _ = tx.send(AgentEvent::Completed).await;
        });
        Ok(AgentRunHandle::with_session_channel(run_id, rx, session_rx))
    }
    async fn resume(
        &self,
        native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.received_prompts
            .lock()
            .unwrap()
            .push(request.input.content.clone());
        let run_id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(16);
        let (_session_tx, session_rx) = watch::channel(Some(native_session_id.to_string()));
        tokio::spawn(async move {
            let _ = tx.send(AgentEvent::Started).await;
            let _ = tx.send(AgentEvent::Completed).await;
        });
        Ok(AgentRunHandle::with_session_channel(run_id, rx, session_rx))
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
}

struct TestServer {
    client: agentmesh_daemon::DaemonClient,
    _codex_prompts: Arc<Mutex<Vec<String>>>,
    claude_prompts: Arc<Mutex<Vec<String>>>,
    _dir: tempfile::TempDir,
}

async fn spawn_test_server() -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("test.db"))
        .await
        .expect("db open");
    let tasks = TaskRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let contexts = ContextRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(db.clone()),
    ));

    let (codex, codex_prompts) = RecordingAdapter::new("codex", true);
    let (claude, claude_prompts) = RecordingAdapter::new("claude", false);

    let mut registry = AgentRegistry::default();
    registry.register(Box::new(codex));
    registry.register(Box::new(claude));

    let task_manager = TaskManager::new(
        Arc::new(registry),
        tasks.clone(),
        artifacts.clone(),
        contexts,
        sessions.clone(),
        workspaces.clone(),
    );

    let instance_id = Uuid::new_v4();
    let workflows_repo = WorkflowRepository::new(db.clone());
    let steps_repo = WorkflowStepRepository::new(db.clone());
    let applies_repo = ApplyRepository::new(db.clone());
    let competitions_repo = agentmesh_storage::CompetitionRepository::new(db.clone());

    let apply = Arc::new(
        ApplyManager::new(
            tasks.clone(),
            workspaces.clone(),
            workflows_repo.clone(),
            steps_repo.clone(),
            applies_repo.clone(),
        )
        .with_competitions(competitions_repo.clone()),
    );

    let workflows = agentmesh_daemon::workflow_service::WorkflowService::new(
        instance_id,
        task_manager.clone(),
        workflows_repo.clone(),
        steps_repo.clone(),
        WorkflowPlanRepository::new(db.clone()),
        agentmesh_storage::WorkflowReplanRepository::new(db.clone()),
        agentmesh_storage::EvaluationRepository::new(db.clone()),
        competitions_repo.clone(),
        workspaces.clone(),
        agentmesh_orchestrator::router::RuleRouter::new(agentmesh_core::RoutingConfig::default()),
    );

    let token = "test-token-cross-context".to_string();
    let state = Arc::new(DaemonState {
        instance_id,
        token: token.clone(),
        task_manager,
        registry: LiveTaskRegistry::new(),
        leases: Arc::new(SessionLeaseManager::new()),
        scope: agentmesh_daemon::Scope::User,
        started_at: chrono::Utc::now(),
        shutdown: Arc::new(Notify::new()),
        shutting_down: AtomicBool::new(false),
        task_repo: tasks,
        workflows: workflows.clone(),
        plans: agentmesh_daemon::planner::PlanService::new(
            workflows.clone(),
            WorkflowPlanRepository::new(db.clone()),
        ),
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
        applies: applies_repo,
        workflows_repo,
        steps: steps_repo,
        competitions: competitions_repo,
        artifacts,
        a2a_agents: Mutex::new(serde_json::json!({})),
        provenance: Arc::new(
            agentmesh_daemon::provenance_service::ProvenanceService::from_db(db.clone()),
        ),
        provenance_repo: agentmesh_storage::ProvenanceRepository::new(db.clone()),
    });

    let (addr, router, listener) = server::bind(state.clone()).await.expect("bind");
    tokio::spawn(server::serve(listener, router, state.shutdown.clone()));

    let client = agentmesh_daemon::DaemonClient::new(
        &agentmesh_daemon::protocol::DaemonMeta {
            protocol_version: agentmesh_daemon::protocol::DAEMON_PROTOCOL_VERSION,
            instance_id: instance_id.to_string(),
            pid: 0,
            address: addr.to_string(),
            started_at: chrono::Utc::now().to_rfc3339(),
        },
        token,
    );

    TestServer {
        client,
        _codex_prompts: codex_prompts,
        claude_prompts,
        _dir: dir,
    }
}

#[tokio::test]
async fn test_cross_agent_context_handoff_from_task() {
    let server = spawn_test_server().await;

    // 1. Run task on codex (GPT)
    let run1 = server
        .client
        .run("codex", "Please design the authentication API", None)
        .await
        .expect("run codex");

    // Wait for task 1 to finish
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 2. Run task on claude with from_task_id inheriting from codex
    let run2 = server
        .client
        .run_with_options(
            "claude",
            "Please implement the client for the designed API",
            None,
            Some(run1.task_id),
            None,
        )
        .await
        .expect("run claude with from_task");

    // Wait for task 2 to finish
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Verify Claude inherited the same context_id
    assert_eq!(run2.context_id, run1.context_id);

    // Verify Claude's received prompt contains the structured <prior_agent_context> block
    let claude_prompts = server.claude_prompts.lock().unwrap().clone();
    assert_eq!(claude_prompts.len(), 1);
    let claude_prompt = &claude_prompts[0];

    assert!(claude_prompt.contains("<prior_agent_context>"));
    assert!(claude_prompt.contains("codex"));
    assert!(claude_prompt.contains("Please design the authentication API"));
    assert!(claude_prompt.contains("spec.json"));
    assert!(claude_prompt.contains("{\"service\":\"auth\"}"));
    assert!(claude_prompt.contains("Please implement the client for the designed API"));
}

#[tokio::test]
async fn test_cross_agent_context_handoff_from_context() {
    let server = spawn_test_server().await;

    // 1. Run task on codex
    let run1 = server
        .client
        .run("codex", "Step 1: Architecture review", None)
        .await
        .expect("run codex");

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 2. Run second task on claude inheriting from the entire context
    let run2 = server
        .client
        .run_with_options(
            "claude",
            "Step 2: Implementation",
            None,
            None,
            Some(run1.context_id),
        )
        .await
        .expect("run claude with from_context");

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    assert_eq!(run2.context_id, run1.context_id);
    let claude_prompts = server.claude_prompts.lock().unwrap().clone();
    assert_eq!(claude_prompts.len(), 1);
    assert!(claude_prompts[0].contains("Step 1: Architecture review"));
    assert!(claude_prompts[0].contains("Step 2: Implementation"));
}
