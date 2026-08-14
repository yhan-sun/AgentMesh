//! Daemon A2A context continuation: the real daemon backend behind A2A.
//!
//! Verifies the Phase 10 context invariant at the daemon layer:
//!
//! * step 1 (`alpha`) creates a context + its own AgentSession,
//! * step 2 (`beta`) joins the *same* context and gets its **own** session,
//! * step 3 (`alpha`) resumes the **same** session it had in step 1.
//!
//! 1 context, 2 sessions, 3 tasks.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use agentmesh_a2a::client::{A2AClient, A2AClientError, A2AClientEvent};
use agentmesh_a2a::server::A2AServerConfig;
use agentmesh_a2a::types::Message;
use agentmesh_adapters::{
    AgentError, AgentHealth, AgentRegistry, AgentRunHandle, AgentRunRequest, CodingAgentAdapter,
};
use agentmesh_apply::ApplyManager;
use agentmesh_core::{AgentDescriptor, AgentEvent, AgentSkill, WorkspaceRequirement};
use agentmesh_daemon::a2a_backend::DaemonA2ABackend;
use agentmesh_daemon::lease::SessionLeaseManager;
use agentmesh_daemon::registry::LiveTaskRegistry;
use agentmesh_daemon::server::DaemonState;
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowReplanRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

/// A minimal adapter that completes immediately, echoing a native session id.
struct QuickAdapter {
    id: String,
}

#[async_trait]
impl CodingAgentAdapter for QuickAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Quick"
    }
    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: self.id.clone(),
            name: format!("Quick {}", self.id),
            description: None,
            skills: vec![AgentSkill::new("code", None)],
            endpoint: format!("agent://{}", self.id),
            workspace_requirement: WorkspaceRequirement::None,
        }
    }
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(None, None))
    }
    async fn start(&self, _request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn(false).await
    }
    async fn resume(
        &self,
        _native_session_id: &str,
        _request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.spawn(true).await
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
}

impl QuickAdapter {
    async fn spawn(&self, resumed: bool) -> Result<AgentRunHandle, AgentError> {
        let (tx, rx) = mpsc::channel(64);
        let (session_tx, session_rx) = watch::channel(None);
        let id = self.id.clone();
        tokio::spawn(async move {
            let _ = session_tx.send(Some(format!("native-{id}")));
            let _ = tx.send(AgentEvent::Started).await;
            if resumed {
                let _ = tx.send(AgentEvent::Message(format!("resumed {id}"))).await;
            } else {
                let _ = tx.send(AgentEvent::Message(format!("done by {id}"))).await;
            }
            let _ = tx.send(AgentEvent::Completed).await;
        });
        Ok(AgentRunHandle::with_session_channel(
            Uuid::new_v4(),
            rx,
            session_rx,
        ))
    }
}

struct TestEnv {
    state: Arc<DaemonState>,
    token: String,
    _dir: tempfile::TempDir,
}

async fn test_env() -> TestEnv {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("db");

    let mut registry = AgentRegistry::default();
    registry.register(Box::new(QuickAdapter {
        id: "alpha".to_string(),
    }));
    registry.register(Box::new(QuickAdapter {
        id: "beta".to_string(),
    }));

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
    let token = "test-token-1234567890abcdef".to_string();
    let instance_id = Uuid::new_v4();
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
    let apply = Arc::new(ApplyManager::new(
        tasks.clone(),
        workspaces.clone(),
        workflows_repo.clone(),
        steps.clone(),
        applies.clone(),
    ));
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
    TestEnv {
        state,
        token,
        _dir: dir,
    }
}

/// Bind an A2A listener for one agent backed by the daemon.
async fn bind_listener(state: Arc<DaemonState>, agent_id: &str, token: &str) -> A2AClient {
    let backend = Arc::new(DaemonA2ABackend::new(state));
    let descriptor = AgentDescriptor {
        id: agent_id.to_string(),
        name: format!("Agent {agent_id}"),
        description: None,
        skills: vec![AgentSkill::new("code", None)],
        endpoint: format!("agent://{agent_id}"),
        workspace_requirement: WorkspaceRequirement::None,
    };
    let config = Arc::new(A2AServerConfig::new(
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
    A2AClient::new(format!("http://{addr}/")).with_token(token.to_string())
}

/// Consume a streaming event stream until a terminal state.
async fn drain_to_terminal(events: impl Stream<Item = Result<A2AClientEvent, A2AClientError>>) {
    let mut stream = Box::pin(events);
    while let Some(event) = stream.next().await {
        if let Ok(A2AClientEvent::Status(status)) = event
            && status.status.state.is_terminal()
        {
            break;
        }
    }
}

#[tokio::test]
async fn second_agent_joins_context_with_own_session_and_first_reuses() {
    let env = test_env().await;
    let alpha = bind_listener(env.state.clone(), "alpha", &env.token).await;
    let beta = bind_listener(env.state.clone(), "beta", &env.token).await;

    // Step 1: alpha starts a task (fresh context + its own session).
    let s1 = alpha
        .send_streaming_message(&Message::user_text("architect this"))
        .await
        .expect("alpha start");
    let context_id = s1.task.context_id.expect("context id");
    drain_to_terminal(s1.events).await;

    // Step 2: beta joins the same context -> new beta session, same context.
    let s2 = beta
        .send_streaming_message_in_context(context_id, &Message::user_text("implement it"))
        .await
        .expect("beta joins context");
    assert_eq!(
        s2.task.context_id,
        Some(context_id),
        "beta shares the context"
    );
    drain_to_terminal(s2.events).await;

    // Step 3: alpha resumes in the same context -> alpha session reused.
    let s3 = alpha
        .send_streaming_message_in_context(context_id, &Message::user_text("review it"))
        .await
        .expect("alpha resumes context");
    assert_eq!(
        s3.task.context_id,
        Some(context_id),
        "alpha stays in the context"
    );
    drain_to_terminal(s3.events).await;

    // Invariant: 1 context, 3 tasks, 2 sessions (one per agent), alpha's
    // session reused across its two tasks.
    let tasks = env
        .state
        .task_manager
        .list_tasks(&agentmesh_storage::TaskFilter::default().limit(100))
        .await
        .expect("list");
    assert_eq!(tasks.len(), 3);

    let contexts: HashSet<_> = tasks.iter().map(|t| t.context_id).collect();
    assert_eq!(contexts.len(), 1, "all tasks share one context");
    assert!(contexts.contains(&context_id));

    let alpha_sessions: HashSet<_> = tasks
        .iter()
        .filter(|t| t.agent_id == "alpha")
        .map(|t| t.agent_session_id.expect("session"))
        .collect();
    let beta_sessions: HashSet<_> = tasks
        .iter()
        .filter(|t| t.agent_id == "beta")
        .map(|t| t.agent_session_id.expect("session"))
        .collect();
    assert_eq!(alpha_sessions.len(), 1, "alpha reuses one session");
    assert_eq!(beta_sessions.len(), 1, "beta gets its own session");
    assert_ne!(
        alpha_sessions, beta_sessions,
        "agents never share a session"
    );
}
