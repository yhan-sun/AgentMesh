//! Shared test harness: a controllable A2A server with broadcast-based
//! subscribe, mirroring the daemon's live-task registry.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use agentmesh_a2a::{A2ABackend, A2ABackendError, A2ARun, A2AServerConfig, A2AStreamEvent};
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentMessage, AgentSkill, AgentTask, Artifact, TaskStatus,
    WorkspaceRequirement,
};
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

/// One running task in the mock: broadcast channel + terminal state.
struct LiveMockTask {
    tx: broadcast::Sender<(u64, A2AStreamEvent)>,
    state: RwLock<Option<TaskStatus>>,
}

/// Backend that runs a scripted event list, publishing to a per-task
/// broadcast so `SubscribeToTask` behaves like the real daemon.
#[derive(Clone, Default)]
pub struct LiveScriptBackend {
    tasks: Arc<RwLock<HashMap<Uuid, Arc<LiveMockTask>>>>,
    next_seq: Arc<AtomicU64>,
    script: Arc<Vec<AgentEvent>>,
    delay: Duration,
}

impl LiveScriptBackend {
    pub fn new(script: Vec<AgentEvent>) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            next_seq: Arc::new(AtomicU64::new(1)),
            script: Arc::new(script),
            delay: Duration::from_millis(20),
        }
    }

    /// Pace the scripted events apart (for reconnect tests).
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    async fn spawn(&self, agent_id: &str) -> A2ARun {
        let task_id = Uuid::new_v4();
        let context_id = Uuid::new_v4();
        let (tx, _) = broadcast::channel(256);
        let live = Arc::new(LiveMockTask {
            tx,
            state: RwLock::new(Some(TaskStatus::Submitted)),
        });
        self.tasks.write().await.insert(task_id, live.clone());
        let (run_tx, run_rx) = tokio::sync::mpsc::channel(256);
        let _ = run_tx
            .send(A2AStreamEvent::TaskInfo {
                task_id,
                context_id,
                agent_session_id: None,
                agent_id: agent_id.to_string(),
            })
            .await;
        let script = self.script.clone();
        let delay = self.delay;
        let seq = self.next_seq.clone();
        tokio::spawn(async move {
            let _ = run_tx
                .send(A2AStreamEvent::Agent(AgentEvent::Started))
                .await;
            let _ = live.tx.send((
                seq.fetch_add(1, Ordering::Relaxed),
                A2AStreamEvent::Agent(AgentEvent::Started),
            ));
            for event in script.iter() {
                tokio::time::sleep(delay).await;
                let _ = run_tx.send(A2AStreamEvent::Agent(event.clone())).await;
                let _ = live.tx.send((
                    seq.fetch_add(1, Ordering::Relaxed),
                    A2AStreamEvent::Agent(event.clone()),
                ));
                if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                    let state = match event {
                        AgentEvent::Completed => TaskStatus::Completed,
                        _ => TaskStatus::Failed,
                    };
                    *live.state.write().await = Some(state);
                }
            }
        });
        A2ARun {
            task_id,
            context_id,
            agent_session_id: None,
            agent_id: agent_id.to_string(),
            events: run_rx,
        }
    }
}

#[async_trait]
impl A2ABackend for LiveScriptBackend {
    async fn start(
        &self,
        agent_id: &str,
        _prompt: &str,
        _workspace: Option<std::path::PathBuf>,
    ) -> Result<A2ARun, A2ABackendError> {
        Ok(self.spawn(agent_id).await)
    }

    async fn start_in_context(
        &self,
        _context_id: Uuid,
        _agent_id: &str,
        _prompt: &str,
    ) -> Result<A2ARun, A2ABackendError> {
        Err(A2ABackendError::Unsupported)
    }

    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        let live = self.tasks.read().await.get(&task_id).cloned();
        let Some(live) = live else { return Ok(None) };
        let state = live.state.read().await.unwrap_or(TaskStatus::Failed);
        let mut task = AgentTask::new("mock", AgentMessage::user("hi"));
        task.id = task_id;
        task.status = state;
        Ok(Some((task, Vec::new())))
    }

    async fn list_tasks(
        &self,
        _context_id: Option<Uuid>,
        _status: Option<TaskStatus>,
        _limit: usize,
    ) -> Result<Vec<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        Ok(Vec::new())
    }

    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError> {
        let live = self
            .tasks
            .read()
            .await
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotFound(task_id))?;
        if live
            .state
            .read()
            .await
            .map(|s| s.is_terminal())
            .unwrap_or(true)
        {
            return Ok(());
        }
        *live.state.write().await = Some(TaskStatus::Cancelled);
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let _ = live.tx.send((
            seq,
            A2AStreamEvent::Agent(AgentEvent::StatusChanged(TaskStatus::Cancelled)),
        ));
        Ok(())
    }

    async fn subscribe(
        &self,
        task_id: Uuid,
        _after: u64,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>>, A2ABackendError> {
        let live = self
            .tasks
            .read()
            .await
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotLive)?;
        if live
            .state
            .read()
            .await
            .map(|s| s.is_terminal())
            .unwrap_or(true)
        {
            return Err(A2ABackendError::TaskNotLive);
        }
        let mut receiver = live.tx.subscribe();
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok((_seq, event)) => yield event,
                    Err(_) => return,
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

/// A running mock A2A server.
pub struct MockServer {
    pub addr: std::net::SocketAddr,
    pub token: String,
}

/// Start a mock A2A server exposing one agent with the given skills.
pub async fn mock_server(skills: &[&str], backend: LiveScriptBackend) -> MockServer {
    let token = "a2a-test-token-1234".to_string();
    let descriptor = AgentDescriptor {
        id: "mock".into(),
        name: "Mock Agent".into(),
        description: Some("controllable A2A test agent".into()),
        skills: skills
            .iter()
            .map(|skill| AgentSkill::new(*skill, None))
            .collect(),
        endpoint: "agent://mock".into(),
        workspace_requirement: WorkspaceRequirement::None,
    };
    let config = Arc::new(A2AServerConfig::new(
        "mock".into(),
        descriptor,
        token.clone(),
        Arc::new(backend),
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    MockServer { addr, token }
}

/// Serve a handcrafted agent card (for untrusted-input parsing tests).
pub async fn card_server(card: serde_json::Value) -> std::net::SocketAddr {
    use axum::Router;
    use axum::extract::State;
    use axum::routing::get;
    #[derive(Clone)]
    struct Card(serde_json::Value);
    async fn serve_card(State(card): State<Card>) -> axum::Json<serde_json::Value> {
        axum::Json(card.0)
    }
    let app = Router::new()
        .route("/.well-known/agent-card.json", get(serve_card))
        .with_state(Card(card));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    addr
}
