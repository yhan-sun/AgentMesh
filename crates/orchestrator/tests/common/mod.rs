//! Shared test harness for orchestrator delegation and workflow tests:
//! controllable A2A agents with broadcast-based subscribe (mirrors the
//! daemon's live registry).
//!
//! Each test binary (`delegate`, `workflow`) only uses a subset of the
//! helpers, so dead-code warnings are expected and suppressed here.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use agentmesh_a2a::{A2ABackend, A2ABackendError, A2ARun, A2AServerConfig, A2AStreamEvent};
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentMessage, AgentSkill, AgentTask, Artifact, TaskStatus,
    WorkspaceRequirement,
};
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{RwLock, broadcast, mpsc};
use uuid::Uuid;

struct LiveTask {
    tx: broadcast::Sender<(u64, A2AStreamEvent)>,
    state: RwLock<Option<TaskStatus>>,
}

/// Scripted backend: emits a script of agent events for every started task.
#[derive(Clone, Default)]
pub struct ScriptedBackend {
    tasks: Arc<RwLock<HashMap<Uuid, Arc<LiveTask>>>>,
    next_seq: Arc<AtomicU64>,
    script: Arc<Vec<AgentEvent>>,
    step: std::time::Duration,
}

impl ScriptedBackend {
    pub fn new(script: Vec<AgentEvent>) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            next_seq: Arc::new(AtomicU64::new(1)),
            script: Arc::new(script),
            step: std::time::Duration::from_millis(10),
        }
    }

    pub fn with_step(mut self, step: std::time::Duration) -> Self {
        self.step = step;
        self
    }

    async fn spawn(&self, agent_id: &str) -> A2ARun {
        let task_id = Uuid::new_v4();
        let context_id = Uuid::new_v4();
        let (tx, _) = broadcast::channel(256);
        let live = Arc::new(LiveTask {
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
        let step = self.step;
        let seq = self.next_seq.clone();
        tokio::spawn(async move {
            let _ = live.tx.send((
                seq.fetch_add(1, Ordering::Relaxed),
                A2AStreamEvent::Agent(AgentEvent::Started),
            ));
            for event in script.iter() {
                tokio::time::sleep(step).await;
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
impl A2ABackend for ScriptedBackend {
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
        let Some(live) = self.tasks.read().await.get(&task_id).cloned() else {
            return Ok(None);
        };
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

/// A running mock A2A agent with its own listener and card.
pub struct MockAgent {
    pub agent_id: String,
    pub url: String,
    pub card_url: String,
    pub token: String,
}

/// Start a mock A2A agent server exposing one agent with the given skills.
pub async fn mock_agent(agent_id: &str, skills: &[&str], backend: ScriptedBackend) -> MockAgent {
    let token = "a2a-test-token-1234".to_string();
    let descriptor = AgentDescriptor {
        id: agent_id.to_string(),
        name: format!("Mock {agent_id}"),
        description: Some("controllable A2A test agent".into()),
        skills: skills
            .iter()
            .map(|skill| AgentSkill::new(*skill, None))
            .collect(),
        endpoint: format!("agent://{agent_id}"),
        workspace_requirement: WorkspaceRequirement::None,
    };
    let config = Arc::new(A2AServerConfig::new(
        agent_id.to_string(),
        descriptor,
        token.clone(),
        Arc::new(backend),
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    MockAgent {
        agent_id: agent_id.to_string(),
        url: format!("http://{addr}/"),
        card_url: format!("http://{addr}/.well-known/agent-card.json"),
        token,
    }
}

// ---------- Phase 10: context-aware workflow mock backend ----------

/// A task recorded by the workflow backend: what was actually delivered to an
/// A2A agent (mirrors the daemon's persisted task + session mapping).
#[derive(Debug, Clone)]
pub struct RecordedTask {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub agent_session_id: Option<Uuid>,
    pub agent_id: String,
    pub prompt: String,
}

struct WorkflowLiveTask {
    tx: broadcast::Sender<(u64, A2AStreamEvent)>,
    run_tx: mpsc::Sender<A2AStreamEvent>,
    state: StdRwLock<Option<TaskStatus>>,
    artifacts: StdRwLock<Vec<Artifact>>,
}

/// Controllable, context-aware A2A backend used by workflow tests.
///
/// Mirrors the daemon semantics Phase 10 relies on:
/// * one context may hold several agent sessions (`UNIQUE(context, agent)`),
/// * the same agent reuses its session inside a context,
/// * a busy session rejects new tasks with [`A2ABackendError::SessionBusy`].
///
/// Every started task is recorded (prompt + context + session), so tests can
/// assert the exact prompts and the context/session invariant.
#[derive(Clone)]
pub struct WorkflowBackend {
    tasks: Arc<StdRwLock<HashMap<Uuid, Arc<WorkflowLiveTask>>>>,
    recordings: Arc<StdRwLock<Vec<RecordedTask>>>,
    sessions: Arc<StdRwLock<HashMap<(Uuid, String), Uuid>>>,
    scripts: Arc<StdRwLock<HashMap<String, VecDeque<Vec<AgentEvent>>>>>,
    busy_agents: Arc<StdRwLock<HashSet<String>>>,
    next_seq: Arc<AtomicU64>,
    step: std::time::Duration,
}

impl WorkflowBackend {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(StdRwLock::new(HashMap::new())),
            recordings: Arc::new(StdRwLock::new(Vec::new())),
            sessions: Arc::new(StdRwLock::new(HashMap::new())),
            scripts: Arc::new(StdRwLock::new(HashMap::new())),
            busy_agents: Arc::new(StdRwLock::new(HashSet::new())),
            next_seq: Arc::new(AtomicU64::new(1)),
            step: std::time::Duration::from_millis(5),
        }
    }

    /// Push the next script an agent runs on its next task (FIFO per agent).
    pub fn push_script(&self, agent_id: &str, script: Vec<AgentEvent>) {
        self.scripts
            .write()
            .unwrap()
            .entry(agent_id.to_string())
            .or_default()
            .push_back(script);
    }

    /// Simulate a busy session for an agent (its next task is rejected).
    pub fn mark_busy(&self, agent_id: &str) {
        self.busy_agents
            .write()
            .unwrap()
            .insert(agent_id.to_string());
    }

    /// Every task delivered to an A2A agent, in order.
    pub fn recordings(&self) -> Vec<RecordedTask> {
        self.recordings.read().unwrap().clone()
    }

    /// Distinct sessions created, as (context, agent) pairs.
    pub fn sessions(&self) -> Vec<(Uuid, String)> {
        self.sessions
            .read()
            .unwrap()
            .iter()
            .map(|((context, agent), _)| (*context, agent.clone()))
            .collect()
    }

    /// The session id bound to a (context, agent) pair.
    pub fn session_id(&self, context_id: Uuid, agent_id: &str) -> Option<Uuid> {
        self.sessions
            .read()
            .unwrap()
            .get(&(context_id, agent_id.to_string()))
            .copied()
    }

    /// The recorded task for a task id.
    pub fn recording(&self, task_id: Uuid) -> Option<RecordedTask> {
        self.recordings
            .read()
            .unwrap()
            .iter()
            .find(|r| r.task_id == task_id)
            .cloned()
    }

    fn create_session(&self, context_id: Uuid, agent_id: &str) -> Uuid {
        let session_id = Uuid::new_v4();
        self.sessions
            .write()
            .unwrap()
            .insert((context_id, agent_id.to_string()), session_id);
        session_id
    }

    fn resolve_or_create_session(&self, context_id: Uuid, agent_id: &str) -> Uuid {
        let key = (context_id, agent_id.to_string());
        if let Some(session_id) = self.sessions.read().unwrap().get(&key) {
            return *session_id;
        }
        self.create_session(context_id, agent_id)
    }

    async fn spawn(
        &self,
        agent_id: &str,
        prompt: &str,
        context_id: Uuid,
        agent_session_id: Uuid,
    ) -> A2ARun {
        let task_id = Uuid::new_v4();
        self.recordings.write().unwrap().push(RecordedTask {
            task_id,
            context_id,
            agent_session_id: Some(agent_session_id),
            agent_id: agent_id.to_string(),
            prompt: prompt.to_string(),
        });

        let script = self
            .scripts
            .write()
            .unwrap()
            .get_mut(agent_id)
            .and_then(|queue| queue.pop_front());

        let (tx, _) = broadcast::channel(256);
        let (run_tx, run_rx) = mpsc::channel(256);
        let live = Arc::new(WorkflowLiveTask {
            tx,
            run_tx: run_tx.clone(),
            state: StdRwLock::new(Some(TaskStatus::Submitted)),
            artifacts: StdRwLock::new(Vec::new()),
        });
        self.tasks.write().unwrap().insert(task_id, live.clone());

        let _ = run_tx
            .send(A2AStreamEvent::TaskInfo {
                task_id,
                context_id,
                agent_session_id: Some(agent_session_id),
                agent_id: agent_id.to_string(),
            })
            .await;

        if let Some(script) = script {
            let step = self.step;
            let seq = self.next_seq.clone();
            let live_for_task = live.clone();
            tokio::spawn(async move {
                let _ = live_for_task.tx.send((
                    seq.fetch_add(1, Ordering::Relaxed),
                    A2AStreamEvent::Agent(AgentEvent::Started),
                ));
                for event in script.iter() {
                    tokio::time::sleep(step).await;
                    let _ = run_tx.send(A2AStreamEvent::Agent(event.clone())).await;
                    let _ = live_for_task.tx.send((
                        seq.fetch_add(1, Ordering::Relaxed),
                        A2AStreamEvent::Agent(event.clone()),
                    ));
                    if let AgentEvent::ArtifactUpdated(artifact) = event {
                        live_for_task
                            .artifacts
                            .write()
                            .unwrap()
                            .push(artifact.clone());
                    }
                    if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                        let state = match event {
                            AgentEvent::Completed => TaskStatus::Completed,
                            _ => TaskStatus::Failed,
                        };
                        *live_for_task.state.write().unwrap() = Some(state);
                    }
                }
            });
        }

        A2ARun {
            task_id,
            context_id,
            agent_session_id: Some(agent_session_id),
            agent_id: agent_id.to_string(),
            events: run_rx,
        }
    }
}

#[async_trait]
impl A2ABackend for WorkflowBackend {
    async fn start(
        &self,
        agent_id: &str,
        prompt: &str,
        _workspace: Option<std::path::PathBuf>,
    ) -> Result<A2ARun, A2ABackendError> {
        let context_id = Uuid::new_v4();
        let session_id = self.create_session(context_id, agent_id);
        Ok(self.spawn(agent_id, prompt, context_id, session_id).await)
    }

    async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
    ) -> Result<A2ARun, A2ABackendError> {
        let session_id = self.resolve_or_create_session(context_id, agent_id);
        if self.busy_agents.read().unwrap().contains(agent_id) {
            return Err(A2ABackendError::SessionBusy);
        }
        Ok(self.spawn(agent_id, prompt, context_id, session_id).await)
    }

    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        let Some(live) = self.tasks.read().unwrap().get(&task_id).cloned() else {
            return Ok(None);
        };
        let recording = self.recording(task_id);
        let state = live.state.read().unwrap().unwrap_or(TaskStatus::Failed);
        let mut task = AgentTask::new(
            recording
                .as_ref()
                .map(|r| r.agent_id.clone())
                .unwrap_or_else(|| "mock".to_string()),
            AgentMessage::user(recording.map(|r| r.prompt).unwrap_or_default()),
        );
        task.id = task_id;
        if let Some(recording) = self.recording(task_id) {
            task.context_id = recording.context_id;
        }
        task.status = state;
        Ok(Some((task, live.artifacts.read().unwrap().clone())))
    }

    async fn list_tasks(
        &self,
        _context_id: Option<Uuid>,
        _status: Option<TaskStatus>,
        _limit: usize,
    ) -> Result<Vec<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        let mut out = Vec::new();
        for recording in self.recordings() {
            if let Some((task, artifacts)) = self.get_task(recording.task_id).await? {
                out.push((task, artifacts));
            }
        }
        Ok(out)
    }

    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError> {
        let live = self
            .tasks
            .read()
            .unwrap()
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotFound(task_id))?;
        if live
            .state
            .read()
            .unwrap()
            .map(|s| s.is_terminal())
            .unwrap_or(true)
        {
            return Ok(());
        }
        *live.state.write().unwrap() = Some(TaskStatus::Cancelled);
        let event = A2AStreamEvent::Agent(AgentEvent::StatusChanged(TaskStatus::Cancelled));
        let _ = live
            .tx
            .send((self.next_seq.fetch_add(1, Ordering::Relaxed), event.clone()));
        let _ = live.run_tx.send(event).await;
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
            .unwrap()
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotLive)?;
        if live
            .state
            .read()
            .unwrap()
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

/// Start a mock A2A agent backed by a shared [`WorkflowBackend`].
pub async fn workflow_agent(
    agent_id: &str,
    skills: &[&str],
    backend: Arc<WorkflowBackend>,
) -> MockAgent {
    let token = "a2a-test-token-1234".to_string();
    let descriptor = AgentDescriptor {
        id: agent_id.to_string(),
        name: format!("Mock {agent_id}"),
        description: Some("controllable workflow A2A test agent".into()),
        skills: skills
            .iter()
            .map(|skill| AgentSkill::new(*skill, None))
            .collect(),
        endpoint: format!("agent://{agent_id}"),
        workspace_requirement: WorkspaceRequirement::None,
    };
    let config = Arc::new(A2AServerConfig::new(
        agent_id.to_string(),
        descriptor,
        token.clone(),
        backend,
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    MockAgent {
        agent_id: agent_id.to_string(),
        url: format!("http://{addr}/"),
        card_url: format!("http://{addr}/.well-known/agent-card.json"),
        token,
    }
}
