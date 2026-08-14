//! A2A backend abstraction: the daemon implements this against the existing
//! AgentMesh runtime (TaskManager / LiveTaskRegistry / leases).

use std::path::PathBuf;

use agentmesh_core::{AgentEvent, AgentTask, Artifact, TaskStatus};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use uuid::Uuid;

/// Vendor-neutral stream events the A2A server consumes.
#[derive(Debug, Clone)]
pub enum A2AStreamEvent {
    TaskInfo {
        task_id: Uuid,
        context_id: Uuid,
        agent_session_id: Option<Uuid>,
        agent_id: String,
    },
    Agent(AgentEvent),
    ReplayGap {
        oldest_available: u64,
    },
}

/// Result of starting a task through the backend.
pub struct A2ARun {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub agent_session_id: Option<Uuid>,
    pub agent_id: String,
    /// Live events; the server consumes these for streaming responses.
    pub events: tokio::sync::mpsc::Receiver<A2AStreamEvent>,
}

/// Errors surfaced by the backend to the A2A layer (sanitized).
#[derive(Debug, Clone, thiserror::Error)]
pub enum A2ABackendError {
    #[error("agent `{0}` not found")]
    AgentNotFound(String),

    #[error("task `{0}` not found")]
    TaskNotFound(Uuid),

    #[error("agent session is busy with another task")]
    SessionBusy,

    #[error("no session for agent in context")]
    SessionForAgentNotFound,

    #[error("task is not live in the current daemon runtime")]
    TaskNotLive,

    #[error("operation is not supported")]
    Unsupported,

    #[error("internal error: {0}")]
    Internal(String),
}

/// Backend contract implemented by the daemon.
#[async_trait]
pub trait A2ABackend: Send + Sync {
    /// Fresh task on `agent_id` (new context).
    async fn start(
        &self,
        agent_id: &str,
        prompt: &str,
        workspace: Option<PathBuf>,
    ) -> Result<A2ARun, A2ABackendError>;

    /// Task in an existing context, continuing the agent's session there.
    async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
    ) -> Result<A2ARun, A2ABackendError>;

    /// Task in an existing context and specific session lane (Phase 23).
    async fn start_in_context_with_lane(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
        session_lane: Option<&str>,
    ) -> Result<A2ARun, A2ABackendError> {
        let _ = session_lane;
        self.start_in_context(context_id, agent_id, prompt).await
    }

    /// Load a task with its artifacts.
    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(AgentTask, Vec<Artifact>)>, A2ABackendError>;

    /// List tasks; filters are optional.
    async fn list_tasks(
        &self,
        context_id: Option<Uuid>,
        status: Option<TaskStatus>,
        limit: usize,
    ) -> Result<Vec<(AgentTask, Vec<Artifact>)>, A2ABackendError>;

    /// Real cancel: kills the live agent process.
    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError>;

    /// Subscribe to a live task's replay + live stream.
    async fn subscribe(
        &self,
        task_id: Uuid,
        after: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>>, A2ABackendError>;
}
