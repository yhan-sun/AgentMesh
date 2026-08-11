//! The unified [`CodingAgentAdapter`] interface every coding agent implements.

use std::path::PathBuf;

use agentmesh_core::{AgentDescriptor, AgentEvent, AgentMessage};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::error::AgentError;

/// Whether an agent is reachable right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Online,
    Offline,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Online => "online",
            HealthStatus::Offline => "offline",
        }
    }
}

/// Result of a health check performed by the orchestrator or the `agents` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHealth {
    pub status: HealthStatus,
    pub version: Option<String>,
    /// Executable the agent would use (e.g. `claude`); `None` for built-in
    /// agents without a backing binary.
    pub command: Option<String>,
    /// Human-friendly status note, e.g. "authentication required (run `codex login`)".
    pub message: Option<String>,
    pub details: Option<String>,
}

impl AgentHealth {
    pub fn online(version: Option<String>, command: Option<String>) -> Self {
        Self {
            status: HealthStatus::Online,
            version,
            command,
            message: None,
            details: None,
        }
    }

    pub fn offline(command: Option<String>, details: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Offline,
            version: None,
            command,
            message: None,
            details: Some(details.into()),
        }
    }
}

/// A request to run (or resume) a task on an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunRequest {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub input: AgentMessage,
    /// Native agent session to resume, when continuing an existing conversation.
    pub session_id: Option<Uuid>,
    /// Working directory the agent runs in; `None` inherits the caller's cwd.
    pub workspace: Option<PathBuf>,
}

impl AgentRunRequest {
    pub fn new(task_id: Uuid, context_id: Uuid, input: AgentMessage) -> Self {
        Self {
            task_id,
            context_id,
            input,
            session_id: None,
            workspace: None,
        }
    }
}

/// Streaming handle of an in-flight agent run.
///
/// The run id is what [`CodingAgentAdapter::cancel`] expects; the task id
/// inside the request links the run to a persisted [`AgentTask`].
///
/// The native session id (e.g. the Claude Code session id) is captured from
/// the agent's structured output while the run streams. It is exposed via
/// [`AgentRunHandle::session_id`] and, more importantly, via a
/// `tokio::sync::watch` channel ([`AgentRunHandle::session_rx`]) so the
/// task manager can persist it the moment it becomes known — without
/// polling.
#[derive(Debug)]
pub struct AgentRunHandle {
    run_id: Uuid,
    events: mpsc::Receiver<AgentEvent>,
    session_rx: watch::Receiver<Option<String>>,
}

impl AgentRunHandle {
    pub fn new(run_id: Uuid, events: mpsc::Receiver<AgentEvent>) -> Self {
        let (_, session_rx) = watch::channel(None);
        Self::with_session_channel(run_id, events, session_rx)
    }

    pub fn with_session_channel(
        run_id: Uuid,
        events: mpsc::Receiver<AgentEvent>,
        session_rx: watch::Receiver<Option<String>>,
    ) -> Self {
        Self {
            run_id,
            events,
            session_rx,
        }
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Native session id reported by the agent (e.g. the Claude Code session
    /// id), when the underlying CLI exposes one.
    pub fn session_id(&self) -> Option<String> {
        self.session_rx.borrow().clone()
    }

    /// Watch channel for the native session id. Subscribe (or re-subscribe)
    /// and await `changed()` to learn about the session id as soon as the
    /// adapter extracts it.
    pub fn session_rx(&self) -> watch::Receiver<Option<String>> {
        self.session_rx.clone()
    }

    /// Receive the next streaming event; `None` once the stream is exhausted.
    pub async fn next_event(&mut self) -> Option<AgentEvent> {
        self.events.recv().await
    }
}

/// Unified interface all coding agents (Claude Code, Codex, OpenCode, ...)
/// must implement so the orchestrator can discover, start, resume and cancel
/// them without knowing their internals.
#[async_trait]
pub trait CodingAgentAdapter: Send + Sync {
    /// Stable identifier used in config, routing and the CLI, e.g. `claude`.
    fn id(&self) -> &str;

    /// Human readable name, e.g. `Claude Code`.
    fn name(&self) -> &str;

    /// Static descriptor used for discovery and Agent Cards.
    fn descriptor(&self) -> AgentDescriptor;

    /// Check whether the underlying agent binary is available and healthy.
    async fn health_check(&self) -> Result<AgentHealth, AgentError>;

    /// Start a new task run.
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError>;

    /// Resume an existing native agent session.
    async fn resume(
        &self,
        native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError>;

    /// Cancel an in-flight run by its run id.
    async fn cancel(&self, run_id: &str) -> Result<(), AgentError>;
}
